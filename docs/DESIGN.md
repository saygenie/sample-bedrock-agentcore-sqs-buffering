# DESIGN — Research and Measurement Records, and Design Rationale

> A "question → research findings → measured evidence" record for the technical questions in PLAN.md §5.
> Research is as of 2026-09, based on official documentation. Measurements were taken against the
> deployed stack in us-west-2 on 2026-09-13; items marked `[to be measured]` will be filled in
> with numbers in this document after they are measured.

## Q1. Putting the agent behind Gateway (§5-1)

### Research findings

- Gateway targets fall into 3 categories: **MCP targets** (aggregate Lambda, OpenAPI, Smithy, MCP
  servers, etc. into a single virtual MCP server — for exposing tools), **HTTP targets** (direct
  proxy with no aggregation or protocol conversion), and **Inference targets** (LLM routing).
  ([Gateway core concepts](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-core-concepts.html))
- **Runtime agents are attached as a dedicated HTTP target type**: specify the Runtime ARN
  (required) and qualifier (optional, default `DEFAULT`) in `targetConfiguration.http.agentcoreRuntime`.
  The Gateway resolves the Runtime endpoint internally, so no URL assembly is needed. GA in 2026-07.
  ([Runtime target](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-target-http-runtime.html),
  [Release notes](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/release-notes.html))
- Constraint: Runtime targets can only be added to **gateways with no protocol type configured**
  (not to MCP protocol gateways). Capability synchronization and semantic tool search are not
  supported — clients address each target individually by path.
- **Invocation URL**: `https://{gatewayId}.gateway.bedrock-agentcore.{region}.amazonaws.com/{targetName}/invocations` (POST)
- **Inbound auth**: OAuth (JWT) / **IAM SigV4** / authenticate-only / no-auth (for development).
  **Outbound (Gateway→Runtime)**: `GATEWAY_IAM_ROLE` (SigV4), caller IAM, OAuth, token passthrough.
  ([Gateway inbound auth](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-inbound-auth.html))
- Blocking bypass: allow only the gateway execution role via a resource-based policy on the
  Runtime — "Front your runtime with an AgentCore Gateway" is the official security best practice.
  ([Runtime security best practices](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-security-best-practices.html))
- Note — the native entry point for direct invocation without the Gateway is `InvokeAgentRuntime`
  (`POST /runtimes/{agentRuntimeArn}/invocations?qualifier=...`, session header
  `X-Amzn-Bedrock-AgentCore-Runtime-Session-Id` 33–256 chars, SigV4 or JWT, payload up to 100MB).
  ([API reference](https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_InvokeAgentRuntime.html))

### Measured evidence (2026-09-13, us-west-2)

- ✅ Without boto3, a **urllib3 POST signed with botocore `SigV4Auth` (service `bedrock-agentcore`)**
  works as-is against `{gatewayUrl}/{targetName}/invocations` — no SDK endpoint_url override is
  needed. The consumer Lambda operates this way (consumer/handler.py).
- ✅ A gateway with no protocol type is obtained by omitting `protocol_type` on `CfnGateway`,
  and creating the `http.agentcoreRuntime` target via `CfnGatewayTarget` succeeds. However, **the CDK L2
  `GatewayTarget` does not yet have a Runtime target factory** (MCP family only), so it is defined at L1.
- ✅ With `AgentRuntimeArtifact.from_asset(platform=Platform.LINUX_ARM64)`, the ARM64 image
  build → ECR push → Runtime creation completes in a single `cdk deploy` (143 seconds).
- ✅ **An allow-only resource policy does not prevent a Gateway bypass.** With a policy whose only
  statement allowed `InvokeAgentRuntime` to the gateway role, an account administrator still
  invoked the Runtime directly and streamed a response — standard IAM evaluation, where a
  same-account identity-based Allow is sufficient on its own and a resource policy only adds
  permissions. Adding an explicit `Deny` for every principal outside the allow list
  (`StringNotEquals` on `aws:PrincipalArn`) blocked the same call with `AccessDeniedException`
  while the Gateway path kept working. The security best practice therefore needs the Deny half
  to be an enforced property rather than a convention.
- ✅ `PutResourcePolicy` rejects a statement whose `Resource` is anything other than exactly the
  one runtime ARN — a `<arn>/*` qualifier variant fails with
  "Policy statement block must contain exactly one resource ARN".
- ✅ The caller only needs `bedrock-agentcore:InvokeGateway` on the gateway ARN. Verified by
  removing the consumer's direct `InvokeAgentRuntime` grant entirely: streaming through the
  Gateway still passed scenario 1, so the Gateway's outbound call authorizes as the gateway role
  and no runtime permission is required on the caller.
- ✅ Least-privilege client policy verified empirically (see README "Permissions"): a temporary
  role holding only `execute-api:Invoke` on the ingest API, `sqs:GetQueueAttributes` on the
  queues, and `bedrock-agentcore:{ListGatewayRateLimits,UpdateGatewayRateLimit}` scoped to the
  gateway ARN could submit jobs, poll results, read DLQ depth and drive the throttle demo. Note
  the IAM policy simulator accepts non-existent action names, so it cannot be used to validate
  action spellings — an actual scoped call can.

## Q2. Whether streaming passes through (§5-2)

### Research findings

- **SSE streaming support for Runtime targets is stated explicitly in the docs**: "Server-Sent Events (SSE) streaming is
  supported for AgentCore Runtime targets." Interceptor Lambdas support buffered mode only
  (no streaming mode) — this sample does not use interceptors.
  ([Runtime target](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-target-http-runtime.html))
- Response streaming on the MCP target path is a separate feature (2026-05, `enableResponseStreaming`) —
  the release note wording that before then "gateway buffered the entire target response"
  corroborates that the buffering issue existed. The Runtime (HTTP) target path is documented
  as supporting SSE with no extra configuration.
  ([MCP streaming](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-mcp-streaming.html))
- Direct Runtime invocation also streams: if the response `contentType` is `text/event-stream`, it is `data:`-line SSE.
  ([Invoke agent](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-invoke-agent.html))
- Limits: Runtime payload 100MB, streaming chunks up to 10MB, synchronous requests 15 minutes (non-adjustable),
  **streaming up to 60 minutes (non-adjustable)**. **Gateway invocation timeout defaults to 15 minutes (adjustable)** —
  the difference that streams through the Gateway are capped at 15 minutes by default matters for the design.
  ([Quotas](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/bedrock-agentcore-limits.html))

### Measured evidence — how "no buffering" is measured

The agent puts each chunk's emission time (`emitTs`) into the chunk body; the consumer records the
relay time (`relayTs`) and the client records the arrival time (`arrivalTs`), and we compare **the
distribution of emit→relay latency** against **inter-chunk arrival gaps**. If buffering were present,
arrival gaps would converge to 0 and cluster at the end. This instrumentation is shared with the
verdict code of scenario 1 (§6). Caution: clocks on different machines have skew (about 1.8 seconds
observed between local and Lambda in measurements), so only **gaps within the same clock** (local
arrival gaps, emit→relay within the Lambda) are used for the verdict.

**Measured results (2026-09-13, us-west-2, simulated streaming, 20 chunks × 250ms):**

- ✅ **emit→relay latency 4–17ms** (through the Gateway) — Gateway SSE pass-through is effectively
  immediate, with no buffering. Chunk boundaries are also preserved (one SSE data event per chunk, 1:1).
- ✅ Client **arrival span 4,744ms vs emission span 4,750ms (ratio 1.00)**, mean arrival gap
  250ms (= emission interval), max 432ms — progressive arrival proven numerically (scenario 1 (§6) PASS).
- `[to be measured]` Whether the Gateway invocation timeout (default 15 minutes) applies to streaming
  connections — jobs in this sample (≤3 minutes) are unaffected.

## Q3. Actual behavior of rate limits (§5-3)

### Research findings

- **Customer-configurable Gateway rate limits** (launched 2026-08): via APIs such as `CreateGatewayRateLimit`,
  set RPS/RPM (requests), TPM (tokens), and CPS (connections) limits on buckets keyed by
  `dimensionKeys` (targetName, JWT claims, iam.sourceIdentity, etc.). Most-specific-match-wins, effective rate =
  min(service-managed, customer-configured), propagation ≤30 seconds, fail-open on failure.
  ([Gateway rate limits](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-rate-limits.html))
- **Throttle responses**: the HTTP path returns `429` +
  `{"error":"Rate limit exceeded","success":false,"limitKey":"...","metric":"requests","retryAfter":1}`
  — **the body includes retryAfter (seconds)**. The MCP path returns JSON-RPC `error.code: -32003` + `error.data.retryAfter`.
  ([Enforcement](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-rate-limits-enforcement.html))
- Gateway service-managed quotas: tool-call rate 200 TPS, 5,000 concurrent connections, etc. (adjustable).
- **Runtime quotas (us-west-2)**: Data plane API request rate **1,000 TPS/account** (adjustable),
  **New Runtime session creation rate 25 TPS/account** (adjustable), Active session workloads
  5,000 (us-west-2, adjustable).
  ([Quotas](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/bedrock-agentcore-limits.html))
- Direct Runtime invocation errors: `ThrottlingException`→**429**, `ServiceQuotaExceededException`→**402** (beware),
  `RetryableConflictException`→409. For Runtime throttles, only exponential backoff is recommended and no
  Retry-After header is documented (in contrast to the Gateway rate limit's retryAfter).
  ([API reference](https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_InvokeAgentRuntime.html))

### Measured evidence (2026-09-13, us-west-2)

- ✅ **429 response measured** (Runtime HTTP target path, when the customer-configured rate limit is exceeded):
  `HTTP 429` + body `{"metric":"requests","retryAfter":60.0,"success":false,"error":"Rate limit
  exceeded","limitKey":"<rateLimitId>"}` — matches the documentation; there is no `Retry-After` HTTP header,
  only the **retryAfter (seconds) in the body**. The retryAfter value was observed as 60 when the period is minute.
- ✅ **The limiter is not a strict blocker but allows bursts (approximate enforcement)**: at a limit of 1 per minute,
  out of a concurrent burst of 10 requests **only 2 got 429 and 8 passed**. Sequential calls (4 calls at
  1.6-second intervals) all passed. Given its distributed-enforcement nature, exact blocking near the
  limit should not be expected; precise pacing must be handled by the queue consumer (maximumConcurrency)
  — reconfirming this division of roles, which is the core rationale of this sample's design.
- ✅ **Propagation time measured**: the docs say ≤30 seconds, but 45 seconds after the update it was not yet
  applied; **application confirmed after a 90-second wait**. The acceptance test (scenario 3) uses a 90-second wait.
  The status shows ACTIVE immediately after the update, so status alone cannot be used to judge
  data-plane propagation.
- ✅ Throttles are returned as 429 pre-stream (before the stream starts) — mid-stream throttling after SSE
  start was not observed in this workload (1 job = 1 invocation).
- `[to be measured]` The bottleneck the New session 25 TPS limit imposes on consumer concurrency design —
  not reached at this sample's scale (concurrency 10).

## Q4. Pacing point (§5-4)

### Research findings

- **The primary pacing mechanism = `ScalingConfig.MaximumConcurrency` on the Lambda event source mapping** (range 2–1,000).
  When the limit is reached, Lambda **stops reading from the queue** (polling suppression) — since it does not
  emit throttling errors, it is lossless backpressure that does not consume ReceiveCount. AWS officially
  recommends "consider this first, instead of reserved concurrency, for limiting SQS consumption rate."
  ([SQS scaling](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-scaling.html),
  [Launch blog](https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/))
- Limiting via reserved concurrency causes the ESM to attempt scaling beyond the limit → invocation
  throttles → messages return to the queue after the visibility timeout (ReceiveCount increases, DLQ risk).
  If used together, reserved must be ≥ the sum of maximumConcurrency across all event sources.
- ESM scaling (2026): starts at 5 concurrent invocations, up to +300 per minute, cap of 1,250 per ESM.
  With maximumConcurrency set, the idle scale-down-to-2 optimization is disabled. The separate provisioned mode
  (dedicated pollers, 3x faster scaling) is mutually exclusive with maximumConcurrency — since pacing is
  the goal, this sample uses maximumConcurrency.
- **Consumer compute choice**: if the agent stream is ~3 minutes, it fits safely within Lambda (900-second limit),
  and polling, retries, partial batch failures, and pacing are all managed. If streams run 10+ minutes or the
  volume is consistently high, switch to Fargate workers (unbounded execution + backlog-per-task autoscaling + ChangeMessageVisibility
  heartbeat) — this decision criterion is documented in the README Limitations.
  ([ECS queue scaling](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-autoscaling-queue.html))
  Note: with Lambda Managed Instances extending the ESM timeout to 90 minutes, "anything over 15 minutes
  must be a container" is no longer absolute.
- **Division of roles**: pacing (proactive control) caps "the number of concurrently open streams" via
  maximumConcurrency × batchSize. Throttle absorption (reactive) makes only the failed messages reappear
  via the visibility timeout + `ReportBatchItemFailures` (partial batch responses). With partial batch
  responses enabled, invocation failures do not shrink polling.
  ([Error handling](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html))
- **retryAfter-aware retry**: inside the function, call
  `ChangeMessageVisibility(VisibilityTimeout = retryAfter)` with the record's `receiptHandle`, then return
  that `messageId` in `batchItemFailures` → the message reappears after the specified delay. Note that
  ReceiveCount still increases, and a total 12-hour cap applies from first receive.
  ([ChangeMessageVisibility](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html))

### Measured evidence (2026-09-13, us-west-2)

- ✅ **Bulk injection of a 30-message burst → peak concurrent streams exactly 10 (= maximumConcurrency), 3 waves,
  start times spread over 23.4 seconds, all completed, 0 lost, 0 in DLQ** (scenario 2 (§6) PASS). Judged with a
  single clock on the consumer side (overlap of invokeTs–lastChunkTs windows).
- ✅ The ChangeMessageVisibility(retryAfter + attempt-scaled jitter) + batchItemFailures combination
  demonstrated (scenario 3 (§6) PASS): a 12-message burst past a lowered limit → **throttled status
  events caused by 429, 12/12 completed via internal retries, 0 in DLQ**. The client experienced only
  status notifications and delay — "throttling appears to the user only as latency" proven numerically.
  Backoff policy note (measured): the server's retryAfter is honored as-is with only jitter added on
  top — an earlier retryAfter×attempt policy made a 4-times-throttled job wait 60+120+180+240 = 600s
  (observed 607s total), while the server-hinted wait plus jitter drains the same backlog in ~1/attempt
  of that time. Multiplying a server-provided hint is redundant backoff.

## Q5. Server-initiated push channel (§5-5)

### Research findings — comparison of options

| Item | API GW WebSocket | **AppSync Events API** | AppSync GraphQL subscriptions | IoT Core (MQTT/WSS) |
|---|---|---|---|---|
| Backend→specific-client push | `PostToConnection` (requires connectionId) | `POST /event` to the jobId channel (SigV4) | requires a GraphQL mutation | Publish to the jobId topic |
| Target addressing | direct connectionId management (DynamoDB mapping required) | client subscribes to the `/ns/{jobId}` channel — no mapping needed | subscription field arguments | topic subscription |
| Connection lifetime/idle | **2 hours / 10-minute idle** | **24 hours**, 60-second keep-alive provided | 24 hours | ≤24 hours (not guaranteed) |
| Payload | 128KB (32KB frames) | 240KB per event | 240KB | 128KB |
| Handling of gone targets | handle 410 GoneException yourself | handled by the service | handled by the service | handled by the service |
| Pricing (messages / connection-minutes, per million) | $1.00 / $0.25 | $1.00 / **$0.08** | similar | ~$1.00 / $0.08 |
| IaC | CFN, CDK L2 | CFN (`AWS::AppSync::Api`/`ChannelNamespace`), **CDK L2 `EventApi`** | CFN, CDK L2 | CFN (complex policies) |
| No-build static client | easy with built-in WebSocket (IAM connection signing is hard) | **protocol officially documented + plain JS example** (~30 lines) | heavy hand-rolling burden | requires an MQTT library + signing |
| Ancillary infrastructure | $connect/$disconnect Lambda + DynamoDB | none | schema + resolvers | IoT policy |

Key sources: [Events API](https://docs.aws.amazon.com/appsync/latest/eventapi/event-api-welcome.html),
[WebSocket protocol](https://docs.aws.amazon.com/appsync/latest/eventapi/event-api-websocket-protocol.html),
[HTTP publish](https://docs.aws.amazon.com/appsync/latest/eventapi/publish-http.html),
[API GW WS limits](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-execution-service-websocket-limits-table.html),
[AppSync pricing](https://aws.amazon.com/appsync/pricing/)

- Why Lambda response streaming / direct SSE is unsuitable: it is a request-scoped channel, so **a separate
  component (the SQS consumer) has no way to inject into an existing stream at an arbitrary time**. Making
  it work would require the consumer to become a long-running HTTP server, defeating the purpose of a
  "managed push channel."
  ([Lambda response streaming](https://docs.aws.amazon.com/lambda/latest/dg/configuration-response-streaming.html))
- AppSync GraphQL subscriptions are triggered only by mutations and require a schema and resolvers — for
  pure pub/sub, the Events API is the de facto replacement.

### Decision (§8-3)

**AppSync Events API adopted.** Rationale: (1) backend push is a single SigV4 HTTP POST — no connectionId
mapping table, no $connect/$disconnect Lambdas, no 410 handling; (2) natural unicast in which the client
simply subscribes to the `/jobs/{jobId}` channel before submitting the job; (3) the protocol is officially
documented and can be hand-rolled in a no-build static page (the official docs' example is plain JS);
(4) 24-hour connections + service keep-alive; (5) CFN/CDK L2 support; (6) connection-minute pricing is
1/3 of API GW WebSocket's. Configuration: client connect/subscribe uses an API key (sample simplicity),
consumer publish uses IAM (`appsync:EventPublish`, namespace-scoped).

Why the runner-up, API GW WebSocket, was rejected: jobId→connectionId DynamoDB management + 2 Lambdas +
410 handling + 10-minute idle/2-hour keepalive and reconnection add 3–4 more moving parts the reader
has to understand.

### Measured evidence (2026-09-13, us-west-2)

- ✅ Chunk arrival ordering is not guaranteed in the docs → the design has **each chunk carry a
  sequence number so the client can reorder and detect gaps**. In practice, every measured run
  (20-chunk single stream, 30-job burst, throttled runs) delivered chunks in emission order with
  no gaps — ordering held empirically, but the sequence numbers stay as the correctness mechanism.
- publish→subscriber absolute latency (p50/p99) is **not measurable across machines**: local-vs-Lambda
  clock skew (~1.8s observed) exceeds the latency itself. Same-clock evidence bounds it instead:
  emit→relay 4–17ms inside the pipeline, and client inter-arrival gaps tracking the emission
  interval (mean 250ms vs 250ms) imply the publish→arrival hop adds no meaningful buffering.
- ✅ Events published before the subscription ack are lost → all tests and the demo client enforce
  "subscribe ack, then submit". Across every measured run chunk seq 1 was always received
  (20/20, 30×10/10 deliveries) — no race observed under this ordering.
- ✅ Event size: agent chunks are token deltas (couple of KB at most; mock chunks ~120 bytes,
  real-LLM deltas well under 1KB) — far below the 240KB silent-drop threshold. A warning belongs
  in the code only if an agent emits jumbo chunks (the 10MB runtime chunk limit exceeds AppSync's
  240KB, so a size guard in the consumer is documented as a caveat).

## Q6. Lifetime and timeout alignment (§5-6)

### Research findings — enumeration of limits

- Lambda timeout up to 900 seconds. ([Timeout](https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html))
- SQS visibility timeout default 30 seconds, max 12 hours. Even with extensions, a total 12-hour cap from first receive.
  ([Visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html))
- ESM hard requirement: **function timeout ≤ visibility timeout** (validated by Lambda). Recommendation: **visibility ≥ 6 ×
  (function timeout + batch window)**. ([Configure SQS ESM](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html))
- Message retention 60 seconds–14 days (default 4 days). DLQ expiration is **based on the original enqueue time**,
  so keep DLQ retention longer than the source queue's. maxReceiveCount ≥5 recommended for Lambda ESM.
  ([DLQ](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html))
- Standard queues are at-least-once — duplicate delivery is possible even without errors; idempotent design is mandatory.
  ([At-least-once](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html))
- AgentCore Runtime: synchronous 15 minutes, streaming 60 minutes, session idle termination default 15 minutes.
- Push channel (AppSync Events): 24-hour connections, 60-second service keep-alive — ample headroom
  relative to chunk intervals and job lifetime.

### System of inequalities

Notation: T_agent = maximum stream duration (≈180s), W = batch window, T_fn = Lambda timeout,
V = visibility timeout, R = maxReceiveCount, Ret = retention period.

1. **T_agent + margin ≤ T_fn ≤ 900s** — the consumer holds the entire stream (T_agent ≤ 60 min: AgentCore limit)
2. **T_fn ≤ V** (ESM requirement) and **V ≥ 6 × (T_fn + W)** (official recommendation)
3. **R × (V + processing time) ≪ Ret** — the full retry cycle must finish within the retention period to avoid expiration loss
4. Sum of visibility extensions ≤ 12h (from first receive)
5. Pacing: **maximumConcurrency × batchSize ≤ downstream concurrent session limit**, maximumConcurrency ≥ 2

### Adopted parameters (T_agent ≈ 180s workload)

| Parameter | Value | Rationale |
|---|---|---|
| batchSize | 1 | 1 job = 1 stream = 1 invocation; concurrent streams = maximumConcurrency |
| Batch window W | 0 | minimize dispatch latency |
| maximumConcurrency | 10 (demo default, minimum 2) | proactive pacing matched to the downstream concurrency limit |
| Lambda timeout T_fn | 300s | 180s + initialization/relay margin |
| Visibility timeout V | 1,800s | satisfies the official 6 × (300 + 0) recommendation |
| maxReceiveCount R | 5 | official ESM recommendation ≥5 |
| Source queue retention | 4 days (default) | worst-case R×V of 2.5h ≪ 4 days |
| DLQ retention | 14 days | guards against expiration based on the original enqueue time |
| FunctionResponseTypes | ReportBatchItemFailures | prevents polling shrinkage on failures |

Verification chain: 180s ≤ 300s ≤ 1,800s = 6×300 ≤ 12h, 5×30min = 2.5h ≪ 4d.

### Duplicate execution and idempotency

- Where duplicates arise (officially enumerated): function errors/timeouts, delete failures, lost acks,
  at-least-once redelivery, batch window + execution time > visibility. ([re:Post KC](https://repost.aws/knowledge-center/lambda-function-process-sqs-messages))
- The convention = **conditional writes to a DynamoDB job status table**: a conditional PutItem keyed by
  the job (IN_PROGRESS claim + in_progress_expiry) → skip on ConditionalCheckFailedException. The practical
  risk in this workload is **the same job's stream being replayed twice, sending duplicate chunks over the
  push channel** — the conditional write is the point that blocks the stream replay itself.
  ([Idempotency KC](https://repost.aws/knowledge-center/lambda-function-idempotent))

### Measured evidence

- ✅ Partially measured: a live transient failure (connection reset mid-stream) exercised the
  FAILED→re-claim path — the retry re-claimed the job (attempts 2–5 observed) and completed it.
  The claim path for a consumer that dies *without* marking FAILED (crash/timeout → IN_PROGRESS
  with unexpired in_progress_expiry) remains `[to be measured]`; until the expiry passes, a
  duplicate delivery is treated as owned and skipped by design.
- ✅ Scenario 2 (§6) judged numerically across three runs: 30/30 completed, 0 lost, DLQ depth
  delta 0 each time (one run needed an SQS retry after a network reset — absorbed as designed).
- Boundary condition (measured the hard way): with maxReceiveCount=5, a job can absorb at most
  **4 throttle/retry rounds** before the DLQ — one job under the old retryAfter×attempt backoff
  completed on its 5th and final attempt. The retryAfter+jitter policy keeps attempts low, but
  R must be sized as R ≥ expected throttle rounds + normal-failure margin.

## Q7. IaC coverage (§5-7)

### Research findings

- **26 CloudFormation `AWS::BedrockAgentCore::*` types exist** — `Runtime`, `RuntimeEndpoint`,
  `Gateway`, `GatewayTarget`, **`GatewayRateLimit`**, `Memory`, `ResourcePolicy`, etc.; every resource
  this sample needs can be defined in CFN.
  ([CFN reference](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/AWS_BedrockAgentCore.html))
- `Runtime`'s `AgentRuntimeArtifact` supports two modes: container (`ContainerConfiguration`, **ECR only**) or
  direct code deployment (`CodeConfiguration`, Python 3.10–3.14/Node 22 — no Docker required).
  ([Runtime](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-bedrockagentcore-runtime.html))
- The **`AgentcoreRuntime` configuration** on `GatewayTarget`'s HTTP target (the Runtime target from §5-1) is expressible in CFN.
  ([GatewayTarget](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-bedrockagentcore-gatewaytarget.html))
- **CDK L2 is stable**: graduated from alpha into `aws-cdk-lib/aws-bedrockagentcore`, including `Runtime`,
  `Gateway`, `GatewayTarget`, `Memory`, etc. `AgentRuntimeArtifact.fromAsset(dir)` integrates local
  Dockerfile → ARM64 build → ECR push → Runtime wiring into a single `cdk deploy`.
  ([CDK alpha README](https://github.com/aws/aws-cdk/blob/main/packages/%40aws-cdk/aws-bedrock-agentcore-alpha/README.md))
- Gateway rate limits can also be defined in IaC as a CFN resource (`GatewayRateLimit`). The Runtime itself
  has no per-resource rate limit setting (account quotas only).
- Runtime container contract: **linux/arm64, 0.0.0.0:8080, `POST /invocations` (JSON or SSE) +
  `GET /ping`**, image up to 2GB.
  ([HTTP contract](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-http-protocol-contract.html))
- The only leftovers unsupported by CFN are one-time account-level setup items (CloudWatch Transaction
  Search, etc.) — the convention is a CDK custom resource or a one-time CLI run in the deployment script.
- Terraform also supports `aws_bedrockagentcore_*` resources, but image builds live outside the IaC (CodeBuild/
  scripts), and a workaround is needed because code changes do not trigger a new Runtime version.

### Decision (§8-1)

**CDK (Python) adopted.** Rationale: (1) AgentCore L2 stable + `fromAsset` Docker asset integration makes
build→push→deploy a single command; (2) one `cdk destroy --force` gives single-command cleanup (including S3/ECR
auto-delete); (3) the official `@aws/agentcore` CLI and the serverless-patterns AgentCore examples are
all CDK-based — consistent with aws-samples convention; (4) the agent (Strands), consumer Lambda, and
acceptance tests are all Python, keeping the repository single-language. Terraform rejected: no image-build integration.

### Measured evidence

- ✅ `fromAsset` is used with an explicit `Platform.LINUX_ARM64` (required for correctness on x86
  build hosts; harmless on ARM hosts).
- ✅ Post-`cdk destroy` residue, measured: (a) implicitly created **Lambda log groups survive** —
  fixed by defining explicit stack-owned LogGroups on both functions; (b) the **AgentCore Runtime's
  auto-created log group** (`/aws/bedrock-agentcore/runtimes/<runtime-id>-DEFAULT`) survives —
  destroy.sh deletes it, scoped to this sample's unique runtime name; (c) CDK bootstrap ECR asset
  images remain by design (shared infrastructure, documented in the README).
- `[to be measured]` When the DEFAULT endpoint reflects a CFN Runtime update (image change) —
  code-only updates in this project redeployed in ~50s and were served immediately afterward.

## Q8. Failure and edge cases (§5-8)

### Research findings

- **The job continues even if the client disconnects mid-stream**: the push channel (AppSync Events) and the
  processing path (SQS→consumer→agent) are decoupled, so there is no built-in mechanism by which a client
  disconnect cancels the upstream — this is the structural basis for separating connection and job
  lifetimes (§2-4). The only explicit means of stopping a session is the `StopRuntimeSession` API.
  ([Stop session](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-stop-session.html))
- Runtime session lifetime: per-session microVM, terminated after 15 minutes idle (default), max 8 hours.
  Operating on the same session while it is terminating yields 409 `RetryableConflictException`.
  ([Sessions](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-sessions.html))
- The server-side behavior when the consumer dies mid-SSE-read (handler continues vs. is interrupted) is
  **not documented** — measurement required. The official long-running pattern is `HealthyBusy` + async
  tasks (out of scope for this sample; linked in Limitations).
- **Observation points for repeated failures**: DLQ `ApproximateNumberOfMessagesVisible` alarm (officially
  recommended), source queue `ApproximateAgeOfOldestMessage`, Lambda ESM metrics (`FailedInvokeEventCount`,
  etc. — ESM metrics must be enabled), AgentCore `AWS/Bedrock-AgentCore` namespace (`Invocations`, `Throttles`,
  `SystemErrors`, `Latency`, `ActiveSessionCount`).
  ([DLQ alarms](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/dead-letter-queues-alarms-cloudwatch.html),
  [Runtime metrics](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/observability-runtime-metrics.html))
- Traces require one-time account-level Transaction Search activation + the ADOT SDK in the agent — for
  this sample, default metrics + self-instrumentation (timestamps) suffice; traces are documented as optional.
- `ReportBatchItemFailures` pitfall: if `batchItemFailures` is an empty list or malformed, **the entire
  batch is treated as successful and failed messages are silently deleted** — manage the return format
  strictly in consumer code.
  ([re:Post KC](https://repost.aws/knowledge-center/lambda-sqs-report-batch-item-failures))

### Measured evidence

- `[to be measured]` Whether an in-flight `/invocations` handler continues or stops when the consumer
  Lambda is forcibly terminated, and confirmation that retries avoid the 409 by using a new session ID.
- ✅ Scenario 4 (§6) PASS (2026-09-13): client WebSocket cut mid-stream (after receiving 11 of 30 chunks)
  → **the job ran to completion server-side with all 30 chunks (COMPLETED)** → the 501-character result
  was retrieved successfully via GET /jobs/{jobId}. Separation of connection lifetime and job lifetime
  confirmed in action.

## Decision summary (final answers to the PLAN §8 open questions)

| # | Item | Decision | Rationale location |
|---|---|---|---|
| 1 | IaC tool | **CDK (Python)** | §5-7 |
| 2 | Agent implementation | **Real LLM (Claude Haiku 4.5) + simulated-streaming flag** | user confirmation |
| 3 | Push channel | **AppSync Events API** | §5-5 |
| 4 | Demo client | **Static web (no build)** | user confirmation |
| 5 | Ingestion API auth | **IAM SigV4** | user confirmation |
| 6 | Queue type | **Standard** (+DLQ) | no ordering requirement; duplicates absorbed by the §5-6 idempotent design |
| 7 | Repository name | `sample-bedrock-agentcore-sqs-buffering` | to be revisited during the publication process |

## Acceptance test results summary (§6) (2026-09-13, us-west-2, tests/run_scenarios.py)

Figures below are from repeated runs on 2026-09-13 (ranges where runs differed):

| # | Scenario | Result | Key figures |
|---|---|---|---|
| 1 | Single-job streaming | **PASS** | 20/20 chunks in order, arrival-span/emission-span ratio **0.99–1.00**, mean arrival gap 246–250ms (emission interval 250ms), max gap 296–432ms |
| 2 | Burst buffering | **PASS** | 30/30 completed, 0 lost, 0 in DLQ, peak concurrent streams **10** (= limit), 3 waves, start spread 18.6–23.4s; one run absorbed a transient connection reset via SQS retry |
| 3 | Throttle absorption | **PASS** | 12-message burst at a 1/min limit → 2–16 throttled events from 429s (retryAfter 60, bursty limiter), **12/12 completed**, 0 in DLQ |
| 4 | Reconnection recovery | **PASS** | cut after receiving 11 chunks → 30/30 completed server-side, result (501 chars) retrieved by job ID |
| 5 | Deploy/cleanup round trip | **PASS** | scripts/e2e.sh from clean state: fresh deploy 120s → scenarios 1–2 PASS → destroy → **zero residual resources** (stack deleted, no leftover log groups, no leftover AgentCore runtime/gateway; shared CDK bootstrap intentionally kept) |

## Appendix A. Regional availability (us-west-2)

- AgentCore GA 2025-10; us-west-2 supports all capabilities, including Runtime, Gateway, Memory, Identity, and Observability.
  The only exception is the Web Search Tool (irrelevant to this sample).
  ([Regions](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-regions.html))
- Endpoints: data plane `bedrock-agentcore.us-west-2.amazonaws.com`, control plane
  `bedrock-agentcore-control.us-west-2.amazonaws.com`, Gateway
  `https://{gatewayId}.gateway.bedrock-agentcore.us-west-2.amazonaws.com`.

## Appendix B. Pricing summary (source for the README cost section)

AgentCore is pay-as-you-go with no upfront or minimum fees. Runtime bills per second of actual microVM
usage (CPU $0.0895/vCPU-h, memory $0.00945/GB-h, no CPU charge while waiting on I/O — favorable for
agents that spend long periods waiting on LLMs). Gateway is $0.005 per 1,000 API calls. Memory and
Identity are either unused by this sample or on a free path.
([Pricing](https://aws.amazon.com/bedrock/agentcore/pricing/))
