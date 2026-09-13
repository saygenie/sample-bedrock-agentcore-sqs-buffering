"""SQS-buffered streaming agent stack.

Request path:  client -> HTTP API (IAM) -> SQS (+DLQ) -> consumer Lambda
               -> AgentCore Gateway -> AgentCore Runtime (streaming agent)
Response path: consumer Lambda -> AppSync Events API -> client (WebSocket)

Parameter reasoning lives in docs/DESIGN.md §5-6 (timeout inequalities):
T_agent(180s) <= T_fn(300s) <= V(1800s) = 6 x T_fn, R=5, retention 4d.
"""

import json

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_apigatewayv2 as apigwv2
from aws_cdk import aws_apigatewayv2_authorizers as apigwv2_authorizers
from aws_cdk import aws_apigatewayv2_integrations as apigwv2_integrations
from aws_cdk import aws_appsync as appsync
from aws_cdk import aws_bedrockagentcore as agentcore
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sqs as sqs
from aws_cdk.aws_ecr_assets import Platform
from cdk_nag import NagSuppressions
from constructs import Construct

CONSUMER_TIMEOUT_SECONDS = 300
VISIBILITY_TIMEOUT_SECONDS = 6 * CONSUMER_TIMEOUT_SECONDS
MAX_RECEIVE_COUNT = 5
GATEWAY_TARGET_NAME = "agent"
CHANNEL_NAMESPACE = "jobs"


class BufferingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        max_concurrency = int(self.node.try_get_context("maxConcurrency") or 10)
        rate_limit_rps = int(self.node.try_get_context("rateLimitRps") or 10)
        model_id = self.node.try_get_context("modelId") or "us.anthropic.claude-haiku-4-5-20251001-v1:0"
        invoke_mode = self.node.try_get_context("invokeMode") or "gateway"

        # --- Job state store -------------------------------------------------
        jobs_table = dynamodb.Table(
            self,
            "JobsTable",
            partition_key=dynamodb.Attribute(name="jobId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expiresAt",
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=False
            ),
        )

        # --- Buffer queue + DLQ ----------------------------------------------
        dlq = sqs.Queue(
            self,
            "JobDlq",
            retention_period=Duration.days(14),
            enforce_ssl=True,
        )
        queue = sqs.Queue(
            self,
            "JobQueue",
            visibility_timeout=Duration.seconds(VISIBILITY_TIMEOUT_SECONDS),
            retention_period=Duration.days(4),
            enforce_ssl=True,
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=MAX_RECEIVE_COUNT, queue=dlq),
        )

        # --- AgentCore Runtime (streaming agent container) --------------------
        runtime = agentcore.Runtime(
            self,
            "AgentRuntime",
            runtime_name="sqs_buffering_sample_agent",
            description="Streaming demo agent (real LLM + mock-streaming flag)",
            agent_runtime_artifact=agentcore.AgentRuntimeArtifact.from_asset(
                "../agent",
                platform=Platform.LINUX_ARM64,
            ),
            environment_variables={"MODEL_ID": model_id},
        )
        runtime.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[
                    "arn:aws:bedrock:*::foundation-model/*",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/*",
                ],
            )
        )

        # --- AgentCore Gateway fronting the runtime ---------------------------
        # Runtime targets require a gateway with no protocol type set, which the
        # L2 Gateway/GatewayTarget constructs cannot express yet -> L1.
        gateway_role = iam.Role(
            self,
            "GatewayRole",
            # Scope the service trust to this account so another customer's
            # gateway cannot assume this role (cross-service confused deputy).
            assumed_by=iam.ServicePrincipal(
                "bedrock-agentcore.amazonaws.com",
                conditions={"StringEquals": {"aws:SourceAccount": self.account}},
            ),
        )
        runtime.grant_invoke(gateway_role)

        gateway = agentcore.CfnGateway(
            self,
            "Gateway",
            name="sqs-buffering-sample-gw",
            authorizer_type="AWS_IAM",
            role_arn=gateway_role.role_arn,
        )

        target = agentcore.CfnGatewayTarget(
            self,
            "RuntimeTarget",
            gateway_identifier=gateway.attr_gateway_identifier,
            name=GATEWAY_TARGET_NAME,
            target_configuration=agentcore.CfnGatewayTarget.TargetConfigurationProperty(
                http=agentcore.CfnGatewayTarget.HttpTargetConfigurationProperty(
                    agentcore_runtime=agentcore.CfnGatewayTarget.RuntimeTargetConfigurationProperty(
                        arn=runtime.agent_runtime_arn,
                    )
                )
            ),
            credential_provider_configurations=[
                agentcore.CfnGatewayTarget.CredentialProviderConfigurationProperty(
                    credential_provider_type="GATEWAY_IAM_ROLE",
                )
            ],
        )

        # Demonstrable throttling for scenario 3; the default bucket ("*")
        # applies to every caller of the runtime target.
        agentcore.CfnGatewayRateLimit(
            self,
            "GatewayRateLimit",
            gateway_identifier=gateway.attr_gateway_identifier,
            dimension_keys=["targetName"],
            entries=[
                agentcore.CfnGatewayRateLimit.LimitEntryProperty(
                    dimensions={"targetName": "*"},
                    requests=[
                        agentcore.CfnGatewayRateLimit.RateConfigProperty(
                            period="second",
                            rate=rate_limit_rps,
                        )
                    ],
                )
            ],
        )

        # --- Push channel: AppSync Events API ---------------------------------
        event_api = appsync.EventApi(
            self,
            "EventsApi",
            api_name="sqs-buffering-sample-events",
            authorization_config=appsync.EventApiAuthConfig(
                auth_providers=[
                    appsync.AppSyncAuthProvider(
                        authorization_type=appsync.AppSyncAuthorizationType.API_KEY
                    ),
                    appsync.AppSyncAuthProvider(
                        authorization_type=appsync.AppSyncAuthorizationType.IAM
                    ),
                ],
                connection_auth_mode_types=[appsync.AppSyncAuthorizationType.API_KEY],
                default_publish_auth_mode_types=[appsync.AppSyncAuthorizationType.IAM],
                default_subscribe_auth_mode_types=[appsync.AppSyncAuthorizationType.API_KEY],
            ),
        )
        event_api.add_channel_namespace(CHANNEL_NAMESPACE)

        # --- Consumer: SQS -> Gateway (SSE) -> Events publish ------------------
        consumer_fn = lambda_.Function(
            self,
            "ConsumerFn",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../consumer"),
            timeout=Duration.seconds(CONSUMER_TIMEOUT_SECONDS),
            memory_size=512,
            # Explicit log group so `cdk destroy` removes it (implicitly created
            # Lambda log groups outlive the stack and violate zero-residue cleanup).
            log_group=logs.LogGroup(
                self,
                "ConsumerLogs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            ),
            environment={
                "JOBS_TABLE": jobs_table.table_name,
                "QUEUE_URL": queue.queue_url,
                "EVENTS_HTTP_DOMAIN": event_api.http_dns,
                "CHANNEL_NAMESPACE": CHANNEL_NAMESPACE,
                "GATEWAY_URL": gateway.attr_gateway_url,
                "TARGET_NAME": GATEWAY_TARGET_NAME,
                "RUNTIME_ARN": runtime.agent_runtime_arn,
                "INVOKE_MODE": invoke_mode,
                "DEFAULT_RETRY_SECONDS": "30",
                "ERROR_RETRY_SECONDS": "15",
            },
        )
        consumer_fn.add_event_source(
            event_sources.SqsEventSource(
                queue,
                batch_size=1,
                max_concurrency=max_concurrency,
                report_batch_item_failures=True,
            )
        )
        # Only the calls the handler actually makes: a conditional claim, status
        # updates, and reading back the owner on a claim conflict.
        jobs_table.grant(consumer_fn, "dynamodb:UpdateItem", "dynamodb:GetItem")
        event_api.grant_publish(consumer_fn)
        # SqsEventSource grants consume (incl. ChangeMessageVisibility for retry pacing).
        consumer_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock-agentcore:InvokeGateway"],
                resources=[gateway.attr_gateway_arn, f"{gateway.attr_gateway_arn}/*"],
            )
        )
        # The default deployment deliberately grants NO direct runtime access, so
        # the Gateway really is the only way in. Opt in with -c invokeMode=direct
        # to measure the Gateway's overhead against a direct baseline.
        allowed_invokers = [gateway_role.role_arn]
        if invoke_mode == "direct":
            runtime.grant_invoke(consumer_fn)
            allowed_invokers.append(consumer_fn.role.role_arn)

        # Identity-based grants alone do not stop a bypass: within one account an
        # identity-based Allow is sufficient on its own, so any privileged
        # principal could call the runtime directly (measured — an admin user
        # succeeded against an Allow-only resource policy). Only an explicit Deny
        # makes "the gateway is the only entrance" an enforced property.
        invoke_actions = [
            "bedrock-agentcore:InvokeAgentRuntime",
            "bedrock-agentcore:InvokeAgentRuntimeForUser",
        ]
        agentcore.CfnResourcePolicy(
            self,
            "RuntimeResourcePolicy",
            resource_arn=runtime.agent_runtime_arn,
            policy=Stack.of(self).to_json_string(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowGateway",
                            "Effect": "Allow",
                            "Principal": {"AWS": allowed_invokers},
                            "Action": invoke_actions,
                            # PutResourcePolicy requires exactly this one ARN —
                            # adding a "/*" qualifier variant is rejected.
                            "Resource": runtime.agent_runtime_arn,
                        },
                        {
                            "Sid": "DenyEveryoneElse",
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": invoke_actions,
                            "Resource": runtime.agent_runtime_arn,
                            "Condition": {"StringNotEquals": {"aws:PrincipalArn": allowed_invokers}},
                        },
                    ],
                }
            ),
        )

        # --- Ingest API (IAM auth) --------------------------------------------
        ingest_fn = lambda_.Function(
            self,
            "IngestFn",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../ingest"),
            timeout=Duration.seconds(10),
            log_group=logs.LogGroup(
                self,
                "IngestLogs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            ),
            environment={
                "JOBS_TABLE": jobs_table.table_name,
                "QUEUE_URL": queue.queue_url,
            },
        )
        queue.grant_send_messages(ingest_fn)
        # Conditional insert on submit, point read on status lookup — nothing else.
        jobs_table.grant(ingest_fn, "dynamodb:PutItem", "dynamodb:GetItem")

        http_api = apigwv2.HttpApi(
            self,
            "IngestApi",
            default_authorizer=apigwv2_authorizers.HttpIamAuthorizer(),
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[apigwv2.CorsHttpMethod.GET, apigwv2.CorsHttpMethod.POST],
                # "*" does not cover Authorization in the Fetch spec, so the
                # browser demo's SigV4 headers must be listed explicitly.
                allow_headers=[
                    "authorization",
                    "content-type",
                    "x-amz-date",
                    "x-amz-security-token",
                    "x-amz-content-sha256",
                ],
            ),
        )
        ingest_integration = apigwv2_integrations.HttpLambdaIntegration("IngestInteg", ingest_fn)
        http_api.add_routes(
            path="/jobs",
            methods=[apigwv2.HttpMethod.POST],
            integration=ingest_integration,
        )
        http_api.add_routes(
            path="/jobs/{jobId}",
            methods=[apigwv2.HttpMethod.GET],
            integration=ingest_integration,
        )

        access_logs = logs.LogGroup(
            self,
            "IngestApiAccessLogs",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        default_stage = http_api.default_stage.node.default_child
        default_stage.access_log_settings = apigwv2.CfnStage.AccessLogSettingsProperty(
            destination_arn=access_logs.log_group_arn,
            format=json.dumps(
                {
                    "requestId": "$context.requestId",
                    "ip": "$context.identity.sourceIp",
                    "requestTime": "$context.requestTime",
                    "httpMethod": "$context.httpMethod",
                    "path": "$context.path",
                    "status": "$context.status",
                    "responseLength": "$context.responseLength",
                }
            ),
        )

        NagSuppressions.add_stack_suppressions(
            self,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "AWSLambdaBasicExecutionRole is the standard CloudWatch-logging policy for sample Lambda functions.",
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Wildcards come from CDK grant helpers scoped to single resources of this stack "
                    "(runtime endpoint qualifiers, per-channel Events publish, gateway target paths, "
                    "runtime log streams / X-Ray) and from cross-region inference profile invocation, "
                    "which requires foundation-model/* plus the account-scoped inference-profile/*.",
                },
                {
                    "id": "AwsSolutions-DDB3",
                    "reason": "The job store holds ephemeral demo data with a TTL; point-in-time recovery adds cost without value here.",
                },
            ],
        )

        # --- Outputs -----------------------------------------------------------
        api_key = next(iter(event_api.api_keys.values()))
        CfnOutput(self, "IngestEndpoint", value=http_api.api_endpoint)
        CfnOutput(self, "EventsHttpDomain", value=event_api.http_dns)
        CfnOutput(self, "EventsRealtimeDomain", value=event_api.realtime_dns)
        CfnOutput(self, "EventsApiKey", value=api_key.attr_api_key)
        CfnOutput(self, "GatewayUrl", value=gateway.attr_gateway_url)
        CfnOutput(self, "GatewayTargetId", value=target.attr_target_id)
        CfnOutput(self, "RuntimeArn", value=runtime.agent_runtime_arn)
        # The demo plots this as the pacing ceiling, so it must be discoverable.
        CfnOutput(self, "MaxConcurrency", value=str(max_concurrency))
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "DlqUrl", value=dlq.queue_url)
        CfnOutput(self, "JobsTableName", value=jobs_table.table_name)
        CfnOutput(self, "ConsumerFunctionName", value=consumer_fn.function_name)
