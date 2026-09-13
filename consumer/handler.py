"""SQS consumer: pull a job, stream the agent's response, relay every chunk
to the client's push channel in real time.

Per-record flow:
  1. Claim the job in DynamoDB with a conditional write (idempotency: a
     duplicate delivery of the same job must not replay the stream).
  2. Invoke the agent through AgentCore Gateway (SigV4, SSE) — or directly
     via InvokeAgentRuntime when INVOKE_MODE=direct (measurement baseline).
  3. Publish each chunk to AppSync Events channel /jobs/{jobId} with relayTs.
  4. On 429, set the message's visibility to the server-provided retryAfter
     and report a batch item failure — the throttle is absorbed here; the
     client only observes a status event and a delayed stream.

Timestamps recorded per job (emitTs from the agent, relayTs here, arrivalTs
at the client) are the §6 acceptance-criteria measurements.
"""

import json
import os
import random
import time
import traceback
import uuid

import boto3
import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import ClientError

REGION = os.environ["AWS_REGION"]
JOBS_TABLE = os.environ["JOBS_TABLE"]
QUEUE_URL = os.environ["QUEUE_URL"]
EVENTS_HTTP_DOMAIN = os.environ["EVENTS_HTTP_DOMAIN"]
CHANNEL_NAMESPACE = os.environ.get("CHANNEL_NAMESPACE", "jobs")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "").rstrip("/")
TARGET_NAME = os.environ.get("TARGET_NAME", "agent")
RUNTIME_ARN = os.environ.get("RUNTIME_ARN", "")
INVOKE_MODE = os.environ.get("INVOKE_MODE", "gateway")
DEFAULT_RETRY_SECONDS = int(os.environ.get("DEFAULT_RETRY_SECONDS", "30"))
ERROR_RETRY_SECONDS = int(os.environ.get("ERROR_RETRY_SECONDS", "15"))
RESULT_MAX_CHARS = 200_000
CONSUMER_READ_TIMEOUT = 290  # just under the Lambda timeout

table = boto3.resource("dynamodb").Table(JOBS_TABLE)
sqs = boto3.client("sqs")
agentcore = boto3.client("bedrock-agentcore")
http = urllib3.PoolManager()
_session = boto3.Session()


class Throttled(Exception):
    def __init__(self, retry_after: int, detail: str = ""):
        super().__init__(f"throttled, retry after {retry_after}s {detail}")
        self.retry_after = retry_after


class JobAlreadyHandled(Exception):
    """Duplicate delivery: another execution completed or owns this job."""


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _signed_headers(method: str, url: str, body: str, service: str, extra: dict | None = None) -> dict:
    headers = {"content-type": "application/json"}
    if extra:
        headers.update(extra)
    request = AWSRequest(method=method, url=url, data=body, headers=headers)
    credentials = _session.get_credentials().get_frozen_credentials()
    SigV4Auth(credentials, service, REGION).add_auth(request)
    return dict(request.headers)


# --- push channel -----------------------------------------------------------


def publish_events(job_id: str, events: list[dict]) -> None:
    url = f"https://{EVENTS_HTTP_DOMAIN}/event"
    for start in range(0, len(events), 5):  # publish accepts at most 5 events
        body = json.dumps(
            {
                "channel": f"/{CHANNEL_NAMESPACE}/{job_id}",
                "events": [json.dumps(e) for e in events[start : start + 5]],
            }
        )
        headers = _signed_headers("POST", url, body, "appsync")
        # Long-lived Lambda sandboxes can hold stale keep-alive connections that
        # get reset by the peer; retrying the publish is safe (clients dedupe by seq).
        response = http.request(
            "POST", url, body=body, headers=headers,
            retries=urllib3.Retry(total=2, backoff_factor=0.2),
        )
        if response.status >= 300:
            print(f"[{job_id}] publish failed {response.status}: {response.data[:300]}")


def publish_status(job_id: str, status: str, **detail) -> None:
    publish_events(job_id, [{"type": "status", "jobId": job_id, "status": status, "ts": _now_ms(), **detail}])


# --- agent invocation (SSE streaming) ----------------------------------------


def _session_id(job_id: str) -> str:
    # New session per attempt: a retry must not collide with a dying session (409).
    return f"{job_id}-{uuid.uuid4()}"


def _iter_sse_payloads(stream) -> "iter":
    """Yield decoded `data:` payloads from an SSE byte stream."""
    buffer = b""
    for chunk in stream:
        buffer += chunk
        while b"\n\n" in buffer:
            event_block, buffer = buffer.split(b"\n\n", 1)
            data_lines = [line[5:].strip() for line in event_block.split(b"\n") if line.startswith(b"data:")]
            if data_lines:
                yield b"\n".join(data_lines).decode("utf-8")


def _parse_retry_after(body_bytes: bytes) -> int:
    try:
        return int(json.loads(body_bytes).get("retryAfter", DEFAULT_RETRY_SECONDS))
    except (json.JSONDecodeError, TypeError, ValueError):
        return DEFAULT_RETRY_SECONDS


def stream_agent_events(job_id: str, payload: dict):
    """Yield agent event dicts, regardless of invocation path."""
    body = json.dumps(payload)
    if INVOKE_MODE == "direct":
        try:
            response = agentcore.invoke_agent_runtime(
                agentRuntimeArn=RUNTIME_ARN,
                runtimeSessionId=_session_id(job_id),
                qualifier="DEFAULT",
                payload=body,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                raise Throttled(DEFAULT_RETRY_SECONDS, "runtime throttling") from e
            raise
        for payload_text in _iter_sse_payloads(response["response"].iter_chunks()):
            yield _decode_event(payload_text)
        return

    url = f"{GATEWAY_URL}/{TARGET_NAME}/invocations"
    response = None
    for attempt in range(2):  # one in-process retry for stale-connection resets
        headers = _signed_headers(
            "POST",
            url,
            body,
            "bedrock-agentcore",
            extra={
                "accept": "application/json, text/event-stream",
                # fresh session per attempt so a retry cannot hit a dying session (409)
                "x-amzn-bedrock-agentcore-runtime-session-id": _session_id(job_id),
            },
        )
        try:
            response = http.request(
                "POST",
                url,
                body=body,
                headers=headers,
                preload_content=False,
                retries=False,
                timeout=urllib3.Timeout(connect=10, read=CONSUMER_READ_TIMEOUT),
            )
            break
        except (urllib3.exceptions.ProtocolError, urllib3.exceptions.NewConnectionError):
            if attempt == 1:
                raise
            print(f"[{job_id}] connection error on invoke, retrying once")
    if response.status in (402, 429):
        retry_after = _parse_retry_after(response.read())
        raise Throttled(retry_after, f"gateway {response.status}")
    if response.status >= 300:
        raise RuntimeError(f"gateway invocation failed {response.status}: {response.read()[:500]}")
    for payload_text in _iter_sse_payloads(response.stream(1024)):
        yield _decode_event(payload_text)


def _decode_event(payload_text: str) -> dict:
    try:
        event = json.loads(payload_text)
    except json.JSONDecodeError:
        return {"type": "chunk", "text": payload_text}
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except json.JSONDecodeError:
            return {"type": "chunk", "text": event}
    return event if isinstance(event, dict) else {"type": "chunk", "text": str(event)}


# --- job state ----------------------------------------------------------------


def claim_job(job_id: str, deadline_ms: int) -> None:
    now = _now_ms()
    try:
        table.update_item(
            Key={"jobId": job_id},
            UpdateExpression=(
                "SET #s = :inprog, startedTs = :now, inProgressExpiry = :deadline, "
                "attempts = if_not_exists(attempts, :zero) + :one"
            ),
            ConditionExpression=(
                "attribute_exists(jobId) AND (#s IN (:queued, :throttled, :failed) "
                "OR (#s = :inprog AND inProgressExpiry < :now))"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":inprog": "IN_PROGRESS",
                ":queued": "QUEUED",
                ":throttled": "THROTTLED",
                ":failed": "FAILED",
                ":now": now,
                ":deadline": deadline_ms,
                ":zero": 0,
                ":one": 1,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        current = table.get_item(Key={"jobId": job_id}).get("Item", {})
        raise JobAlreadyHandled(f"status={current.get('status')}") from e


def mark(job_id: str, status: str, **attrs) -> None:
    names = {"#s": "status"}
    values = {":s": status}
    sets = ["#s = :s"]
    for i, (key, value) in enumerate(attrs.items()):
        names[f"#a{i}"] = key
        values[f":v{i}"] = value
        sets.append(f"#a{i} = :v{i}")
    table.update_item(
        Key={"jobId": job_id},
        UpdateExpression="SET " + ", ".join(sets),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


# --- record processing ----------------------------------------------------------


def process_record(record: dict, context) -> None:
    message = json.loads(record["body"])
    job_id = message["jobId"]
    receive_ts = _now_ms()
    sent_ts = int(record["attributes"].get("SentTimestamp", receive_ts))
    deadline_ms = receive_ts + int(context.get_remaining_time_in_millis())

    try:
        claim_job(job_id, deadline_ms)
    except JobAlreadyHandled as e:
        print(f"[{job_id}] duplicate delivery ignored ({e})")
        return

    publish_status(job_id, "started", queueWaitMs=receive_ts - sent_ts, attempt=int(record["attributes"].get("ApproximateReceiveCount", 1)))

    payload = {
        "jobId": job_id,
        "prompt": message.get("prompt", ""),
        "mock": message.get("mock", False),
        "mockChunks": message.get("mockChunks", 20),
        "mockIntervalMs": message.get("mockIntervalMs", 250),
    }

    invoke_ts = _now_ms()
    first_chunk_ts = None
    last_chunk_ts = None
    chunk_count = 0
    text_parts: list[str] = []

    try:
        for event in stream_agent_events(job_id, payload):
            relay_ts = _now_ms()
            event.setdefault("jobId", job_id)
            event["relayTs"] = relay_ts
            if event.get("type") == "chunk":
                chunk_count += 1
                first_chunk_ts = first_chunk_ts or relay_ts
                last_chunk_ts = relay_ts
                text_parts.append(event.get("text", ""))
            publish_events(job_id, [event])
    except Throttled as e:
        mark(job_id, "THROTTLED", lastThrottleTs=_now_ms(), retryAfter=e.retry_after)
        publish_status(job_id, "throttled", retryAfter=e.retry_after)
        raise
    except Exception as e:
        mark(job_id, "FAILED", lastError=str(e)[:1000], failedTs=_now_ms())
        publish_status(job_id, "retrying", error=str(e)[:200])
        raise

    mark(
        job_id,
        "COMPLETED",
        completedTs=_now_ms(),
        chunkCount=chunk_count,
        result="".join(text_parts)[:RESULT_MAX_CHARS],
        metrics={
            "sentTs": sent_ts,
            "receiveTs": receive_ts,
            "invokeTs": invoke_ts,
            "firstChunkTs": first_chunk_ts,
            "lastChunkTs": last_chunk_ts,
            "queueWaitMs": receive_ts - sent_ts,
            "timeToFirstChunkMs": (first_chunk_ts - invoke_ts) if first_chunk_ts else None,
        },
    )


def _set_visibility(record: dict, delay_seconds: int) -> None:
    try:
        sqs.change_message_visibility(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=record["receiptHandle"],
            VisibilityTimeout=min(delay_seconds, 43199),
        )
    except ClientError as e:
        print(f"change_message_visibility failed: {e}")


def lambda_handler(event: dict, context) -> dict:
    failures = []
    for record in event["Records"]:
        attempt = int(record["attributes"].get("ApproximateReceiveCount", 1))
        try:
            process_record(record, context)
        except Throttled as e:
            # Honor the server's retryAfter; jitter (scaled by attempt) spreads a
            # burst so retries don't return in lockstep. Don't multiply retryAfter
            # itself — the server already said when capacity frees up.
            _set_visibility(record, e.retry_after + random.randint(0, 2 + 2 * attempt))
            failures.append({"itemIdentifier": record["messageId"]})
        except Exception as e:
            traceback.print_exc()
            print(f"record {record['messageId']} failed: {e}")
            _set_visibility(record, ERROR_RETRY_SECONDS * attempt)
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
