#!/usr/bin/env python3
"""Acceptance tests for the §6 scenarios in PLAN.md — every pass/fail judgment
is made on measured numbers, not log inspection.

Usage:
    python tests/run_scenarios.py            # scenarios 1-4
    python tests/run_scenarios.py 2 3        # subset

Reads stack outputs from outputs/stack-outputs.local.json (written by
scripts/deploy.sh). Requires: boto3, urllib3, websockets.

Measurement note: timestamps from different machines carry clock skew, so
progressive-arrival checks use *inter-arrival gaps on the local clock* and
concurrency checks use *consumer-side timestamps* (one clock each).
"""

import asyncio
import base64
import json
import statistics
import sys
import time
import uuid
from pathlib import Path

import boto3
import urllib3
import websockets
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

REGION = "us-west-2"
OUTPUTS_PATH = Path(__file__).parent.parent / "outputs" / "stack-outputs.local.json"

outputs = json.loads(OUTPUTS_PATH.read_text())["AgentCoreSqsBuffering"]
INGEST = outputs["IngestEndpoint"]
EVENTS_HTTP = outputs["EventsHttpDomain"]
EVENTS_REALTIME = outputs["EventsRealtimeDomain"]
API_KEY = outputs["EventsApiKey"]
DLQ_URL = outputs["DlqUrl"]
GATEWAY_ID = outputs["GatewayUrl"].split("//")[1].split(".")[0]

http = urllib3.PoolManager()
session = boto3.Session(region_name=REGION)
sqs = session.client("sqs")
control = session.client("bedrock-agentcore-control")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    url = INGEST + path
    data = json.dumps(body) if body else ""
    request = AWSRequest(method=method, url=url, data=data, headers={"content-type": "application/json"})
    credentials = session.get_credentials().get_frozen_credentials()
    SigV4Auth(credentials, "execute-api", REGION).add_auth(request)
    response = http.request(method, url, body=data or None, headers=dict(request.headers))
    return response.status, json.loads(response.data)


def dlq_depth() -> int:
    attrs = sqs.get_queue_attributes(
        QueueUrl=DLQ_URL,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]) + int(attrs["ApproximateNumberOfMessagesNotVisible"])


def wait_for_jobs(job_ids: list[str], timeout_s: int) -> dict[str, dict]:
    """Poll the job store until every job reaches a terminal state."""
    deadline = time.time() + timeout_s
    results: dict[str, dict] = {}
    pending = set(job_ids)
    while pending and time.time() < deadline:
        time.sleep(2)
        for job_id in list(pending):
            _, item = api("GET", f"/jobs/{job_id}")
            if item.get("status") in ("COMPLETED", "FAILED"):
                results[job_id] = item
                pending.discard(job_id)
    for job_id in pending:
        _, results[job_id] = api("GET", f"/jobs/{job_id}")
    return results


class Subscriber:
    """AppSync Events WebSocket subscriber recording local arrival timestamps."""

    def __init__(self):
        self.ws = None
        self.received: dict[str, list[dict]] = {}

    async def connect(self):
        auth = self._auth()
        subprotocols = [
            "aws-appsync-event-ws",
            "header-" + base64.urlsafe_b64encode(json.dumps(auth).encode()).decode().rstrip("="),
        ]
        self.ws = await websockets.connect(f"wss://{EVENTS_REALTIME}/event/realtime", subprotocols=subprotocols)
        await self.ws.send(json.dumps({"type": "connection_init"}))
        await self._recv_until({"connection_ack"})

    @staticmethod
    def _auth() -> dict:
        return {"host": EVENTS_HTTP, "x-api-key": API_KEY}

    async def _recv_until(self, types: set, timeout: int = 30) -> dict:
        while True:
            message = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=timeout))
            if message["type"] in types:
                return message
            if message["type"] == "data":
                self._store(message)

    def _store(self, message: dict) -> None:
        event = json.loads(message["event"])
        event["arrivalTs"] = now_ms()
        self.received.setdefault(event.get("jobId", "?"), []).append(event)

    async def subscribe(self, job_id: str):
        sub_id = str(uuid.uuid4())
        await self.ws.send(json.dumps(
            {"type": "subscribe", "id": sub_id, "channel": f"/jobs/{job_id}", "authorization": self._auth()}
        ))
        message = await self._recv_until({"subscribe_success", "subscribe_error"})
        assert message["type"] == "subscribe_success", message

    async def pump(self, seconds: float):
        """Receive events for a fixed duration."""
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                message = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=max(0.1, deadline - time.time())))
            except (asyncio.TimeoutError, TimeoutError):
                break
            if message["type"] == "data":
                self._store(message)

    async def pump_until_end(self, job_ids: set, timeout_s: int):
        ended: set = set()
        deadline = time.time() + timeout_s
        while ended < job_ids and time.time() < deadline:
            try:
                message = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=max(0.1, deadline - time.time())))
            except (asyncio.TimeoutError, TimeoutError):
                break
            if message["type"] == "data":
                self._store(message)
                event = json.loads(message["event"])
                if event.get("type") == "end":
                    ended.add(event["jobId"])

    async def close(self):
        if self.ws:
            await self.ws.close()


def submit(job_id: str, **params) -> None:
    status, body = api("POST", "/jobs", {"jobId": job_id, **params})
    assert status == 202, (status, body)


def verdict(name: str, ok: bool, detail: str) -> bool:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")
    return ok


# --------------------------------------------------------------------------- #
async def scenario_1() -> bool:
    """Single-job streaming: chunks must arrive progressively, spaced like the
    agent's emission interval — not in one clump."""
    print("\n[1] 단건 스트리밍 — 점진 도착")
    chunks, interval_ms = 20, 250
    sub = Subscriber()
    await sub.connect()
    job_id = str(uuid.uuid4())
    await sub.subscribe(job_id)
    submit(job_id, mock=True, mockChunks=chunks, mockIntervalMs=interval_ms)
    await sub.pump_until_end({job_id}, timeout_s=120)
    await sub.close()

    chunk_events = [e for e in sub.received.get(job_id, []) if e.get("type") == "chunk"]
    arrivals = [e["arrivalTs"] for e in chunk_events]
    seqs = [int(e["seq"]) for e in chunk_events]
    ok = verdict("chunk delivery", len(arrivals) == chunks, f"received {len(arrivals)}/{chunks}")
    ok &= verdict("arrival order", seqs == sorted(seqs), f"sequence as arrived: monotonic={seqs == sorted(seqs)}")
    gaps = [b - a for a, b in zip(arrivals, arrivals[1:])]
    emission_span = (chunks - 1) * interval_ms
    arrival_span = arrivals[-1] - arrivals[0] if len(arrivals) >= 2 else 0
    ratio = arrival_span / emission_span if emission_span else 0
    ok &= verdict(
        "progressive arrival",
        ratio >= 0.6 and max(gaps, default=0) <= 4 * interval_ms,
        f"arrival span {arrival_span}ms vs emission span {emission_span}ms (ratio {ratio:.2f}), "
        f"gaps mean {statistics.mean(gaps):.0f}ms / max {max(gaps)}ms (emission interval {interval_ms}ms)",
    )
    return ok


async def scenario_2(n_jobs: int = 30, max_concurrency: int = 10) -> bool:
    """Burst buffering: N jobs at once — all complete, zero loss, zero DLQ,
    processing advances in waves bounded by the consumer concurrency limit."""
    print(f"\n[2] 버스트 버퍼링 — {n_jobs}건 일괄 투입")
    dlq_before = dlq_depth()
    job_ids = [str(uuid.uuid4()) for _ in range(n_jobs)]
    for job_id in job_ids:
        submit(job_id, mock=True, mockChunks=10, mockIntervalMs=200)
    # Generous window: a job that loses its stream to a transient network reset
    # is retried by SQS (this is the pattern working) and must still count.
    results = wait_for_jobs(job_ids, timeout_s=420)

    completed = [r for r in results.values() if r.get("status") == "COMPLETED"]
    for job_id, r in results.items():
        if r.get("status") != "COMPLETED":
            print(f"  straggler {job_id}: status={r.get('status')} attempts={r.get('attempts')} "
                  f"retryAfter={r.get('retryAfter')} lastError={str(r.get('lastError'))[:120]}")
    ok = verdict("completion", len(completed) == n_jobs, f"{len(completed)}/{n_jobs} COMPLETED, loss 0")
    ok &= verdict("DLQ", dlq_depth() == dlq_before, f"depth delta {dlq_depth() - dlq_before}")

    # Peak overlap of consumer-side streaming windows (single clock) must
    # respect the pacing limit; a burst must not start all at once.
    windows = sorted(
        (int(r["metrics"]["invokeTs"]), int(r["metrics"]["lastChunkTs"])) for r in completed if r.get("metrics")
    )
    events = sorted([(s, 1) for s, _ in windows] + [(e, -1) for _, e in windows])
    peak = level = 0
    for _, delta in events:
        level += delta
        peak = max(peak, level)
    start_spread_ms = windows[-1][0] - windows[0][0] if windows else 0
    ok &= verdict(
        "pacing",
        peak <= max_concurrency and start_spread_ms > 1000,
        f"peak concurrent streams {peak} (limit {max_concurrency}), start spread {start_spread_ms}ms — "
        f"{n_jobs} jobs advanced in ~{-(-n_jobs // max_concurrency)} waves",
    )
    return ok


async def scenario_3(n_jobs: int = 12) -> bool:
    """Throttle absorption: drive the gateway past a lowered rate limit; 429s
    must be retried internally, the client sees only status + delay, and the
    final completion rate is 100%.

    Measured limiter behavior (see DESIGN.md §5-3): enforcement is bursty, not
    strict — some of a concurrent burst passes even far above the limit — so the
    test asserts that *some* 429s occur and are absorbed, not an exact count.
    At 1 request/second the burst tolerance absorbs everything (0 throttles
    measured), so the test uses 1/minute — which reliably produced 429s in two
    measured runs; the consumer's retryAfter+jitter backoff drains the backlog
    in one retryAfter cycle per throttle.
    Update propagation measured ~90s (docs say ≤30s), hence the generous wait."""
    print(f"\n[3] 스로틀 흡수 — rate limit 1/minute로 강하 후 {n_jobs}건 버스트")
    rate_limits = control.list_gateway_rate_limits(gatewayIdentifier=GATEWAY_ID)["rateLimits"]
    rate_limit_id = rate_limits[0]["rateLimitId"]
    baseline_entries = [
        {key: entry[key] for key in ("dimensions", "requests", "connections", "tokens") if key in entry}
        for entry in rate_limits[0]["entries"]
    ]
    control.update_gateway_rate_limit(
        gatewayIdentifier=GATEWAY_ID,
        rateLimitId=rate_limit_id,
        entries=[{"dimensions": {"targetName": "*"}, "requests": [{"rate": 1, "period": "minute"}]}],
    )
    print("  rate limit lowered (1/minute); waiting 90s for data-plane propagation")
    await asyncio.sleep(90)

    try:
        dlq_before = dlq_depth()
        sub = Subscriber()
        await sub.connect()
        job_ids = [str(uuid.uuid4()) for _ in range(n_jobs)]
        for job_id in job_ids:
            await sub.subscribe(job_id)
        for job_id in job_ids:
            submit(job_id, mock=True, mockChunks=5, mockIntervalMs=200)
        await sub.pump_until_end(set(job_ids), timeout_s=360)
        await sub.close()
        results = wait_for_jobs(job_ids, timeout_s=180)
    finally:
        control.update_gateway_rate_limit(
            gatewayIdentifier=GATEWAY_ID,
            rateLimitId=rate_limit_id,
            entries=baseline_entries,
        )
        print("  rate limit restored")

    throttle_events = [
        e for events in sub.received.values() for e in events
        if e.get("type") == "status" and e.get("status") == "throttled"
    ]
    completed = sum(1 for r in results.values() if r.get("status") == "COMPLETED")
    retry_afters = sorted({e.get("retryAfter") for e in throttle_events})
    ok = verdict("throttling occurred", len(throttle_events) > 0,
                 f"{len(throttle_events)} throttled status events, retryAfter values {retry_afters}")
    ok &= verdict("absorption", completed == n_jobs, f"{completed}/{n_jobs} COMPLETED despite 429s")
    ok &= verdict("DLQ", dlq_depth() == dlq_before, f"depth delta {dlq_depth() - dlq_before}")
    return ok


async def scenario_4() -> bool:
    """Reconnect recovery: drop the push connection mid-stream — the job must
    finish anyway, and the result must be retrievable by job id."""
    print("\n[4] 재접속 복구 — 스트림 도중 연결 절단")
    chunks = 30
    sub = Subscriber()
    await sub.connect()
    job_id = str(uuid.uuid4())
    await sub.subscribe(job_id)
    submit(job_id, mock=True, mockChunks=chunks, mockIntervalMs=500)
    await sub.pump(seconds=6)  # receive a few chunks...
    received_before_drop = len([e for e in sub.received.get(job_id, []) if e.get("type") == "chunk"])
    await sub.close()          # ...then hang up mid-stream
    print(f"  dropped connection after {received_before_drop} chunks")

    result = wait_for_jobs([job_id], timeout_s=120)[job_id]
    ok = verdict(
        "job survived disconnect",
        result.get("status") == "COMPLETED" and int(result.get("chunkCount", 0)) == chunks,
        f"status {result.get('status')}, server-side chunkCount {result.get('chunkCount')}/{chunks} "
        f"(client saw only {received_before_drop} before dropping)",
    )
    ok &= verdict("result retrievable", bool(result.get("result")), f"result length {len(result.get('result', ''))}")
    return ok


SCENARIOS = {1: scenario_1, 2: scenario_2, 3: scenario_3, 4: scenario_4}


async def main():
    wanted = [int(a) for a in sys.argv[1:]] or sorted(SCENARIOS)
    outcomes = {}
    for number in wanted:
        outcomes[number] = await SCENARIOS[number]()
    print("\n==== summary ====")
    for number, ok in outcomes.items():
        print(f"  scenario {number}: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if all(outcomes.values()) else 1)


if __name__ == "__main__":
    asyncio.run(main())
