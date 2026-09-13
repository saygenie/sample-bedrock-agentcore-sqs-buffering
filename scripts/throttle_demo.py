#!/usr/bin/env python3
"""Toggle a deliberately tiny Gateway rate limit so the browser demo shows
throttle absorption (scenario 3) live.

With the deployed defaults — rate limit 10 requests/second vs consumer pacing of
10 concurrent streams — throttling never happens no matter how many jobs you
submit: pacing keeps the request rate under the limit, which is the whole point
of the pattern. To *see* a 429 being absorbed you have to create a deliberate
mismatch by shrinking the limit.

    python3 scripts/throttle_demo.py on      # limit -> 1 request/minute
    python3 scripts/throttle_demo.py off     # restore the deployed limit
    python3 scripts/throttle_demo.py status

Requires boto3 (pip install -r tests/requirements.txt). Rate limit changes take
up to ~90s to reach the data plane (measured; the docs say 30s), so `on` waits.
"""

import json
import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
OUTPUTS_PATH = ROOT / "outputs" / "stack-outputs.local.json"
PROPAGATION_WAIT_S = 90

try:
    outputs = json.loads(OUTPUTS_PATH.read_text())["AgentCoreSqsBuffering"]
except FileNotFoundError:
    sys.exit(f"Stack outputs not found at {OUTPUTS_PATH} — run ./scripts/deploy.sh first.")

gateway_url = outputs["GatewayUrl"]
GATEWAY_ID = gateway_url.split("//")[1].split(".")[0]
REGION = gateway_url.split(".")[-3]
DEPLOYED_RPS = json.loads((ROOT / "infra" / "cdk.json").read_text())["context"].get("rateLimitRps", 10)

control = boto3.client("bedrock-agentcore-control", region_name=REGION)


def current() -> dict:
    limits = control.list_gateway_rate_limits(gatewayIdentifier=GATEWAY_ID)["rateLimits"]
    if not limits:
        sys.exit("No rate limit found on the gateway.")
    return limits[0]


def set_entries(rate: int, period: str) -> None:
    control.update_gateway_rate_limit(
        gatewayIdentifier=GATEWAY_ID,
        rateLimitId=current()["rateLimitId"],
        entries=[{"dimensions": {"targetName": "*"}, "requests": [{"rate": rate, "period": period}]}],
    )


def describe() -> str:
    return json.dumps(current()["entries"], default=str)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "status"

    if mode == "on":
        set_entries(1, "minute")
        print(f"Rate limit set to 1 request/minute. Waiting {PROPAGATION_WAIT_S}s for data-plane propagation...")
        time.sleep(PROPAGATION_WAIT_S)
        print("Ready — submit a burst in the browser. Expect yellow 'throttled' markers, those")
        print("rows starting ~60s later (the server's retryAfter), and every job still completing.")
        print("Restore afterwards with: python3 scripts/throttle_demo.py off")
    elif mode == "off":
        set_entries(DEPLOYED_RPS, "second")
        print(f"Restored to {DEPLOYED_RPS} requests/second.")
    elif mode == "status":
        print(describe())
    else:
        sys.exit(f"usage: {sys.argv[0]} {{on|off|status}}")


if __name__ == "__main__":
    main()
