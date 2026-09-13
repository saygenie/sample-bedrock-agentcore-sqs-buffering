#!/usr/bin/env python3
"""Local demo server.

Serves the static demo page AND signs ingest-API calls with your local AWS
credentials (default profile / AWS_PROFILE), so the browser never sees them:

    GET  /api/config     -> stack endpoints + Events API key (from outputs/)
    POST /api/jobs       -> SigV4-signed proxy to the ingest API
    GET  /api/jobs/{id}  -> SigV4-signed proxy to the ingest API
    everything else      -> static files from this directory

The page auto-detects /api/config and hides the credential inputs. Binds to
127.0.0.1 only. Requires boto3 + urllib3 (already in tests/requirements.txt).

Usage: python3 client/serve.py [port]      (default port 8765)
"""

import json
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import boto3
import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

CLIENT_DIR = Path(__file__).resolve().parent
OUTPUTS_PATH = CLIENT_DIR.parent / "outputs" / "stack-outputs.local.json"

try:
    outputs = json.loads(OUTPUTS_PATH.read_text())["AgentCoreSqsBuffering"]
except FileNotFoundError:
    sys.exit(f"Stack outputs not found at {OUTPUTS_PATH} — run ./scripts/deploy.sh first.")

INGEST = outputs["IngestEndpoint"].rstrip("/")
REGION = INGEST.split(".")[2]  # https://xxx.execute-api.<region>.amazonaws.com

http = urllib3.PoolManager()
session = boto3.Session()
if session.get_credentials() is None:
    sys.exit("No AWS credentials found — configure a profile or set AWS_PROFILE.")


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(CLIENT_DIR), **kwargs)

    def _respond(self, status: int, data: bytes) -> None:
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _proxy(self, method: str, path: str, body: str = "") -> None:
        url = INGEST + path
        request = AWSRequest(method=method, url=url, data=body, headers={"content-type": "application/json"})
        credentials = session.get_credentials().get_frozen_credentials()
        SigV4Auth(credentials, "execute-api", REGION).add_auth(request)
        upstream = http.request(method, url, body=body or None, headers=dict(request.headers))
        self._respond(upstream.status, upstream.data)

    def do_GET(self):  # noqa: N802 (http.server naming)
        if self.path == "/api/config":
            config = {
                "IngestEndpoint": INGEST,
                "EventsHttpDomain": outputs["EventsHttpDomain"],
                "EventsRealtimeDomain": outputs["EventsRealtimeDomain"],
                "EventsApiKey": outputs["EventsApiKey"],
                "MaxConcurrency": outputs.get("MaxConcurrency"),
                "Region": REGION,
            }
            self._respond(200, json.dumps(config).encode())
        elif self.path.startswith("/api/jobs/"):
            self._proxy("GET", self.path.removeprefix("/api"))
        else:
            super().do_GET()

    def do_POST(self):  # noqa: N802
        if self.path == "/api/jobs":
            length = int(self.headers.get("content-length", 0))
            self._proxy("POST", "/jobs", self.rfile.read(length).decode())
        else:
            self.send_error(404)

    def log_message(self, *_args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    print(f"Demo ready: http://127.0.0.1:{port}")
    print("Ingest calls are signed locally with your AWS credentials; the browser never sees them.")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
