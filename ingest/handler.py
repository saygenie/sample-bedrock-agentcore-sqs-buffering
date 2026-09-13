"""Ingest API: accept a job, enqueue it, return an acknowledgement immediately.

POST /jobs          -> validate, persist QUEUED, send to SQS, 202 {jobId}
GET  /jobs/{jobId}  -> job status/result lookup (reconnect-recovery path)

The client generates jobId and subscribes to the push channel BEFORE
submitting, so no chunk published later can be missed.
"""

import json
import os
import re
import time
import uuid

import boto3
from botocore.exceptions import ClientError

JOBS_TABLE = os.environ["JOBS_TABLE"]
QUEUE_URL = os.environ["QUEUE_URL"]
JOB_TTL_SECONDS = 7 * 24 * 3600
JOB_ID_PATTERN = re.compile(r"^[a-zA-Z0-9-]{8,64}$")

table = boto3.resource("dynamodb").Table(JOBS_TABLE)
sqs = boto3.client("sqs")


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _submit(event: dict) -> dict:
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "invalid JSON body"})

    job_id = body.get("jobId") or str(uuid.uuid4())
    if not JOB_ID_PATTERN.match(job_id):
        return _response(400, {"error": "jobId must match ^[a-zA-Z0-9-]{8,64}$"})

    submitted_ts = _now_ms()
    message = {
        "jobId": job_id,
        "prompt": str(body.get("prompt", ""))[:4000],
        "mock": bool(body.get("mock", False)),
        "mockChunks": int(body.get("mockChunks", 20)),
        "mockIntervalMs": int(body.get("mockIntervalMs", 250)),
        "submittedTs": submitted_ts,
    }

    try:
        table.put_item(
            Item={
                "jobId": job_id,
                "status": "QUEUED",
                "submittedTs": submitted_ts,
                "request": message,
                "expiresAt": submitted_ts // 1000 + JOB_TTL_SECONDS,
            },
            ConditionExpression="attribute_not_exists(jobId)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return _response(409, {"error": "jobId already exists", "jobId": job_id})
        raise

    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(message))
    return _response(202, {"jobId": job_id, "status": "QUEUED", "submittedTs": submitted_ts})


def _get_job(job_id: str) -> dict:
    item = table.get_item(Key={"jobId": job_id}).get("Item")
    if not item:
        return _response(404, {"error": "job not found", "jobId": job_id})
    # DynamoDB numbers arrive as Decimal; normalize for JSON.
    item = json.loads(json.dumps(item, default=lambda v: int(v) if v == int(v) else float(v)))
    return _response(200, item)


def lambda_handler(event: dict, _context) -> dict:
    method = event["requestContext"]["http"]["method"]
    path_params = event.get("pathParameters") or {}

    if method == "POST":
        return _submit(event)
    if method == "GET" and path_params.get("jobId"):
        return _get_job(path_params["jobId"])
    return _response(404, {"error": "route not found"})
