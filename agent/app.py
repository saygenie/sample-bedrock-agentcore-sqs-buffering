"""AgentCore Runtime streaming agent.

Emits chunks progressively so the buffering/streaming demo can measure
chunk-level timing end to end. Every chunk carries `emitTs` (epoch ms at the
moment the agent yields it); downstream components add their own timestamps
(relayTs at the consumer, arrivalTs at the client) to prove "no buffering"
with numbers instead of logs.

Two modes, selected per request:
- real (default): streams a Bedrock LLM response via Strands Agents.
- mock (payload {"mock": true}): emits a fixed number of chunks at a fixed
  interval with zero model cost — used by deterministic acceptance tests.
"""

import asyncio
import os
import time

from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent
from strands.models import BedrockModel

MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
SYSTEM_PROMPT = (
    "You are a concise assistant in a streaming demo. "
    "Answer the user's question in a few short paragraphs."
)

app = BedrockAgentCoreApp()


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _chunk(job_id: str, seq: int, text: str) -> dict:
    return {"type": "chunk", "jobId": job_id, "seq": seq, "text": text, "emitTs": _now_ms()}


def _end(job_id: str, seq: int) -> dict:
    return {"type": "end", "jobId": job_id, "seq": seq, "chunkCount": seq, "emitTs": _now_ms()}


async def _mock_stream(job_id: str, payload: dict):
    chunk_count = int(payload.get("mockChunks", 20))
    interval_ms = int(payload.get("mockIntervalMs", 250))
    for seq in range(1, chunk_count + 1):
        yield _chunk(job_id, seq, f"mock chunk {seq}/{chunk_count} ")
        await asyncio.sleep(interval_ms / 1000)
    yield _end(job_id, chunk_count + 1)


async def _llm_stream(job_id: str, payload: dict):
    prompt = payload.get("prompt", "Explain why queueing and streaming can coexist.")
    agent = Agent(model=BedrockModel(model_id=MODEL_ID, streaming=True), system_prompt=SYSTEM_PROMPT)
    seq = 0
    async for event in agent.stream_async(prompt):
        text = event.get("data") if isinstance(event, dict) else None
        if text:
            seq += 1
            yield _chunk(job_id, seq, text)
    yield _end(job_id, seq + 1)


@app.entrypoint
async def invoke(payload: dict):
    job_id = payload.get("jobId", "unknown")
    stream = _mock_stream if payload.get("mock") else _llm_stream
    async for event in stream(job_id, payload):
        yield event


if __name__ == "__main__":
    app.run()
