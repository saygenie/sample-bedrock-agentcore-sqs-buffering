# AgentCore Runtime requires linux/arm64 images serving HTTP on port 8080
# (POST /invocations, GET /ping) — BedrockAgentCoreApp implements the contract.
FROM --platform=linux/arm64 python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080
CMD ["python", "app.py"]
