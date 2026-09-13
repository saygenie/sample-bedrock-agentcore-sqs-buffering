#!/usr/bin/env bash
# Scenario 5 (§6): full round trip — one-command deploy, automated acceptance
# scenarios, one-command cleanup, residual-resource check.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==== [1/4] deploy ===="
./scripts/deploy.sh

echo "==== [2/4] acceptance scenarios 1 & 2 ===="
python3 -m venv .venv-tests 2>/dev/null || true
.venv-tests/bin/pip install --quiet -r tests/requirements.txt
.venv-tests/bin/python tests/run_scenarios.py 1 2

echo "==== [3/4] destroy ===="
./scripts/destroy.sh

echo "==== [4/4] residual resource check (scoped to this stack's names only) ===="
FAIL=0

STACK_STATUS=$(aws cloudformation describe-stacks --stack-name AgentCoreSqsBuffering \
  --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "DELETED")
if [ "$STACK_STATUS" = "DELETED" ] || [ "$STACK_STATUS" = "DELETE_COMPLETE" ]; then
  echo "OK: CloudFormation stack fully deleted."
else
  echo "FAIL: stack still present with status $STACK_STATUS" >&2; FAIL=1
fi

LOG_GROUPS=$(aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/AgentCoreSqsBuffering-" \
  --query "length(logGroups)" --output text 2>/dev/null || echo 0)
if [ "$LOG_GROUPS" = "0" ]; then
  echo "OK: no leftover Lambda log groups."
else
  echo "FAIL: $LOG_GROUPS leftover log group(s) under /aws/lambda/AgentCoreSqsBuffering-" >&2; FAIL=1
fi

RUNTIMES=$(aws bedrock-agentcore-control list-agent-runtimes \
  --query "length(agentRuntimes[?agentRuntimeName=='sqs_buffering_sample_agent'])" --output text 2>/dev/null || echo 0)
GATEWAYS=$(aws bedrock-agentcore-control list-gateways \
  --query "length(items[?name=='sqs-buffering-sample-gw'])" --output text 2>/dev/null || echo 0)
if [ "$RUNTIMES" = "0" ] && [ "$GATEWAYS" = "0" ]; then
  echo "OK: no leftover AgentCore runtime/gateway."
else
  echo "FAIL: leftover AgentCore resources (runtimes=$RUNTIMES gateways=$GATEWAYS)" >&2; FAIL=1
fi

echo "Note: the shared CDK bootstrap stack and its ECR asset images are intentionally left in place."
exit $FAIL
