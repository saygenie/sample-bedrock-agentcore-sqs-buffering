#!/usr/bin/env bash
# One-command cleanup: destroys every stack resource (queues, table, runtime,
# gateway, events API). Scoped strictly to this stack — never scans the account.
set -euo pipefail
cd "$(dirname "$0")/../infra"

if [ -d .venv ]; then source .venv/bin/activate; fi

CDK="cdk"
command -v cdk >/dev/null 2>&1 || CDK="npx -y aws-cdk"

$CDK destroy --force "$@"

# AgentCore Runtime auto-creates log groups outside the stack; remove the ones
# belonging to this sample's uniquely named runtime (never touches anything else).
for lg in $(aws logs describe-log-groups \
    --log-group-name-prefix "/aws/bedrock-agentcore/runtimes/sqs_buffering_sample_agent-" \
    --query "logGroups[].logGroupName" --output text 2>/dev/null); do
  echo "deleting leftover log group: $lg"
  aws logs delete-log-group --log-group-name "$lg"
done
