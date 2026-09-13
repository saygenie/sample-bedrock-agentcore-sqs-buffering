#!/usr/bin/env bash
# One-command deploy: build the agent image, deploy the full stack, print outputs.
set -euo pipefail
cd "$(dirname "$0")/../infra"

if [ ! -d .venv ]; then
  python3 -m venv .venv
  .venv/bin/pip install --quiet -r requirements.txt
fi
source .venv/bin/activate

CDK="cdk"
command -v cdk >/dev/null 2>&1 || CDK="npx -y aws-cdk"

$CDK deploy --require-approval never --outputs-file ../outputs/stack-outputs.local.json "$@"
echo
echo "Outputs written to outputs/stack-outputs.local.json"
