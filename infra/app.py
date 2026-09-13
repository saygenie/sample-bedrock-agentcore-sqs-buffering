#!/usr/bin/env python3
import os

import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks
from stacks.buffering_stack import BufferingStack

app = cdk.App()
BufferingStack(
    app,
    "AgentCoreSqsBuffering",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
)
cdk.Aspects.of(app).add(AwsSolutionsChecks())
app.synth()
