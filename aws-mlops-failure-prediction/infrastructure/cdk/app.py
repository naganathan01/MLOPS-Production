# infrastructure/cdk/app.py
#!/usr/bin/env python3
import aws_cdk as cdk
from mlops_stack import MLOpsStack

app = cdk.App()

# Development environment
dev_stack = MLOpsStack(
    app, 
    "MLOpsFailurePredictionDev",
    env=cdk.Environment(
        account="123456789012",
        region="us-east-1"
    ),
    environment="dev"
)

# Production environment
prod_stack = MLOpsStack(
    app, 
    "MLOpsFailurePredictionProd",
    env=cdk.Environment(
        account="123456789012", 
        region="us-east-1"
    ),
    environment="prod"
)

app.synth()