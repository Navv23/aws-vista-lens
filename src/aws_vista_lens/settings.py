import os
from pathlib import Path

# Load .env file if it exists
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                os.environ[key] = value

# AWS Configuration
AWS_REGION = os.environ.get('AWS_REGION', 'ap-south-1')
AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '562125663402')

# S3 Configuration
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'aws-vista-lens')
S3_SCRIPT_LOCATION = f"s3://{S3_BUCKET_NAME}/scripts/dq_metrics.py"

# Glue Configuration
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'dq-metrics-job')
GLUE_ROLE_NAME = os.environ.get('GLUE_ROLE_NAME', 'project1-glue-role')

# Lambda Configuration
LAMBDA_FUNCTION_NAME = os.environ.get('LAMBDA_FUNCTION_NAME', 's3-trigger-fn')
LAMBDA_ROLE_ARN = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/lambda-execution-role"