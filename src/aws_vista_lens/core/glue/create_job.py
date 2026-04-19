import boto3
from aws_vista_lens import settings

glue = boto3.client("glue", region_name=settings.AWS_REGION)

response = glue.create_job(
    Name=settings.GLUE_JOB_NAME,
    Role=settings.GLUE_ROLE_NAME,
    Command={
        "Name": "glueetl",
        "ScriptLocation": settings.S3_SCRIPT_LOCATION,
        "PythonVersion": "3"
    },
    DefaultArguments={
        "--job-language": "python"
    },
    GlueVersion="3.0",
    WorkerType="G.1X",
    NumberOfWorkers=2
)

print("Created:", response["Name"])