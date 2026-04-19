import boto3
from aws_vista_lens import settings

glue = boto3.client("glue")

response = glue.create_job(
    Name=settings.GLUE_JOB_NAME,
    Role=settings.GLUE_ROLE_NAME,
    ExecutionProperty={
        'MaxConcurrentRuns': 1
    },
    Command={
        'Name': 'glueetl',
        'ScriptLocation': settings.S3_SCRIPT_LOCATION,
        'PythonVersion': '3'
    },
    DefaultArguments={
        '--job-language': 'python',
        '--TempDir': f's3://{settings.S3_BUCKET_NAME}/temp/',
        '--enable-continuous-cloudwatch-log': 'true'
    },
    GlueVersion='4.0',
    NumberOfWorkers=2,
    WorkerType='G.1X'
)

print(response)