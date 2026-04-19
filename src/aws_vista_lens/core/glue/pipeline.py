import boto3

glue = boto3.client("glue")

response = glue.create_job(
    Name="dq-metrics-job",
    Role="GlueServiceRole",
    ExecutionProperty={
        'MaxConcurrentRuns': 1
    },
    Command={
        'Name': 'glueetl',
        'ScriptLocation': 's3://your-bucket/scripts/dq_job.py',
        'PythonVersion': '3'
    },
    DefaultArguments={
        '--job-language': 'python',
        '--TempDir': 's3://your-bucket/temp/',
        '--enable-continuous-cloudwatch-log': 'true'
    },
    GlueVersion='4.0',
    NumberOfWorkers=2,
    WorkerType='G.1X'
)

print(response)