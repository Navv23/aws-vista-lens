import boto3
import zipfile
import textwrap
from aws_vista_lens import settings


class LambdaManager:
    def __init__(self, function_name, role_arn, bucket_name, region=settings.AWS_REGION):
        self.function_name = function_name
        self.role_arn = role_arn
        self.bucket_name = bucket_name
        self.region = region
        self.lambda_client = boto3.client("lambda", region_name=region)

    def _build_zip(self, zip_path="lambda.zip"):
        code = textwrap.dedent(f"""
            import json
            import boto3
            import uuid

            s3 = boto3.client("s3")

            def lambda_handler(event, context):
                print("EVENT:", json.dumps(event))

                try:
                    # Extract S3 details
                    record = event['Records'][0]
                    bucket = record['s3']['bucket']['name']
                    key = record['s3']['object']['key']

                    print(f"Received file: s3://{{bucket}}/{{key}}")

                    # Validate file type
                    if not key.endswith(".csv"):
                        print("Rejected: Not a CSV file")
                        return {{"statusCode": 400}}

                    # Validate file size
                    meta = s3.head_object(Bucket=bucket, Key=key)

                    if meta['ContentLength'] == 0:
                        print("Rejected: Empty file")
                        return {{"statusCode": 400}}

                    print("Validation passed")

                    glue = boto3.client("glue")

                    run_id = str(uuid.uuid4())
                    glue.start_job_run(
                        JobName="{settings.GLUE_JOB_NAME}",
                        Arguments={{
                            "--s3_path": f"s3://{{bucket}}/{{key}}",
                            "--run_id": run_id
                        }}
                    )

                    print("Glue job triggered")

                    return {{"statusCode": 200}}

                except Exception as e:
                    print("Error:", str(e))
                    raise
        """)

        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("lambda_function.py", code)

        with open(zip_path, "rb") as f:
            return f.read()

    def create_function(self):
        zipped_code = self._build_zip()

        response = self.lambda_client.create_function(FunctionName=self.function_name,
                                                      Runtime="python3.12",
                                                      Role=self.role_arn,
                                                      Handler="lambda_function.lambda_handler",
                                                      Code={"ZipFile": zipped_code},
                                                      Timeout=30,
                                                      MemorySize=128)

        print("Lambda created:", response["FunctionArn"])
        return response

    def update_code(self):
        zipped_code = self._build_zip()

        response = self.lambda_client.update_function_code(FunctionName=self.function_name, ZipFile=zipped_code)
        print("Lambda updated")
        return response

    def add_s3_permission(self, statement_id="s3invoke"):
        try:
            response = self.lambda_client.add_permission(FunctionName=self.function_name,
                                                         StatementId=statement_id,
                                                         Action="lambda:InvokeFunction",
                                                         Principal="s3.amazonaws.com",
                                                         SourceArn=f"arn:aws:s3:::{self.bucket_name}")
            print("Permission added")
            return response
        except self.lambda_client.exceptions.ResourceConflictException:
            print("Permission already exists")

    def get_policy(self):
        response = self.lambda_client.get_policy(FunctionName=self.function_name)
        print(response["Policy"])
        return response
    

if __name__ == "__main__":  
    manager = LambdaManager(function_name=settings.LAMBDA_FUNCTION_NAME,
                            role_arn=settings.LAMBDA_ROLE_ARN,
                            bucket_name=settings.S3_BUCKET_NAME)

    # # Create Lambda
    # manager.create_function()

    # Update code
    manager.update_code()

    # # Add S3 trigger permission
    # manager.add_s3_permission()

    # Verify policy
    manager.get_policy()