import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def check_aws_connection():
    try:
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()

        return {
            "status": "SUCCESS",
            "account": identity.get("Account"),
            "arn": identity.get("Arn")
        }

    except NoCredentialsError:
        return {
            "status": "FAIL",
            "error": "No AWS credentials found"
        }

    except ClientError as e:
        return {
            "status": "FAIL",
            "error": str(e)
        }
        
print(check_aws_connection())