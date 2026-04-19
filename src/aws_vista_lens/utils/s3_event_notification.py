import boto3
from aws_vista_lens import settings

s3 = boto3.client("s3")

s3.put_bucket_notification_configuration(
    Bucket=settings.S3_BUCKET_NAME,
    NotificationConfiguration={
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn": f"arn:aws:lambda:{settings.AWS_REGION}:{settings.AWS_ACCOUNT_ID}:function:{settings.LAMBDA_FUNCTION_NAME}",
                "Events": ["s3:ObjectCreated:*"]
            }
        ]
    }
)


print(s3.get_bucket_notification_configuration(Bucket=settings.S3_BUCKET_NAME))
