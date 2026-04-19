# import boto3

# s3 = boto3.client("s3")

# s3.put_bucket_notification_configuration(
#     Bucket="aws-vista-lens",
#     NotificationConfiguration={
#         "LambdaFunctionConfigurations": [
#             {
#                 "LambdaFunctionArn": "arn:aws:lambda:ap-south-1:562125663402:function:s3-trigger-fn",
#                 "Events": ["s3:ObjectCreated:*"]
#             }
#         ]
#     }
# )


# print(s3.get_bucket_notification_configuration(Bucket="aws-vista-lens"))
