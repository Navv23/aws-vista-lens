from aws_vista_lens.io.s3_client import S3Client
from aws_vista_lens import settings

s3 = S3Client()

# s3.create_bucket(bucket_name=settings.S3_BUCKET_NAME, region=settings.AWS_REGION)
# print("Bucket created")

# print(s3.list_buckets())

s3.upload_file(bucket_name=settings.S3_BUCKET_NAME, local_path=r"/mnt/d/Programming/pyspark/customers-100000.csv", key="raw/customers-100000.csv")
print("Uploaded")


