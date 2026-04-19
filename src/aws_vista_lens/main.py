from aws_vista_lens.io.s3_client import S3Client

s3 = S3Client()

# s3.create_bucket(bucket_name="aws-vista-lens", region="ap-south-1")
# print("Bucket created")

# print(s3.list_buckets())

s3.upload_file(bucket_name="aws-vista-lens", local_path="/mnt/d/Programming/aws-vista-lens/stats.csv", key="raw/stats.csv")
print("Uploaded")


