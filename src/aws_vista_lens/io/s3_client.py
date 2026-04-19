import boto3
import json


class S3Client:
    def __init__(self):
        self.s3 = boto3.client("s3")

    def create_bucket(self, bucket_name: str, region: str):
        self.s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})

    def list_buckets(self):
        response = self.s3.list_buckets()
        return [bucket["Name"] for bucket in response.get("Buckets", [])]
    
    def upload_file(self, bucket_name: str, local_path: str, key: str):
        self.s3.upload_file(local_path, bucket_name, key)

    def download_file(self, bucket_name: str, key: str, local_path: str):
        self.s3.download_file(bucket_name, key, local_path)

    def put_json(self, bucket_name: str, key: str, data: dict):
        self.s3.put_object(Bucket=bucket_name,
                           Key=key,
                           Body=json.dumps(data).encode("utf-8"),
                           ContentType="application/json")

    def move(self, bucket_name: str, source_key: str, dest_key: str):
        self.s3.copy_object(Bucket=bucket_name,
                            CopySource={"Bucket": bucket_name, "Key": source_key},
                            Key=dest_key)
        self.s3.delete_object(Bucket=bucket_name, Key=source_key)