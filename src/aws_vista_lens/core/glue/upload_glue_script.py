from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.getOrCreate()

args = sys.argv
s3_path = args[args.index('--s3_path') + 1]

df = spark.read.csv(s3_path, header=True, inferSchema=True)

row_count = df.count()

print(f"Row count: {row_count}")