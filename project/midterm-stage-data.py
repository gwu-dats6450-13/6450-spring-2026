"""
DATS 6450 — Applied Big Data Analytics, Spring 2026
Midterm 2 — INSTRUCTOR: One-shot data staging script

Reads a small slice of the full Reddit dataset from the requester-pays
source bucket, filters to selected subreddits + one month, and writes
to a public-read midterm bucket that all students can read.

Run ONCE before the midterm (not during class):
    uv run python project/midterm-stage-data.py spark://<MASTER_PRIVATE_IP>:7077

Prerequisites:
    1. The midterm S3 bucket must already exist and have a public-read policy:
           aws s3 mb s3://dats6450-midterm-s2026 --region us-east-1
       Then apply a bucket policy that allows s3:GetObject for all principals.

    2. Your IAM role (LabInstanceProfile) must have PutObject rights on the
       destination bucket.

Output path:
    s3a://dats6450-midterm-s2026/reddit/comments/
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
MASTER_URL = sys.argv[1] if len(sys.argv) > 1 else "local[*]"

# Source: requester-pays bucket (instructor pays)
SOURCE_PATH = "s3a://adg-reddit-data/parquet/comments/yyyy=2023/mm=08/"

# Destination: public-read midterm bucket (all students read this)
DEST_PATH = "s3a://dats6450-midterm-s2026/reddit/comments/"

# Subreddits that span a range of tones/topics (~200K rows for Aug 2023)
SUBREDDITS = [
    "politics",
    "AskReddit",
    "MachineLearning",
    "movies",
    "news",
]
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder.master(MASTER_URL)
    .appName("DATS6450-Midterm2-StageData")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider",
    )
    #    .config(
    #        "spark.hadoop.fs.s3a.aws.credentials.provider",
    #        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    #    )
    #    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
    #   "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    # Requester-pays for source bucket
    .config("spark.hadoop.fs.s3a.requester.pays.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Reading source data from {SOURCE_PATH} ...")
df = spark.read.parquet(SOURCE_PATH)

print(f"Total rows in August 2023: {df.count():,}")

# Select only the columns students need (keep schema clean)
KEEP_COLS = [
    "id",
    "subreddit",
    "author",
    "body",
    "score",
    "created_utc",
    "controversiality",
    "parent_id",
    "link_id",
    "gilded",
]
df = df.select([c for c in KEEP_COLS if c in df.columns])

# Filter to chosen subreddits
df_filtered = df.filter(col("subreddit").isin(SUBREDDITS))

count = df_filtered.count()
print(f"Filtered row count ({', '.join(SUBREDDITS)}): {count:,}")

# Repartition to a reasonable number of parquet files for the midterm
df_filtered = df_filtered.repartition(8)

print(f"Writing to {DEST_PATH} ...")
df_filtered.write.mode("overwrite").parquet(DEST_PATH)

print("Done. Verify:")
print(
    f"  aws s3 ls s3://dats6450-midterm-s2026/reddit/comments/ --recursive --human-readable | tail -5"
)

# Quick sanity check
verify = spark.read.parquet(DEST_PATH)
print(f"Verification read count: {verify.count():,}")
verify.groupBy("subreddit").count().orderBy("count", ascending=False).show()

spark.stop()
