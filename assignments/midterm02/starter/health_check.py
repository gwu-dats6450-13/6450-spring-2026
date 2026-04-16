"""
DATS 6450 Midterm 2 — Cluster Health Check

Run this BEFORE starting midterm.py to confirm your Spark cluster
and S3 access are working correctly.

Usage:
    uv run python health_check.py spark://<MASTER_PRIVATE_IP>:7077
"""

import sys
import time

from pyspark.sql import SparkSession

MASTER_URL = sys.argv[1] if len(sys.argv) > 1 else "local[*]"
DATA_PATH  = "s3a://dats6450-midterm-s2026/reddit/comments/"

print(f"Connecting to {MASTER_URL} ...")
t0 = time.time()

spark = (
    SparkSession.builder
    .master(MASTER_URL)
    .appName("DATS6450-Midterm2-HealthCheck")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Worker count
parallelism = spark.sparkContext.defaultParallelism
print(f"[1] defaultParallelism = {parallelism}")
assert parallelism >= 6, (
    f"Expected >= 6 (3 workers x 2 cores each), got {parallelism}. "
    "Is your cluster fully started? Check http://<MASTER_PUBLIC_IP>:8080"
)
print("    OK — cluster has enough parallelism")

# 2. S3 read check
print(f"[2] Reading 10 rows from {DATA_PATH} ...")
df = spark.read.parquet(DATA_PATH)
sample = df.limit(10).collect()
assert len(sample) == 10, "Could not read 10 rows from the dataset"
print(f"    OK — read {len(sample)} rows")
print(f"    Schema: {[f.name for f in df.schema.fields]}")

# 3. Wall-clock
elapsed = time.time() - t0
print(f"\n[3] Total elapsed: {elapsed:.1f}s")
assert elapsed < 90, (
    f"Health check took {elapsed:.0f}s — cluster may be overloaded. "
    "Raise your hand."
)
print("    OK — within time budget")

spark.stop()
print("\nHealth check PASSED. You are good to start midterm.py.")
