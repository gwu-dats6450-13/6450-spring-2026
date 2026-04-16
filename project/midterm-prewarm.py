"""
DATS 6450 — Applied Big Data Analytics, Spring 2026
Midterm 2 — INSTRUCTOR: Day-of cluster pre-warm script

Run this ~30 minutes before class to:
    1. Verify the Spark cluster is healthy (worker count, parallelism)
    2. Read the midterm dataset to warm block caches
    3. Download and cache the Spark NLP `analyze_sentiment` pretrained
       pipeline on all worker nodes so students don't wait for downloads

Usage (from your EC2 dev machine):
    uv run python project/midterm-prewarm.py spark://<MASTER_PRIVATE_IP>:7077

Expected output: "Pre-warm COMPLETE. Cluster is ready for the midterm."
"""

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

MASTER_URL = sys.argv[1] if len(sys.argv) > 1 else "local[*]"
DATA_PATH  = "s3a://dats6450-midterm-s2026/reddit/comments/"

print(f"Connecting to {MASTER_URL} ...")
t0 = time.time()

spark = (
    SparkSession.builder
    .master(MASTER_URL)
    .appName("DATS6450-Midterm2-PreWarm")
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

# --- 1. Cluster health check -------------------------------------------------
parallelism = spark.sparkContext.defaultParallelism
print(f"\n[1] Cluster parallelism: {parallelism}")
if parallelism < 6:
    print(f"    WARNING: expected >= 6 (3 workers x 2 cores). Got {parallelism}.")
    print(f"    Check Spark UI at http://<MASTER_PUBLIC_IP>:8080")
else:
    print(f"    OK")

# --- 2. Dataset warm-up -------------------------------------------------------
print(f"\n[2] Reading dataset from {DATA_PATH} ...")
df = spark.read.parquet(DATA_PATH)
count = df.count()
print(f"    Row count: {count:,}")
print("    Subreddit breakdown:")
df.groupBy("subreddit").count().orderBy("count", ascending=False).show()
df.cache()
df.count()  # materialise cache
print("    Dataset cached in memory.")

# --- 3. Spark NLP model warm-up -----------------------------------------------
print("\n[3] Downloading and caching Spark NLP analyze_sentiment pipeline ...")
print("    (This may take a few minutes on first run.)")

try:
    from sparknlp.pretrained import PretrainedPipeline
    pipeline = PretrainedPipeline("analyze_sentiment", lang="en")
    # Run a tiny inference to force model distribution to workers
    test_result = pipeline.annotate("This is a great test.")
    print(f"    Test inference result: {test_result.get('sentiment', ['?'])}")
    print("    Spark NLP analyze_sentiment pipeline cached OK.")
except ImportError:
    print("    WARNING: sparknlp not importable from this env.")
    print("    Ensure spark-nlp is installed in the cluster venv:")
    print("      uv add spark-nlp  (then restart cluster)")
except Exception as e:
    print(f"    WARNING: NLP warm-up failed: {e}")
    print("    Students will experience a download delay on Task 3.")

# --- Summary ------------------------------------------------------------------
elapsed = time.time() - t0
print(f"\n{'='*60}")
print(f"Pre-warm COMPLETE in {elapsed:.0f}s.")
print(f"Cluster parallelism:      {parallelism}")
print(f"Dataset row count:        {count:,}")
print(f"Spark UI:                 http://<MASTER_PUBLIC_IP>:8080")
print(f"{'='*60}")
print("Cluster is ready for the midterm.")

spark.stop()
