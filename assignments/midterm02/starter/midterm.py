"""
DATS 6450 — Applied Big Data Analytics, Spring 2026
Midterm 2: Apache Spark on EC2

Usage:
    uv run python midterm.py spark://<MASTER_PRIVATE_IP>:7077 | tee output.log

Rules:
    - No AI assistants
    - You may reference course slides and your own lab notebooks
    - Work must be your own

Replace every  # TODO  block with your code.
Do NOT remove any print() calls — they are used for grading.
"""

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, from_unixtime
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import pandas_udf

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer,
    StopWordsRemover,
    HashingTF,
    IDF,
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import pandas as pd

# ---------------------------------------------------------------------------
# Spark session — pre-configured for S3A access; do not modify
# ---------------------------------------------------------------------------
MASTER_URL = sys.argv[1] if len(sys.argv) > 1 else "local[*]"

spark = (
    SparkSession.builder
    .master(MASTER_URL)
    .appName("DATS6450-Midterm2")
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

# ---------------------------------------------------------------------------
# Dataset path — same for every student; do not change
# ---------------------------------------------------------------------------
DATA_PATH = "s3a://dats6450-midterm-s2026/reddit/comments/"


def banner(task_num: str, title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  TASK {task_num}: {title}")
    print(f"{'='*60}")


# ===========================================================================
# TASK 1 — PySpark DataFrames and SparkSQL  (25 points)
# ===========================================================================
banner("1", "PySpark DataFrames and SparkSQL")
t1_start = time.time()

# (a) Load the parquet dataset. Print schema and row count.
# TODO: Load from DATA_PATH into a DataFrame called `df`

# TODO: Print the schema

# TODO: Print the row count with a label, e.g.:
#   print(f"Row count: {???}")


# (b) DataFrame API — top 10 subreddits by comment volume
# TODO: Use groupBy + count + orderBy to compute top 10 subreddits.
#   Assign to `top_subreddits` and call .show()


# (c) SparkSQL — top 10 authors by total score
# TODO: Register df as a temp view called "comments"
# TODO: Write a spark.sql(...) query that returns (author, total_score)
#   sorted by total_score descending, limit 10.
#   Assign to `top_authors` and call .show()


# (d) Per-day comment volume for August 2023
# TODO: Add a column `date` = to_date(from_unixtime(col("created_utc")))
# TODO: Filter to 2023-08-01 through 2023-08-31 (inclusive)
# TODO: groupBy("date").count(), sort by date ascending
# TODO: Assign to `daily_counts` and call .show(31)

print(f"\nTask 1 wall-clock: {time.time() - t1_start:.1f}s")


# ===========================================================================
# TASK 2 — Pandas UDF  (15 points)
# ===========================================================================
banner("2", "Pandas UDF — word count")
t2_start = time.time()

# (a) Define a scalar Pandas UDF named `word_count_udf`.
#     Input:  pd.Series of strings (the `body` column)
#     Output: pd.Series of int (number of whitespace-split tokens)
#     Use the @pandas_udf("int") decorator.
# TODO


# (b) Apply `word_count_udf` to create a new column `word_count` on `df`.
# TODO: df = df.withColumn("word_count", word_count_udf(col("body")))


# (c) Compute and print mean, median, max word count.
#     For median, use F.percentile_approx("word_count", 0.5)
# TODO: Use df.agg(...) and print the result with .show()

print(f"\nTask 2 wall-clock: {time.time() - t2_start:.1f}s")


# ===========================================================================
# TASK 3 — Spark NLP Sentiment Analysis  (25 points)
# ===========================================================================
banner("3", "Spark NLP — sentiment analysis with LightPipeline")
t3_start = time.time()

# (a) Sample ~2,000 rows from df.
#     Hint: estimate a fraction, then call .limit(2000) to be exact.
# TODO: sample_df = df.sample(False, <fraction>, seed=42).limit(2000)

# TODO: Convert to Pandas: sample_pd = sample_df.select("subreddit", "body").toPandas()

# (b) Load the pretrained sentiment pipeline and run LightPipeline inference.
# TODO: from sparknlp.pretrained import PretrainedPipeline
# TODO: pipeline = PretrainedPipeline("analyze_sentiment", lang="en")
# TODO: results = pipeline.annotate(sample_pd["body"].fillna("").tolist())
#   Each element of `results` is a dict; extract results[i]["sentiment"][0]
#   to get "positive" or "negative"

# (c) Build a new Spark DataFrame with columns (subreddit, sentiment_label).
# TODO: sentiment_labels = [r["sentiment"][0] for r in results]
# TODO: sample_pd["sentiment_label"] = sentiment_labels
# TODO: sentiment_df = spark.createDataFrame(
#           sample_pd[["subreddit", "sentiment_label"]])

# (d) Compute % positive per subreddit.
# TODO: grouped = sentiment_df.groupBy("subreddit").agg(
#           (F.sum((col("sentiment_label") == "positive").cast("int"))
#            / F.count("*") * 100).alias("pct_positive")
#       ).orderBy("pct_positive", ascending=False)
# TODO: grouped.show()

print(f"\nTask 3 wall-clock: {time.time() - t3_start:.1f}s")


# ===========================================================================
# TASK 4 — Spark MLlib Classification Pipeline  (35 points)
# ===========================================================================
banner("4", "Spark MLlib — binary classification pipeline")
t4_start = time.time()

# (a) Prepare data: filter nulls/empty bodies, add binary label column.
# TODO: ml_df = df.filter(
#           col("body").isNotNull() & (F.length(col("body")) > 0)
#       )
# TODO: ml_df = ml_df.withColumn("label", (col("score") >= 5).cast("int"))

# (b) Build a Pipeline with these stages:
#       1. Tokenizer(inputCol="body", outputCol="tokens")
#       2. StopWordsRemover(inputCol="tokens", outputCol="clean_tokens")
#       3. HashingTF(inputCol="clean_tokens", outputCol="tf", numFeatures=16384)
#       4. IDF(inputCol="tf", outputCol="features")
#       5. LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
# TODO: tokenizer  = ...
# TODO: remover    = ...
# TODO: hashing_tf = ...
# TODO: idf        = ...
# TODO: lr         = ...
# TODO: pipeline   = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, lr])

# (c) Train / test split and fit.
# TODO: train, test = ml_df.randomSplit([0.8, 0.2], seed=42)
# TODO: model = pipeline.fit(train)
# TODO: predictions = model.transform(test)

# (d) Evaluate with BinaryClassificationEvaluator (areaUnderROC).
# TODO: evaluator = BinaryClassificationEvaluator(
#           metricName="areaUnderROC", labelCol="label"
#       )
# TODO: auc = evaluator.evaluate(predictions)
# TODO: print(f"Test AUC: {auc:.4f}")

print(f"\nTask 4 wall-clock: {time.time() - t4_start:.1f}s")


# ---------------------------------------------------------------------------
spark.stop()
print("\nDone. Commit midterm.py, RESPONSES.md, and output.log.")
