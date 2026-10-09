"""Hourly Batch Sentiment Processing Job (Project 14).

Reads Parquet data from HDFS (/data/reddit/raw/year=.../month=.../day=.../hour=...),
applies the NLP sentiment classification model (VADER / RoBERTa),
computes % Positive, % Neutral, % Negative per subreddit per hour,
and idempotently upserts the results into PostgreSQL.
"""
import argparse
import os
import sys
import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Import classifier interface
from classifiers.vader import VaderClassifier
from classifiers.roberta import RobertaClassifier


def get_classifier(name: str):
    name = (name or "vader").lower()
    if name == "vader":
        return VaderClassifier()
    elif name == "roberta":
        return RobertaClassifier()
    else:
        raise ValueError(f"Unknown classifier: {name}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run Reddit batch sentiment analysis.")
    parser.add_argument("--hdfs-url", default=os.environ.get("HDFS_URL", "hdfs://namenode:9000"))
    parser.add_argument("--raw-path", default=os.environ.get("HDFS_RAW_PATH", "/data/reddit/raw"))
    parser.add_argument("--year", default=None, help="Specific year to process (e.g. 2015)")
    parser.add_argument("--month", default=None, help="Specific month to process (e.g. 05)")
    parser.add_argument("--day", default=None, help="Specific day to process (e.g. 01)")
    parser.add_argument("--hour", default=None, help="Specific hour to process (e.g. 00)")
    parser.add_argument("--classifier", default=os.environ.get("CLASSIFIER", "vader"))
    parser.add_argument("--pg-host", default=os.environ.get("POSTGRES_HOST", "postgres"))
    parser.add_argument("--pg-port", type=int, default=int(os.environ.get("POSTGRES_PORT", "5432")))
    parser.add_argument("--pg-db", default=os.environ.get("POSTGRES_DB", "reddit_sentiment"))
    parser.add_argument("--pg-user", default=os.environ.get("POSTGRES_USER", "postgres"))
    parser.add_argument("--pg-password", default=os.environ.get("POSTGRES_PASSWORD", "postgres"))
    return parser.parse_args()


def upsert_to_postgres(rows, args):
    """Upserts aggregated rows into PostgreSQL table subreddit_sentiment_hourly."""
    if not rows:
        print("[batch-sentiment] No rows to upsert into PostgreSQL.")
        return

    print(f"[batch-sentiment] Upserting {len(rows)} records into PostgreSQL table subreddit_sentiment_hourly...")
    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password
    )
    cursor = conn.cursor()

    query = """
    INSERT INTO subreddit_sentiment_hourly (
        subreddit, window_start, pos_count, neu_count, neg_count, total,
        pct_pos, pct_neu, pct_neg, classifier, computed_at
    ) VALUES %s
    ON CONFLICT (subreddit, window_start, classifier)
    DO UPDATE SET
        pos_count = EXCLUDED.pos_count,
        neu_count = EXCLUDED.neu_count,
        neg_count = EXCLUDED.neg_count,
        total = EXCLUDED.total,
        pct_pos = EXCLUDED.pct_pos,
        pct_neu = EXCLUDED.pct_neu,
        pct_neg = EXCLUDED.pct_neg,
        computed_at = NOW();
    """

    data = [
        (
            r["subreddit"],
            r["window_start"],
            r["pos_count"],
            r["neu_count"],
            r["neg_count"],
            r["total"],
            float(r["pct_pos"]),
            float(r["pct_neu"]),
            float(r["pct_neg"]),
            args.classifier,
            r["computed_at"]
        )
        for r in rows
    ]

    execute_values(cursor, query, data)
    conn.commit()
    cursor.close()
    conn.close()
    print("[batch-sentiment] PostgreSQL upsert completed successfully.")


def main():
    args = parse_args()
    print(f"[batch-sentiment] Starting batch run using classifier='{args.classifier}'")

    classifier = get_classifier(args.classifier)

    # Initialize Spark Session
    spark = (
        SparkSession.builder
        .appName("reddit-batch-sentiment")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Target path
    input_path = f"{args.hdfs_url.rstrip('/')}/{args.raw_path.lstrip('/')}"
    print(f"[batch-sentiment] Reading Parquet data from: {input_path}")

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"[batch-sentiment] Error reading Parquet from HDFS: {e}")
        sys.exit(1)

    # Apply partition filters if specified
    if args.year:
        df = df.filter(F.col("year") == str(args.year).zfill(4))
    if args.month:
        df = df.filter(F.col("month") == str(args.month).zfill(2))
    if args.day:
        df = df.filter(F.col("day") == str(args.day).zfill(2))
    if args.hour:
        df = df.filter(F.col("hour") == str(args.hour).zfill(2))

    # Clean data: drop nulls, [deleted], [removed]
    cleaned = df.filter(
        F.col("body").isNotNull() &
        ~F.trim(F.col("body")).isin("", "[deleted]", "[removed]") &
        F.col("subreddit").isNotNull()
    )

    total_comments = cleaned.count()
    print(f"[batch-sentiment] Processing {total_comments:,} valid comments...")
    if total_comments == 0:
        print("[batch-sentiment] No comments found matching criteria.")
        return

    # Define classification UDF
    classify_udf = F.udf(lambda text: classifier.classify(text), StringType())

    # Add sentiment category column
    classified = cleaned.withColumn("sentiment", classify_udf(F.col("body")))

    # Construct window_start timestamp from partition columns
    classified = classified.withColumn(
        "window_start",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.concat_ws("-", F.col("year"), F.col("month"), F.col("day")),
                F.concat(F.col("hour"), F.lit(":00:00"))
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # Aggregate counts and percentages per subreddit per hour
    aggregated = (
        classified.groupBy("subreddit", "window_start")
        .agg(
            F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("pos_count"),
            F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0)).alias("neu_count"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("neg_count"),
            F.count(F.lit(1)).alias("total"),
        )
        .withColumn("pct_pos", F.round((F.col("pos_count") / F.col("total")) * 100.0, 2))
        .withColumn("pct_neu", F.round((F.col("neu_count") / F.col("total")) * 100.0, 2))
        .withColumn("pct_neg", F.round((F.col("neg_count") / F.col("total")) * 100.0, 2))
        .withColumn("computed_at", F.current_timestamp())
    )

    results = aggregated.collect()
    print(f"[batch-sentiment] Computed metrics for {len(results)} (subreddit, hour) windows.")

    # Show preview in logs
    aggregated.show(10, truncate=False)

    # Upsert results to PostgreSQL
    upsert_to_postgres(results, args)

    print("[batch-sentiment] Batch job finished successfully.")


if __name__ == "__main__":
    main()
