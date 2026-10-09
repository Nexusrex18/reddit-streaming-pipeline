"""Scheduler service for automated hourly batch sentiment processing (Project 14).

Periodically runs the PySpark batch sentiment job to analyze raw comments
stored in HDFS and update the subreddit_sentiment_hourly PostgreSQL table.
"""
import os
import subprocess
import time


INTERVAL = int(os.environ.get("BATCH_INTERVAL_SECONDS", "60"))


def run_batch_job():
    print("[scheduler] Starting scheduled PySpark batch sentiment run...", flush=True)
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "local[2]",
        "--driver-memory", "1g",
        "/app/batch_sentiment.py"
    ]
    try:
        res = subprocess.run(cmd, check=True)
        print(f"[scheduler] Batch job completed with exit code {res.returncode}", flush=True)
    except subprocess.CalledProcessError as e:
        print(f"[scheduler] Batch job failed with exit code {e.returncode}", flush=True)
    except Exception as e:
        print(f"[scheduler] Unexpected error running batch job: {e}", flush=True)


def main():
    print(f"[scheduler] Scheduler started. Check interval: {INTERVAL} seconds", flush=True)
    # Initial small wait to allow initial streaming micro-batches to land in HDFS
    time.sleep(15)

    while True:
        run_batch_job()
        print(f"[scheduler] Next run in {INTERVAL} seconds...", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
