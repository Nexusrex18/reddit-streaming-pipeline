# Agent Guidelines & Repository Context

This document provides architectural context and operational guidelines for AI coding agents and human developers working in this repository.

---

## 1. Project Overview & Active Goal

* **Project**: **Project 14 — Reddit Comment Sentiment Scoring** (Big Data Analytics)
* **Problem Statement**: Community managers cannot systematically gauge overall sentiment across thousands of daily comments, relying instead on spot-checking a small sample.
* **Objective**: Stream Reddit comments via Kafka, persist them into **HDFS**, run an NLP classification pipeline computing **% Positive, % Neutral, and % Negative per subreddit**, refreshed hourly, and display trends in a **Grafana dashboard** querying Postgres.
* **Tech Stack**: Kafka, Hadoop HDFS, Apache Spark (3.5.1), Python (NLTK VADER / HuggingFace), PostgreSQL, Grafana, Docker Compose.

---

## 2. Directory Structure & Workspace Layout

```
reddit-streaming-pipeline/
├── docker-compose.yml        # Active pipeline orchestrator (DO NOT use K8s/Terraform)
├── .env.example              # Sample environment configuration template
├── .env                      # Active runtime environment variables
├── config/
│   └── hadoop.env            # Single-node HDFS cluster configuration
├── data/                     # Data directory (mounted into services; gitignored)
│   └── sample/               # Sample Reddit datasets (CSV/SQLite)
├── services/                 # All active application containers
│   ├── replayer/             # Dataset stream replayer -> Kafka
│   └── spark/                # Spark streaming sink (HDFS) & batch NLP jobs
├── docs/                     # Planning and architectural documentation
│   └── plan.md               # Detailed implementation plan and phase tracker
├── legacy_k8s_pipeline/      # ARCHIVED: Original reference code (DO NOT MODIFY)
└── README.md                 # Primary user documentation
```

---

## 3. The `legacy_k8s_pipeline/` Archive

### Summary & Purpose
The `legacy_k8s_pipeline/` folder contains the original repository baseline bootstrapped from [`nama1arpit/reddit-streaming-pipeline`](https://github.com/nama1arpit/reddit-streaming-pipeline). It implemented a real-time streaming pipeline using **Minikube/Kubernetes, Terraform, Apache Cassandra, and the live Reddit API (PRAW)**.

### Why It Exists Here
It is preserved as a historical reference to retain commit lineage and acknowledge the original project implementation without discarding prior work.

### Critical Agent Rules for `legacy_k8s_pipeline/`
1. **DO NOT edit or create new code inside `legacy_k8s_pipeline/`** unless the user explicitly asks to modify the legacy project.
2. **DO NOT run Terraform, Minikube, or Cassandra commands** from that directory.
3. All active feature work, bug fixes, and extensions must be targeted at the root `docker-compose.yml` and the `services/` directory.

### Quick Comparison

| Aspect | Legacy Archive (`legacy_k8s_pipeline/`) | Active Project (Root / `services/`) |
| :--- | :--- | :--- |
| **Data Source** | Live Reddit API (`PRAW`) | Kaggle dataset replayer (`replayer.py`) |
| **Storage Sink** | Apache Cassandra (NoSQL) | **Hadoop HDFS** (Parquet data lake) |
| **Sentiment Metric** | Continuous compound float (`-1.0` to `+1.0`) | **% Positive, % Neutral, % Negative** |
| **Serving Database** | Cassandra | **PostgreSQL** (`subreddit_sentiment_hourly`) |
| **Deployment** | Minikube + Terraform | **Docker Compose** |

---

## 4. Key Developer Constraints
- **Resource Budget**: The entire stack must run inside Docker Compose on a single 16 GB RAM developer machine (~5–7 GB total memory footprint).
- **Data Partitioning**: Parquet data written to HDFS must be partitioned by the comment's event timestamp (`year=YYYY/month=MM/day=DD/hour=HH/`), NOT by ingestion/arrival time.
- **Idempotency**: All writes into Postgres must use `ON CONFLICT (...) DO UPDATE` upserts so batch reprocessing is safe to retry.
