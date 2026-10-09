# Reddit Comment Sentiment Scoring Pipeline (Project 14)

An end-to-end Big Data streaming and batch processing pipeline for community sentiment analysis. The pipeline replays Reddit comments from a Kaggle dataset into Apache Kafka, persists raw data into Hadoop HDFS partitioned by event time, executes an NLP sentiment classification job computing hourly positive/neutral/negative distributions per subreddit, and surfaces metrics on a Grafana dashboard.

---

## 1. Architecture Overview

```
Kaggle Dataset (CSV / SQLite)
        │
        ▼ (paced by created_utc)
services/replayer/replayer.py
        │
        ▼
Apache Kafka (Topic: reddit-comments) ───[ Kafdrop UI (Port 9000) ]
        │
        ▼ (PySpark Structured Streaming)
services/spark/hdfs_sink.py
        │
        ▼
Hadoop HDFS: /data/reddit/raw/year=YYYY/month=MM/day=DD/hour=HH/ (Parquet)
        │
        ▼ (Scheduled PySpark + NLP VADER Classifier)
services/spark/batch_sentiment.py
        │
        ▼
PostgreSQL: subreddit_sentiment_hourly
        │
        ▼
Grafana Dashboard (Port 3000)
```

---

## 2. Directory Layout

```
reddit-streaming-pipeline/
├── docker-compose.yml        # Orchestrates the entire containerized pipeline
├── .env.example              # Configuration template
├── .env                      # Active runtime environment configuration
├── config/
│   └── hadoop.env            # Single-node HDFS configuration
├── data/                     # Data directory (mounted into services; gitignored)
│   └── sample/               # Sample dataset location (Option A)
├── services/                 # Application containers
│   ├── replayer/             # Kaggle dataset replayer (producer)
│   └── spark/                # Spark streaming sink & batch NLP classifier
├── docs/                     # Architecture documentation & plan.md
├── legacy_k8s_pipeline/      # ARCHIVED: Original Minikube/Cassandra baseline
├── AGENTS.md                 # Context & instructions for AI agents
└── README.md                 # This file
```

---

## 3. Quick Start

### Prerequisites
- Docker Engine 24+ and Docker Compose v2+
- 16 GB RAM recommended (~5–7 GB allocated to Docker)

### 1. Configure Environment
```bash
cp .env.example .env
```

### 2. Prepare Dataset (Option A: Sample)
Place your dataset file in `data/` and verify the path in `.env`:
```properties
DATA_FORMAT=csv
DATA_PATH=/data/reddit_sample.csv
REPLAY_MODE=accelerated
SPEEDUP_FACTOR=120
```

### 3. Start the Pipeline
```bash
docker compose up -d --build
```

---

## 4. Service Verification & Endpoints

| Service | Endpoint | Purpose |
| :--- | :--- | :--- |
| **Kafdrop UI** | [http://localhost:9000](http://localhost:9000) | Inspect Kafka topics, partitions, and streaming messages |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | Browse HDFS data lake partitions under `/data/reddit/raw` |
| **Grafana UI** | [http://localhost:3000](http://localhost:3000) | Community manager sentiment dashboards |

Check container logs:
```bash
# Follow replayer progress
docker compose logs -f replayer

# Follow HDFS sink progress
docker compose logs -f hdfs-sink

# Verify HDFS partitions
docker compose exec namenode hdfs dfs -ls -R /data/reddit/raw
```

---

## 5. Legacy Codebase Notice

The original prototype of this repository used **Minikube/Kubernetes, Terraform, Apache Cassandra, and the live Reddit API (PRAW)**. That implementation has been archived in [`legacy_k8s_pipeline/`](file:///home/lavi/Code/kaaleju/bda/minip/reddit-streaming-pipeline/legacy_k8s_pipeline/) for reference. See [`legacy_k8s_pipeline/README.md`](file:///home/lavi/Code/kaaleju/bda/minip/reddit-streaming-pipeline/legacy_k8s_pipeline/README.md) for details.
