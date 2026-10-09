# Legacy Kubernetes & Cassandra Pipeline (Archived)

> [!WARNING]
> **ARCHIVED / INACTIVE CODEBASE**: This folder is **NOT part of the active project**. It is preserved purely for historical and educational reference.
> The active, production pipeline for **Project 14 (Reddit Comment Sentiment Scoring)** is located at the root of this repository and managed via `docker-compose.yml`.

---

## 1. Why Does This Folder Exist Here?

This repository was originally bootstrapped from [`nama1arpit/reddit-streaming-pipeline`](https://github.com/nama1arpit/reddit-streaming-pipeline), an open-source demo designed to showcase real-time sentiment streaming with Spark, Cassandra, and Kubernetes.

Rather than deleting that codebase, it has been isolated into `legacy_k8s_pipeline/` so that:
- Historical commit context and architectural references remain intact.
- Original author attributions and implementations are preserved.
- The root project workspace remains clean, organized, and focused strictly on the active project requirements.

---

## 2. Why Was It Replaced?

This legacy implementation was retired in favor of the root-level architecture for three critical reasons:

1. **Reddit API Changes & Paywalls**:
   - The legacy code used `PRAW` to query Reddit's live API in real time. Following Reddit's 2023 API pricing policy updates, live programmatic access requires paid subscriptions and is heavily rate-limited.
   - The active project replaces this with a deterministic historical replayer streaming the Kaggle Reddit Comments dataset.

2. **Project 14 Assignment Requirements**:
   - **Storage**: Project 14 mandates **Hadoop HDFS** as the raw data lake storage engine, whereas this legacy implementation used **Apache Cassandra (NoSQL)**.
   - **Metric & Model**: Project 14 requires a 3-class sentiment classifier producing **% Positive, % Neutral, and % Negative per subreddit** over hourly windows, whereas this legacy implementation computed a raw scalar compound score (`-1.0` to `+1.0`) over 1-minute windows.
   - **Dashboard**: Project 14 uses **Postgres + Grafana** for native SQL-based community manager dashboards instead of an external Cassandra plugin.

3. **Resource & Orchestration Overhead**:
   - The legacy architecture required running Minikube, Terraform providers, and multiple heavy JVM services (Spark + Cassandra + Kafka + Zookeeper), which easily exceeded 10–12 GB RAM and had fragile port-forwarding issues.
   - The new project uses a streamlined **Docker Compose** stack fitting comfortably within 6–7 GB RAM on a single machine.

---

## 3. Overview of Legacy Components Inside This Directory

| Directory / File | Description |
| :--- | :--- |
| `reddit_producer/` | Python script streaming live Reddit comments via `praw` into Kafka. |
| `cassandra/` | CQL setup script and Dockerfile for Cassandra tables (`comments`, `subreddit_sentiment_avg`). |
| `spark/` | PySpark Structured Streaming script (`stream_processor.py`) writing to Cassandra. |
| `grafana/` | Grafana image bundled with the `hadesarchitect-cassandra-datasource` plugin. |
| `terraform-k8s/` | Terraform manifests creating Minikube deployments, services, PVs, and PVCs. |
| `docker_image_publish.sh` | Shell script used to build and push legacy images to Docker Hub. |
| `README_ORIGINAL.md` | The original README describing the legacy Kubernetes pipeline. |

---

## 4. Active Project Navigation

To work on or run the active pipeline:
- Central Orchestration: [`../docker-compose.yml`](file:///home/lavi/Code/kaaleju/bda/minip/reddit-streaming-pipeline/docker-compose.yml)
- Active Services: [`../services/`](file:///home/lavi/Code/kaaleju/bda/minip/reddit-streaming-pipeline/services/)
- Documentation & Architecture: [`../docs/plan.md`](file:///home/lavi/Code/kaaleju/bda/minip/reddit-streaming-pipeline/docs/plan.md)
- Root Guide: [`../README.md`](file:///home/lavi/Code/kaaleju/bda/minip/reddit-streaming-pipeline/README.md)
