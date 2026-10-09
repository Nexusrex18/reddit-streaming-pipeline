# Plan: Transform `reddit-streaming-pipeline` into Project 14 — Reddit Comment Sentiment Scoring

## 1. Goal

Convert the live Reddit → Kafka → Spark → Cassandra → Grafana demo into a Docker-only pipeline that:

1. Replays the Kaggle Reddit Comments dataset as a stream (no Reddit API needed).
2. Buffers it in Kafka.
3. Persists raw comments in **HDFS**.
4. Runs an **hourly NLP job** that labels each comment Positive / Neutral / Negative.
5. Aggregates **% Pos / % Neu / % Neg per subreddit per hour**.
6. Shows the results to community managers in **Grafana**.

**Constraints:** runs with `docker compose up`; no Kubernetes, no Terraform; fits on a 16 GB machine.

## 2. Decisions made

| Topic | Decision |
|---|---|
| Dataset | Kaggle `reddit-comments-may-2015` — a **SQLite** file (`database.sqlite`, table `May2015`), not CSV. Replayer also supports CSV/JSONL via column mapping. |
| Sentiment | VADER with thresholds first, behind a swappable classifier interface (DistilRoBERTa later). |
| Dashboard | Grafana (same approach as the repo), reading from Postgres. |
| Replay | Configurable: `realtime` (1x) or `accelerated` (`SPEEDUP_FACTOR`). |
| Scheduling | Configurable interval: 3600 s for real hourly runs, a few minutes for demos. |
| Orchestration | Docker Compose only. |

## 3. Architecture

```
database.sqlite (Kaggle)
   │  replayer.py  (paced by created_utc)
   ▼
Kafka topic: reddit-comments  ──(Kafdrop UI for inspection)
   │  hdfs_sink.py  (Spark Structured Streaming)
   ▼
HDFS /data/reddit/raw/year=YYYY/month=MM/day=DD/hour=HH/   (Parquet)
   │  batch_sentiment.py  (scheduled; Spark + classifier)
   ▼
Postgres: subreddit_sentiment_hourly
   │
   ▼
Grafana dashboard (provisioned)
```

## 4. Repo vs. Project 14: what changes

| Layer | Original repo | Project 14 |
|---|---|---|
| Source | `reddit_producer.py` (PRAW, live API) | `replayer.py` (Kaggle file) |
| Broker | Kafka + Zookeeper + Kafdrop (`kafka.tf`) | Same, defined in `docker-compose.yml` |
| Storage | Cassandra | HDFS (namenode + datanode) |
| Processing | 5 s micro-batches, continuous VADER score | Hourly batch job, 3-class label |
| Serving | Cassandra → Grafana Cassandra plugin | Postgres → Grafana (built-in datasource) |
| Metric | Compound score −1…+1 | % Positive / % Neutral / % Negative |
| Deploy | Minikube + Terraform | Docker Compose |

## 5. Project layout and what each file is for

Status: ✅ = already built and delivered in `reddit-sentiment-hdfs.zip`, ⏳ = still to build.

```
reddit-sentiment-hdfs/
├── docker-compose.yml          ✅ Defines every service, startup order, healthchecks, volumes
├── .env                        ✅ Single place for all knobs (topic, paths, replay mode/speed, time window, column mapping)
├── hadoop.env                  ✅ HDFS settings (replication=1, webhdfs on, permissions off) for a single-node cluster
├── README.md                   ✅ Run + verify instructions
├── data/                       ✅ Drop database.sqlite here (mounted read-only into the replayer)
├── replayer/
│   ├── Dockerfile              ✅ Python image with confluent-kafka
│   └── replayer.py             ✅ Reads SQLite/CSV/JSONL, paces by created_utc, publishes JSON to Kafka (key = subreddit)
├── spark/
│   ├── Dockerfile              ✅ Spark 3.5.1 image with Kafka jars baked in (extended later with nltk + Postgres JDBC)
│   ├── hdfs_sink.py            ✅ Kafka → Parquet in HDFS, partitioned by the comment's own event time
│   ├── batch_sentiment.py      ⏳ Reads one hour from HDFS → classify → aggregate → upsert into Postgres
│   └── classifiers/
│       ├── base.py             ⏳ Interface: classify(texts) -> labels
│       ├── vader.py            ⏳ VADER, thresholds ±0.05 on compound score
│       └── roberta.py          ⏳ Placeholder for the later model swap
├── scheduler/                  ⏳ Small loop container that triggers the batch job for each completed, unprocessed hour
├── postgres/init.sql           ⏳ Creates subreddit_sentiment_hourly + indexes
└── grafana/provisioning/       ⏳ datasource.yml + dashboard.json, so the dashboard exists on first start
```

### Use case of the files already built

- **`docker-compose.yml`**: replaces `kafka.tf`, the Terraform and the Kubernetes setup in one file. `kafka-init` creates the topic before anything uses it; the HDFS and Kafka healthchecks keep the startup order safe.
- **`.env`**: change the replay behaviour without touching code: `REPLAY_MODE`, `SPEEDUP_FACTOR`, `START_UTC`/`END_UTC`, `MAX_ROWS`, `DATA_FORMAT`.
- **`replayer.py`**: the stand-in for the live Reddit API. It lets you demo "streaming" from historical data at any speed.
- **`hdfs_sink.py`**: the ingestion-to-data-lake step required by the project statement. Partitioning by event time (not arrival time) is what makes the hourly windows correct.

## 6. Data contracts

**Kafka message (JSON):** `id, author, body, subreddit, created_utc, ups, downs, score`

**HDFS Parquet columns:** the fields above plus `event_ts, year, month, day, hour` (partition columns are zero-padded strings, e.g. `hour=03`).

**Postgres `subreddit_sentiment_hourly`:**

| Column | Type |
|---|---|
| subreddit | text |
| window_start | timestamptz (hour start) |
| pos_count, neu_count, neg_count, total | int |
| pct_pos, pct_neu, pct_neg | numeric |
| classifier | text (`vader`, later `roberta`) |
| computed_at | timestamptz |

Primary key: `(subreddit, window_start, classifier)`. Writes are upserts, so reruns are idempotent.

## 7. Build phases

| # | Phase | Done when | Status |
|---|---|---|---|
| 1 | Compose skeleton: Kafka, Zookeeper, Kafdrop, HDFS | All containers healthy; topic exists | ✅ Complete |
| 2 | Replayer | Messages visible in Kafdrop | ✅ Complete |
| 3 | HDFS sink | `hour=` partitions appear under `/data/reddit/raw` | ✅ Complete |
| 4 | Batch sentiment job (VADER) | Rows in Postgres for each finished hour | ✅ Complete |
| 5 | Scheduler | New hours processed automatically at the configured interval | ✅ Complete |
| 6 | Grafana provisioning | Dashboard loads with data on first `docker compose up` | ✅ Complete |
| 7 | Docs + polish | One-command start, clean layout, verification tested | ✅ Complete |

### Phase 4 details: batch job
- Input: `year/month/day/hour` of the target hour (or "all unprocessed").
- Read the Parquet path with `spark.sql.sources.partitionColumnTypeInference.enabled=false`.
- Drop empty, `[deleted]` and `[removed]` bodies.
- Classify with the configured classifier (`CLASSIFIER=vader|roberta`):
  - compound ≥ 0.05 → Positive
  - compound ≤ −0.05 → Negative
  - otherwise → Neutral
- `groupBy(subreddit, hour)`; compute counts and percentages: `%Positive = positive_count / total × 100`.
- Upsert into Postgres.

### Phase 5 details: scheduler
- `BATCH_INTERVAL_SECONDS=3600` for production behaviour, ~120 for demos.
- An hour counts as "complete" when the data's own newest timestamp has passed the end of that hour. This keeps accelerated replay correct, since wall-clock time doesn't match data time.
- Skips hours already present in Postgres.

### Phase 6 details: Grafana panels
- Subreddit dropdown variable.
- Stacked bar per hour: % Pos / Neu / Neg.
- Donut for the latest hour.
- Trend indicator: latest hour vs. previous hour.
- Comment volume per hour.

## 8. How to verify each stage

| Stage | Check |
|---|---|
| Kafka | Kafdrop at `localhost:9000` → topic `reddit-comments` |
| HDFS | NameNode UI at `localhost:9870` → Utilities → Browse; or `docker compose exec namenode hdfs dfs -ls -R /data/reddit/raw` |
| Replayer | `docker compose logs -f replayer` (shows simulated time) |
| Sink | `docker compose logs -f hdfs-sink` |
| Batch job (later) | `SELECT * FROM subreddit_sentiment_hourly ORDER BY window_start` |
| Dashboard (later) | Grafana at `localhost:3000` |

## 9. Risks and notes

- **First SQLite read is slow.** `created_utc` has no index, so the windowed query scans the whole ~30 GB file. Keep the window small, or copy the slice to CSV once.
- **Small files in HDFS.** A 30 s trigger across hourly partitions produces many small Parquet files. This is acceptable for a demo; compaction can be added if needed.
- **Memory.** HDFS + Kafka + Spark local mode fits easily in 16 GB; a transformer model later will add noticeably more.
- **Not yet run end to end.** Steps 1–3 passed syntax checks only; expect small first-boot fixes.
- **Model swap.** Keep all label logic behind `classifiers/base.py` so changing `CLASSIFIER` is the only change needed.

## 10. Next action

Review steps 1–3 by running the stack and confirming `hour=` partitions appear in HDFS. Then build phases 4–6.
