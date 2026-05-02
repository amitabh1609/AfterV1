# GitHub Archive Lakehouse

A local data lakehouse that ingests a full day of GitHub Archive events (3.8 M rows) through a **Bronze → Silver → Gold** medallion pipeline built on Apache Iceberg, MinIO, and DuckDB — with a live Streamlit dashboard and Iceberg time-travel queries.

Built as a portfolio project to demonstrate production-grade data engineering patterns without cloud costs.

---

## Architecture

```
GitHub Archive (gharchive.org)
        │  24 hourly .json.gz files
        ▼
┌───────────────────┐
│     BRONZE        │  Raw events, schema-enforced, append-only
│  bronze.github_   │  PyArrow → PyIceberg → MinIO (S3-compatible)
│  events           │
└────────┬──────────┘
         │  per-file transform
         ▼
┌───────────────────┐   ┌───────────────────┐
│     SILVER        │   │    QUARANTINE      │
│  silver.github_   │   │  silver.quarantine │
│  events           │   │  (bad records +    │
│  (typed, deduped, │   │   error reason)    │
│   partitioned by  │   └───────────────────┘
│   event_date)     │
└────────┬──────────┘
         │  DuckDB aggregation
         ▼
┌───────────────────────────────────┐
│              GOLD                 │
│  gold.repo_daily_activity         │
│  gold.actor_daily_activity        │
│  (partitioned by event_date)      │
└───────────────────────────────────┘
         │
         ▼
┌───────────────────┐
│  Streamlit        │
│  Dashboard        │  Pipeline status · Analytics · Table explorer
└───────────────────┘
```

**Key design choices:**
- **Manifest-driven pipeline** — JSON file tracks which hourly files are done; safe to kill and resume at any point
- **Atomic downloads** — writes to `.gz.tmp`, renames on completion; never parses a partial file
- **Quarantine routing** — events missing `id`/`type` or with malformed `payload` land in `silver.quarantine` with an error reason rather than failing the whole file
- **Gold always recomputed** — `COUNT DISTINCT` aggregates cannot be safely appended; Gold is drop + recreate from all Silver on each run
- **Iceberg time travel** — every hourly append creates a new snapshot; query any past state with `scan(snapshot_id=X)`

---

## Stack

| Layer | Technology |
|---|---|
| Table format | Apache Iceberg (PyIceberg 0.9) |
| Object store | MinIO (local S3-compatible, Docker) |
| Query engine | DuckDB |
| Columnar I/O | PyArrow |
| DataFrame | Pandas |
| Dashboard | Streamlit + Plotly |
| Language | Python 3.11 |

---

## Project Structure

```
ingestion/
  bronze_ingest.py        # single-file Bronze demo
  silver_transform.py     # single-file Silver demo
  gold_aggregate.py       # Gold aggregation (DuckDB)
  ingest_incremental.py   # full 24-hour pipeline (resumable, quarantine)
  time_travel.py          # Iceberg time-travel query demo
  explore_duckdb.py       # EDA notebook-style exploration
  download_sample.py      # download one sample file

dashboard/
  app.py                  # Streamlit dashboard (4 pages)

data/
  raw/                    # downloaded .json.gz files (gitignored)
  processed/
    manifest.json         # pipeline state (which hours are done)
    iceberg_catalog.db    # SQLite Iceberg catalog

requirements.txt
.env                      # MinIO connection config (gitignored)
```

---

## Setup

### 1. Prerequisites

- Python 3.11
- Docker (for MinIO)

### 2. Clone and install

```bash
git clone https://github.com/amitabh1609/AfterV1.git
cd AfterV1
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 3. Start MinIO

```bash
docker run -d \
  --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minio \
  -e MINIO_ROOT_PASSWORD=minio123 \
  -v "$(pwd)/minio_data:/data" \
  quay.io/minio/minio server /data --console-address ":9001"
```

Create the bucket at [http://localhost:9001](http://localhost:9001) (login: `minio` / `minio123`) → **Create Bucket** → name it `lakehouse`.

### 4. Configure environment

```bash
cat > .env <<EOF
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minio
S3_SECRET_KEY=minio123
S3_BUCKET=lakehouse
EOF
```

---

## Running the Pipeline

### Full incremental run (all 24 hours)

```bash
.venv/bin/python ingestion/ingest_incremental.py
```

Downloads all 24 hourly files for `2024-01-01` from [gharchive.org](https://gharchive.org), ingests each through Bronze → Silver (with quarantine), then rebuilds Gold. Safe to interrupt and resume — already-processed hours are skipped.

```bash
# Start fresh (drop all tables, clear manifest)
.venv/bin/python ingestion/ingest_incremental.py --fresh
```

### Expected output

```
Processing 2024-01-01-0.json.gz ...
  Bronze: 153,644 rows written
  Silver: 153,619 rows  |  Quarantine: 25 rows  |  Dupes dropped: 0
...
Processing 2024-01-01-23.json.gz ...
  Bronze: 151,193 rows written
  Silver: 151,168 rows  |  Quarantine: 25 rows  |  Dupes dropped: 0

==> Recomputing Gold ...
  gold.repo_daily_activity  : 438,902 repo-day rows
  gold.actor_daily_activity : 951,277 actor-day rows

All 24 files done. Total silver rows: 3,879,837
```

### Run individual layers (demo scripts)

```bash
.venv/bin/python ingestion/bronze_ingest.py     # single-file Bronze
.venv/bin/python ingestion/silver_transform.py  # single-file Silver
.venv/bin/python ingestion/gold_aggregate.py    # Gold aggregation
```

---

## Time Travel

Every Silver append creates an Iceberg snapshot. Query any past state:

```bash
# List all 24 snapshots with row counts
.venv/bin/python ingestion/time_travel.py --list

# Default: compare hour-0 snapshot vs current (full day diff)
.venv/bin/python ingestion/time_travel.py

# Compare a specific snapshot
.venv/bin/python ingestion/time_travel.py --snapshot-id 3028477219900682598

# Compare by timestamp
.venv/bin/python ingestion/time_travel.py --as-of "2024-01-01T13:20:00"
```

Sample output:
```
════════════════════════════════════════════════════════════
  TIME-TRAVEL COMPARISON
════════════════════════════════════════════════════════════
  BEFORE : after hour 0 only (153,619 rows)
  AFTER  : after all 24 hours (3,879,837 rows) (current)

  ROW COUNT DELTA
  Before :      153,619 rows
  After  :    3,879,837 rows
  Added  :    3,726,218 rows  (+2425.6%)
```

---

## Dashboard

```bash
.venv/bin/streamlit run dashboard/app.py --server.port 8601
```

Open [http://localhost:8601](http://localhost:8601).

Pages:
- **Overview** — pipeline health, manifest status, per-hour ingestion summary
- **Analytics** — event type distribution, top repos, top actors (Plotly charts)
- **Table Explorer** — query Bronze / Silver / Gold tables directly
- **Quarantine** — browse bad records with error reasons

---

## Data

Source: [GitHub Archive](https://gharchive.org) — public hourly dumps of all GitHub events.

- Date ingested: `2024-01-01` (24 hourly files)
- Raw events: ~3.9 M
- Silver (clean): **3,879,837 rows**
- Quarantined: ~600 rows (missing id/type or malformed payload)
- Gold repo rows: ~439 K repo-day aggregates
- Gold actor rows: ~951 K actor-day aggregates

---

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Iceberg append-only writes | `ingest_incremental.py` |
| Iceberg time travel (`scan(snapshot_id=X)`) | `time_travel.py` |
| Manifest-based resumable pipeline | `ingest_incremental.py` — `manifest.json` |
| Quarantine / dead-letter routing | `transform_to_silver_and_quarantine()` |
| Atomic file downloads (`.tmp` rename) | `download_file()` |
| DuckDB for in-process aggregation | `gold_aggregate.py` |
| PyArrow schema enforcement | All ingestion scripts |
| Local S3 with MinIO (no AWS needed) | Catalog config, `FsspecFileIO` |

---

*Built by [Amitabh Choudhury](https://github.com/amitabh1609)*
