# Failure Scenario 2: Duplicate Events Within a File

## Problem

GitHub Archive hourly files occasionally contain the same event ID more than once in the same file. This happens when GH Archive's collection job retries a failed API call and captures the same event twice before the hour boundary closes. For a pipeline tracking contributor activity counts, a duplicate PushEvent means one commit gets counted twice — inflating push counts and unique contributor estimates in Gold without any visible error.

## How It Was Detected

During Silver transform development, adding `drop_duplicates(subset="id", keep="first")` to the cleaning step and logging `before - after` as `dupe_count` showed small but non-zero counts on several hourly files (typically 1–5 dupes per file). The per-file summary log printed this explicitly:

```
Processed: 153,619  Quarantined: 0  Dupes dropped: 25  ✓
```

Without the explicit dedup step and the counter, these would have silently inflated every downstream metric.

## How the Pipeline Reacted

Deduplication runs in-memory on the clean DataFrame before it is written to Silver, so no duplicate ever lands in Iceberg:

```python
before = len(clean_df)
clean_df = clean_df.drop_duplicates(subset="id", keep="first")
dupe_count = before - len(clean_df)
```

The `dupe_count` is returned to the caller and written to the manifest alongside `bronze_rows` and `silver_rows`, giving a permanent audit trail of exactly how many duplicates were dropped per file.

Cross-file duplicates are not a risk for this dataset: GH Archive guarantees each event ID appears in exactly one hourly file, so per-file deduplication is sufficient. This assumption is documented in the code.

## Recovery Steps

Duplicates are handled automatically on every run. To verify the current state:

```bash
# Compare bronze_rows vs silver_rows in manifest to find files with dupes
python3 -c "
import json
m = json.load(open('data/processed/manifest.json'))
dupes = {k: v['bronze_rows'] - v['silver_rows']
         for k,v in m.items()
         if v.get('bronze_rows',0) != v.get('silver_rows',0)}
print('Files with duplicates dropped:')
for f, d in sorted(dupes.items()):
    print(f'  {f}: {d} dupes')
print(f'Total dupes: {sum(dupes.values())}')
"
```

If a Soda check catches duplicates in Silver (which should never happen given the above), the recovery is to drop and reprocess:

```bash
.venv/bin/python ingestion/ingest_incremental.py --fresh
```

## What I Learned

Deduplication logic needs to be tested with a real sample of the input data, not assumed away. The correct place to deduplicate is as early as possible in the pipeline — in this case, before writing to Silver — not as a remediation step after the fact. Logging the dupe count as a first-class metric (not just a debug print) turns a silent correctness issue into an observable operational metric.
