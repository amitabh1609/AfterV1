"""
Load GitHub Archive sample into DuckDB and run exploratory queries.
Usage: .venv/bin/python ingestion/explore_duckdb.py
"""

import duckdb

RAW_FILE = "data/raw/2024-01-01-0.json.gz"
DB_FILE = "data/processed/github.duckdb"

con = duckdb.connect(DB_FILE)

# Read the newline-delimited JSON directly — DuckDB handles .gz natively
print("==> Creating events table from raw JSON...")
con.execute(f"""
    CREATE OR REPLACE TABLE events AS
    SELECT
        id,
        type,
        actor->>'login'        AS actor_login,
        actor->>'id'           AS actor_id,
        repo->>'name'          AS repo_name,
        repo->>'id'            AS repo_id,
        created_at::TIMESTAMP  AS created_at,
        payload
    FROM read_ndjson('{RAW_FILE}',
        columns = {{
            id:         'VARCHAR',
            type:       'VARCHAR',
            actor:      'JSON',
            repo:       'JSON',
            created_at: 'VARCHAR',
            payload:    'JSON',
            public:     'BOOLEAN'
        }}
    )
""")

print("\n==> Total events loaded:")
con.sql("SELECT COUNT(*) AS total FROM events").show()

print("\n==> Event type counts:")
con.sql("""
    SELECT type, COUNT(*) AS cnt
    FROM events
    GROUP BY type
    ORDER BY cnt DESC
""").show()

print("\n==> Top 10 most active repos (by PushEvent):")
con.sql("""
    SELECT repo_name, COUNT(*) AS pushes
    FROM events
    WHERE type = 'PushEvent'
    GROUP BY repo_name
    ORDER BY pushes DESC
    LIMIT 10
""").show()

print("\n==> Top 10 most active contributors (by PushEvent):")
con.sql("""
    SELECT actor_login, COUNT(*) AS pushes
    FROM events
    WHERE type = 'PushEvent'
    GROUP BY actor_login
    ORDER BY pushes DESC
    LIMIT 10
""").show()

print("\n==> Sample: filter all events for a single repo:")
con.sql("""
    SELECT type, actor_login, created_at
    FROM events
    WHERE repo_name = 'torvalds/linux'
    ORDER BY created_at
""").show()

print("\n==> Schema of events table:")
con.sql("DESCRIBE events").show()

con.close()
print(f"\nDuckDB database saved to {DB_FILE}")
