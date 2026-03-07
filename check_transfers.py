#!/usr/bin/env python3
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://hafsql_public:hafsql_public@hafsql-sql.mahdiyari.info:5432/haf_block_log"
USERNAME = "username"  # enter desired username here (without @)

engine = create_engine(DB_URL, connect_args={"connect_timeout": 30})

with engine.connect() as conn:
    row = conn.execute(text("""
        SELECT
            COALESCE(SUM(CASE WHEN symbol='HIVE' THEN amount ELSE 0 END), 0) AS hive_total,
            COALESCE(SUM(CASE WHEN symbol='HBD'  THEN amount ELSE 0 END), 0) AS hbd_total,
            COUNT(*) AS transfer_count
        FROM hafsql.operation_transfer_table
        WHERE from_account=:u
    """), {"u": USERNAME}).fetchone()
    print(f"@{USERNAME} total transfers sent:")
    print(f"  HIVE: {row[0]:.3f}")
    print(f"  HBD:  {row[1]:.3f}")
    print(f"  Number of transfers: {row[2]}")
    rows = conn.execute(text("""
        SELECT to_account, SUM(amount) AS total, COUNT(*) AS cnt
        FROM hafsql.operation_transfer_table
        WHERE from_account=:u
        GROUP BY to_account
        ORDER BY total DESC
        LIMIT 10
    """), {"u": USERNAME}).fetchall()
    print(f"\nTop 10 recipients:")
    for r in rows:
        print(f"  @{r[0]}: {r[1]:.3f} ({r[2]}x)")