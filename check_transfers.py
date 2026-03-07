#!/usr/bin/env python3
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://hafsql_public:hafsql_public@hafsql-sql.mahdiyari.info:5432/haf_block_log"
USERNAME = "username"  # enter desired username here

engine = create_engine(DB_URL, connect_args={"connect_timeout": 30})

with engine.connect() as conn:
    # Total summary sent
    row_sent = conn.execute(text("""
        SELECT
            COALESCE(SUM(CASE WHEN symbol='HIVE' THEN amount ELSE 0 END), 0) AS hive_total,
            COALESCE(SUM(CASE WHEN symbol='HBD'  THEN amount ELSE 0 END), 0) AS hbd_total,
            COUNT(*) AS transfer_count
        FROM hafsql.operation_transfer_table
        WHERE from_account=:u
    """), {"u": USERNAME}).fetchone()

    # Total summary received
    row_recv = conn.execute(text("""
        SELECT
            COALESCE(SUM(CASE WHEN symbol='HIVE' THEN amount ELSE 0 END), 0) AS hive_total,
            COALESCE(SUM(CASE WHEN symbol='HBD'  THEN amount ELSE 0 END), 0) AS hbd_total,
            COUNT(*) AS transfer_count
        FROM hafsql.operation_transfer_table
        WHERE to_account=:u
    """), {"u": USERNAME}).fetchone()

    print(f"@{USERNAME} total transfers sent:")
    print(f"  HIVE: {row_sent[0]:,.0f}")
    print(f"  HBD:  {row_sent[1]:,.0f}")
    print(f"  Number of transfers: {row_sent[2]}")

    print(f"\n@{USERNAME} total transfers received:")
    print(f"  HIVE: {row_recv[0]:,.0f}")
    print(f"  HBD:  {row_recv[1]:,.0f}")
    print(f"  Number of transfers: {row_recv[2]}")

    # Top 10 recipients by HIVE (sent)
    rows = conn.execute(text("""
        SELECT to_account, SUM(amount) AS total, COUNT(*) AS cnt
        FROM hafsql.operation_transfer_table
        WHERE from_account=:u AND symbol='HIVE'
        GROUP BY to_account
        ORDER BY total DESC
        LIMIT 10
    """), {"u": USERNAME}).fetchall()
    print(f"\nTop 10 recipients (HIVE sent):")
    for r in rows:
        print(f"  @{r[0]}: {r[1]:,.0f} HIVE ({r[2]}x)")

    # Top 10 recipients by HBD (sent)
    rows = conn.execute(text("""
        SELECT to_account, SUM(amount) AS total, COUNT(*) AS cnt
        FROM hafsql.operation_transfer_table
        WHERE from_account=:u AND symbol='HBD'
        GROUP BY to_account
        ORDER BY total DESC
        LIMIT 10
    """), {"u": USERNAME}).fetchall()
    print(f"\nTop 10 recipients (HBD sent):")
    for r in rows:
        print(f"  @{r[0]}: {r[1]:,.0f} HBD ({r[2]}x)")

    # Top 10 senders by HIVE (received)
    rows = conn.execute(text("""
        SELECT from_account, SUM(amount) AS total, COUNT(*) AS cnt
        FROM hafsql.operation_transfer_table
        WHERE to_account=:u AND symbol='HIVE'
        GROUP BY from_account
        ORDER BY total DESC
        LIMIT 10
    """), {"u": USERNAME}).fetchall()
    print(f"\nTop 10 senders (HIVE received):")
    for r in rows:
        print(f"  @{r[0]}: {r[1]:,.0f} HIVE ({r[2]}x)")

    # Top 10 senders by HBD (received)
    rows = conn.execute(text("""
        SELECT from_account, SUM(amount) AS total, COUNT(*) AS cnt
        FROM hafsql.operation_transfer_table
        WHERE to_account=:u AND symbol='HBD'
        GROUP BY from_account
        ORDER BY total DESC
        LIMIT 10
    """), {"u": USERNAME}).fetchall()
    print(f"\nTop 10 senders (HBD received):")
    for r in rows:
        print(f"  @{r[0]}: {r[1]:,.0f} HBD ({r[2]}x)")
