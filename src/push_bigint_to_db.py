import psycopg2
import re
from pathlib import Path

BASE = Path(__file__).parent.parent

SQL_FILES = [
    BASE / "sql/ddl/bronze_table_create.sql",
    BASE / "sql/ddl/silver_table_create.sql",
    BASE / "sql/ddl/gold_table_create.sql",
    BASE / "sql/silver/silver_import.sql",
    BASE / "sql/silver/silver_process.sql",
    BASE / "sql/gold/gold_import.sql",
]

# Order matters: dims before facts, bronze before silver before gold
CALLS = [
    # Bronze
    "call bronze.create_unlogged_bronze_main_tables()",
    "call bronze.create_unlogged_bronze_daily_tables()",
    "call bronze.bronze_raw_autovacuum_off()",
    "call bronze.bronze_daily_autovacuum_off()",
    # Silver daily + raw tables
    "call silver.create_silver_daily_tables()",
    "call silver.silver_daily_autovacuum_off()",
    "call silver.create_silver_main_tables()",
    "call silver.pgpartman_silver()",
    # Gold
    "call gold.create_gold_tables()",
    "call gold.gold_pgpartman()",
    "call gold.populate_dim_date()",
    "call gold.create_gold_indexes()",
]


def get_conn():
    return psycopg2.connect(
        host="localhost", port=5432,
        database="Demo_warehouse", user="sazid",
    )


def extract_procedures(sql: str) -> list[str]:
    """Extract create or replace procedure ... $$; blocks."""
    blocks = []
    pattern = re.compile(r'create\s+or\s+replace\s+procedure\s', re.IGNORECASE)
    pos = 0
    while True:
        m = pattern.search(sql, pos)
        if not m:
            break
        start = m.start()
        end_marker = sql.find('$$;', m.end())
        if end_marker == -1:
            break
        end = end_marker + 3
        blocks.append(sql[start:end])
        pos = end
    return blocks


def main():
    conn = get_conn()
    cur = conn.cursor()

    print("=== Step 1: CREATE OR REPLACE all procedures ===")
    for path in SQL_FILES:
        sql = path.read_text()
        procs = extract_procedures(sql)
        for proc in procs:
            name_m = re.search(r'procedure\s+([\w.]+)', proc, re.IGNORECASE)
            name = name_m.group(1) if name_m else "?"
            cur.execute(proc)
            conn.commit()
            print(f"  [OK] {name}")

    print("\n=== Step 2: Recreate all tables + setup ===")
    for call in CALLS:
        cur.execute(call)
        conn.commit()
        print(f"  [OK] {call}")

    cur.close()
    conn.close()
    print("\nDone — all tables recreated with bigint ID columns.")


if __name__ == "__main__":
    main()
