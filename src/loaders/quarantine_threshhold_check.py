import psycopg2


def main():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='Demo_warehouse',
        user='sazid',
    )
    try:
        cur = conn.cursor()
        cur.execute("call silver.quarantine_threshold_check();")
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Quarantine threshold check failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
