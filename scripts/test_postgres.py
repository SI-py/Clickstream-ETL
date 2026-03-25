from app.db.postgres import get_connection


def main():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print("Connected to PostgreSQL")
            print(version)
    finally:
        conn.close()


if __name__ == "__main__":
    main()