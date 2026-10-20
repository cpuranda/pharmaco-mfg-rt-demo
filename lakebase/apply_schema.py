#!/usr/bin/env python3
"""Apply lakebase/schema.sql to the Lakebase Postgres instance.

Auth: OAuth token passed as the Postgres password (env PGPASSWORD), Databricks
identity as the user (env PGUSER). Host/db from env. Never hardcodes secrets."""
import os
import sys
import psycopg

HOST = os.environ["PGHOST"]
PORT = os.environ.get("PGPORT", "5432")
DB = os.environ.get("PGDATABASE", "databricks_postgres")
USER = os.environ["PGUSER"]
PWD = os.environ["PGPASSWORD"]
SCHEMA_FILE = os.environ.get("SCHEMA_FILE",
                             os.path.join(os.path.dirname(__file__), "schema.sql"))


def main() -> int:
    with open(SCHEMA_FILE) as f:
        sql = f.read()
    with psycopg.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PWD,
                         sslmode="require", autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            # verify
            cur.execute("""SELECT table_name FROM information_schema.tables
                           WHERE table_schema = 'pharmaco_mfg_rt' ORDER BY table_name""")
            tables = [r[0] for r in cur.fetchall()]
            print("tables in pharmaco_mfg_rt:", tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())
