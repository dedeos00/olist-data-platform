from __future__ import annotations

import trino


def get_trino_connection():
    return trino.dbapi.connect(
        host="trino",
        port=8080,
        user="airflow",
        catalog="iceberg",
        schema="bronze",
    )


def execute_sql(sql: str) -> None:
    conn = get_trino_connection()
    cur = conn.cursor()
    cur.execute(sql)
    cur.fetchall()
    cur.close()
    conn.close()


def fetch_one(sql: str):
    conn = get_trino_connection()
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def fetch_all(sql: str):
    conn = get_trino_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
