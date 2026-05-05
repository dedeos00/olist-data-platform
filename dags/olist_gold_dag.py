from __future__ import annotations

import time
from datetime import datetime

from airflow.decorators import dag, task

from src.gold.build_gold_tables import (
    build_all_gold_tables,
    create_gold_schema,
    validate_gold_counts,
    validate_gold_quality,
)

from src.utils.trino_client import fetch_one


@dag(
    dag_id="olist_gold_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "gold", "iceberg", "dimensional"],
)
def olist_gold_pipeline():
    @task
    def wait_for_trino():
        last_error = None

        for attempt in range(1, 31):
            try:
                row = fetch_one("SELECT 1")
                if row and row[0] == 1:
                    print("Trino está disponível.")
                    return
            except Exception as exc:
                last_error = exc
                print(f"Tentativa {attempt}/30: Trino ainda indisponível: {exc}")
                time.sleep(5)

        raise RuntimeError(f"Trino não ficou disponível a tempo. Último erro: {last_error}")

    @task
    def create_schema():
        create_gold_schema()

    @task
    def build_tables():
        build_all_gold_tables()

    @task
    def validate_counts():
        validate_gold_counts()

    @task
    def validate_quality():
        validate_gold_quality()

    trino_ready = wait_for_trino()
    schema = create_schema()
    build = build_tables()
    counts = validate_counts()
    quality = validate_quality()

    trino_ready >> schema >> build >> counts >> quality


olist_gold_pipeline()
