from __future__ import annotations

import time
from datetime import datetime

from airflow.decorators import dag, task

from src.bronze.load_holidays_bronze import (
    BRONZE_API_DIR,
    HOLIDAYS_TABLE_NAME,
    load_brazil_holidays_to_local_parquet,
    validate_brazil_holidays_local_parquet,
    validate_brazil_holidays_iceberg,
)

from src.bronze.register_bronze_iceberg import (
    register_external_parquet_as_iceberg_table,
)

from src.utils.trino_client import fetch_one


@dag(
    dag_id="holidays_bronze_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["api", "bronze", "brasilapi", "iceberg"],
)
def holidays_bronze_pipeline():
    @task
    def extract_api_to_parquet():
        load_brazil_holidays_to_local_parquet(
            start_year=2016,
            end_year=2018,
        )

    @task
    def validate_local_parquet():
        validate_brazil_holidays_local_parquet()

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
    def register_iceberg_table():
        parquet_path = BRONZE_API_DIR / HOLIDAYS_TABLE_NAME / "data.parquet"

        register_external_parquet_as_iceberg_table(
            table_name=HOLIDAYS_TABLE_NAME,
            parquet_path=str(parquet_path),
            namespace="bronze",
        )

    @task
    def validate_iceberg_table():
        validate_brazil_holidays_iceberg()

    extract = extract_api_to_parquet()
    local_validation = validate_local_parquet()
    trino_ready = wait_for_trino()
    register = register_iceberg_table()
    iceberg_validation = validate_iceberg_table()

    extract >> local_validation >> trino_ready >> register >> iceberg_validation


holidays_bronze_pipeline()
