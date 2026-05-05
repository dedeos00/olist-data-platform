from __future__ import annotations

import time
from datetime import datetime

from airflow.decorators import dag, task

from src.bronze.load_olist_bronze import (
    create_bronze_schema,
    load_csv_to_iceberg,
    validate_bronze_counts,
    validate_raw_files_exist,
)

from src.bronze.register_bronze_iceberg import (
    register_parquet_as_iceberg_table,
    validate_iceberg_bronze_counts,
)

from src.utils.trino_client import fetch_one


@dag(
    dag_id="olist_bronze_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "bronze", "iceberg"],
)
def olist_bronze_pipeline():
    @task
    def check_raw_files():
        validate_raw_files_exist()

    @task
    def create_schema():
        create_bronze_schema()

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

        raise RuntimeError(
            f"Trino não ficou disponível a tempo. Último erro: {last_error}"
        )

    @task
    def load_orders():
        load_csv_to_iceberg(
            table_name="olist_orders",
            filename="olist_orders_dataset.csv",
            sample_limit=500,
        )

    @task
    def load_customers():
        load_csv_to_iceberg(
            table_name="olist_customers",
            filename="olist_customers_dataset.csv",
            sample_limit=500,
        )

    @task
    def load_order_items():
        load_csv_to_iceberg(
            table_name="olist_order_items",
            filename="olist_order_items_dataset.csv",
            sample_limit=500,
        )

    @task
    def validate_local_parquet_counts():
        validate_bronze_counts()

    @task
    def register_orders_iceberg():
        register_parquet_as_iceberg_table("olist_orders")

    @task
    def register_customers_iceberg():
        register_parquet_as_iceberg_table("olist_customers")

    @task
    def register_order_items_iceberg():
        register_parquet_as_iceberg_table("olist_order_items")

    @task
    def validate_iceberg_counts():
        validate_iceberg_bronze_counts()

    check = check_raw_files()
    schema = create_schema()
    trino_ready = wait_for_trino()

    orders = load_orders()
    customers = load_customers()
    order_items = load_order_items()

    local_validation = validate_local_parquet_counts()

    register_orders = register_orders_iceberg()
    register_customers = register_customers_iceberg()
    register_order_items = register_order_items_iceberg()

    iceberg_validation = validate_iceberg_counts()

    check >> schema >> trino_ready >> [orders, customers, order_items] >> local_validation
    local_validation >> register_orders >> register_customers >> register_order_items >> iceberg_validation


olist_bronze_pipeline()