from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from src.bronze.load_olist_bronze import (
    create_bronze_schema,
    load_csv_to_iceberg,
    validate_bronze_counts,
    validate_raw_files_exist,
)


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
    def load_orders():
        load_csv_to_iceberg(
            table_name="olist_orders",
            filename="olist_orders_dataset.csv",
            sample_limit=5000,
        )

    @task
    def load_customers():
        load_csv_to_iceberg(
            table_name="olist_customers",
            filename="olist_customers_dataset.csv",
            sample_limit=5000,
        )

    @task
    def load_order_items():
        load_csv_to_iceberg(
            table_name="olist_order_items",
            filename="olist_order_items_dataset.csv",
            sample_limit=5000,
        )

    @task
    def validate_counts():
        validate_bronze_counts()

    check = check_raw_files()
    schema = create_schema()

    orders = load_orders()
    customers = load_customers()
    order_items = load_order_items()

    validation = validate_counts()

    check >> schema >> [orders, customers, order_items] >> validation


olist_bronze_pipeline()
