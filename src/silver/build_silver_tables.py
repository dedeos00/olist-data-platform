from __future__ import annotations

from src.utils.trino_client import execute_sql, fetch_one


SILVER_TABLES = [
    "olist_orders",
    "olist_customers",
    "olist_order_items",
    "brazil_holidays",
]


def create_silver_schema() -> None:
    execute_sql(
        """
        CREATE SCHEMA IF NOT EXISTS iceberg.silver
        WITH (location = 's3://lakehouse/warehouse/silver')
        """
    )


def build_silver_orders() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.silver.olist_orders")

    execute_sql(
        """
        CREATE TABLE iceberg.silver.olist_orders AS
        SELECT
            order_id,
            customer_id,
            lower(trim(order_status)) AS order_status,
            TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            TRY_CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
            TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
            _ingestion_source_file,
            _ingestion_layer
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY order_id
                    ORDER BY order_purchase_timestamp DESC
                ) AS rn
            FROM iceberg.bronze.olist_orders
            WHERE order_id IS NOT NULL
        ) t
        WHERE rn = 1
        """
    )


def build_silver_customers() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.silver.olist_customers")

    execute_sql(
        """
        CREATE TABLE iceberg.silver.olist_customers AS
        SELECT
            customer_id,
            customer_unique_id,
            TRY_CAST(customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,
            lower(trim(customer_city)) AS customer_city,
            upper(trim(customer_state)) AS customer_state,
            _ingestion_source_file,
            _ingestion_layer
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY customer_id
                    ORDER BY customer_unique_id
                ) AS rn
            FROM iceberg.bronze.olist_customers
            WHERE customer_id IS NOT NULL
        ) t
        WHERE rn = 1
        """
    )


def build_silver_order_items() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.silver.olist_order_items")

    execute_sql(
        """
        CREATE TABLE iceberg.silver.olist_order_items AS
        SELECT
            order_id,
            TRY_CAST(order_item_id AS INTEGER) AS order_item_id,
            product_id,
            seller_id,
            TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
            TRY_CAST(price AS DOUBLE) AS price,
            TRY_CAST(freight_value AS DOUBLE) AS freight_value,
            _ingestion_source_file,
            _ingestion_layer
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY order_id, order_item_id
                    ORDER BY shipping_limit_date DESC
                ) AS rn
            FROM iceberg.bronze.olist_order_items
            WHERE order_id IS NOT NULL
              AND order_item_id IS NOT NULL
        ) t
        WHERE rn = 1
        """
    )


def build_silver_brazil_holidays() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.silver.brazil_holidays")

    execute_sql(
        """
        CREATE TABLE iceberg.silver.brazil_holidays AS
        SELECT
            TRY_CAST(holiday_date AS DATE) AS holiday_date,
            trim(holiday_name) AS holiday_name,
            lower(trim(holiday_type)) AS holiday_type,
            TRY_CAST(year AS INTEGER) AS year,
            _ingestion_source,
            _ingestion_url,
            _ingestion_layer,
            TRY_CAST(_ingestion_timestamp_utc AS TIMESTAMP) AS _ingestion_timestamp_utc
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY holiday_date, holiday_name
                    ORDER BY year
                ) AS rn
            FROM iceberg.bronze.brazil_holidays
            WHERE holiday_date IS NOT NULL
        ) t
        WHERE rn = 1
        """
    )


def build_all_silver_tables() -> None:
    create_silver_schema()
    build_silver_orders()
    build_silver_customers()
    build_silver_order_items()
    build_silver_brazil_holidays()


def validate_silver_counts() -> None:
    for table_name in SILVER_TABLES:
        row = fetch_one(f"SELECT COUNT(*) FROM iceberg.silver.{table_name}")
        count = row[0] if row else 0

        if count <= 0:
            raise ValueError(f"Tabela silver vazia: iceberg.silver.{table_name}")

        print(f"Validação OK: iceberg.silver.{table_name} possui {count} registros.")


def validate_silver_quality() -> None:
    checks = {
        "orders_sem_order_id": """
            SELECT COUNT(*)
            FROM iceberg.silver.olist_orders
            WHERE order_id IS NULL
        """,
        "customers_sem_customer_id": """
            SELECT COUNT(*)
            FROM iceberg.silver.olist_customers
            WHERE customer_id IS NULL
        """,
        "order_items_preco_negativo": """
            SELECT COUNT(*)
            FROM iceberg.silver.olist_order_items
            WHERE price < 0 OR freight_value < 0
        """,
        "holidays_sem_data": """
            SELECT COUNT(*)
            FROM iceberg.silver.brazil_holidays
            WHERE holiday_date IS NULL
        """,
    }

    for check_name, sql in checks.items():
        row = fetch_one(sql)
        invalid_count = row[0] if row else 0

        if invalid_count != 0:
            raise ValueError(
                f"Falha na qualidade Silver: {check_name} retornou {invalid_count}"
            )

        print(f"Qualidade OK: {check_name}")
