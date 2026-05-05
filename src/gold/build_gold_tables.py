from __future__ import annotations

from src.utils.trino_client import execute_sql, fetch_one


GOLD_TABLES = [
    "dim_cliente",
    "dim_tempo",
    "dim_status_pedido",
    "fato_pedidos",
]


def create_gold_schema() -> None:
    execute_sql(
        """
        CREATE SCHEMA IF NOT EXISTS iceberg.gold
        WITH (location = 's3://lakehouse/warehouse/gold')
        """
    )


def build_dim_cliente() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.gold.dim_cliente")

    execute_sql(
        """
        CREATE TABLE iceberg.gold.dim_cliente AS
        SELECT
            customer_id AS cliente_id,
            customer_unique_id AS cliente_unico_id,
            customer_zip_code_prefix AS cep_prefixo,
            customer_city AS cidade,
            customer_state AS estado
        FROM iceberg.silver.olist_customers
        WHERE customer_id IS NOT NULL
        """
    )


def build_dim_tempo() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.gold.dim_tempo")

    execute_sql(
        """
        CREATE TABLE iceberg.gold.dim_tempo AS
        WITH datas AS (
            SELECT DISTINCT CAST(order_purchase_timestamp AS DATE) AS data_ref
            FROM iceberg.silver.olist_orders
            WHERE order_purchase_timestamp IS NOT NULL

            UNION

            SELECT DISTINCT holiday_date AS data_ref
            FROM iceberg.silver.brazil_holidays
            WHERE holiday_date IS NOT NULL
        )
        SELECT
            data_ref AS data_id,
            year(data_ref) AS ano,
            month(data_ref) AS mes,
            day(data_ref) AS dia,
            quarter(data_ref) AS trimestre,
            day_of_week(data_ref) AS dia_semana,
            CASE
                WHEN day_of_week(data_ref) IN (6, 7) THEN true
                ELSE false
            END AS fim_de_semana,
            CASE
                WHEN h.holiday_date IS NOT NULL THEN true
                ELSE false
            END AS feriado_nacional,
            h.holiday_name AS nome_feriado
        FROM datas d
        LEFT JOIN iceberg.silver.brazil_holidays h
            ON d.data_ref = h.holiday_date
        WHERE data_ref IS NOT NULL
        """
    )


def build_dim_status_pedido() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.gold.dim_status_pedido")

    execute_sql(
        """
        CREATE TABLE iceberg.gold.dim_status_pedido AS
        SELECT DISTINCT
            order_status AS status_pedido_id,
            CASE
                WHEN order_status = 'delivered' THEN 'Entregue'
                WHEN order_status = 'shipped' THEN 'Enviado'
                WHEN order_status = 'canceled' THEN 'Cancelado'
                WHEN order_status = 'invoiced' THEN 'Faturado'
                WHEN order_status = 'processing' THEN 'Processando'
                WHEN order_status = 'unavailable' THEN 'Indisponível'
                WHEN order_status = 'approved' THEN 'Aprovado'
                WHEN order_status = 'created' THEN 'Criado'
                ELSE 'Outro'
            END AS status_pedido_descricao
        FROM iceberg.silver.olist_orders
        WHERE order_status IS NOT NULL
        """
    )


def build_fato_pedidos() -> None:
    execute_sql("DROP TABLE IF EXISTS iceberg.gold.fato_pedidos")

    execute_sql(
        """
        CREATE TABLE iceberg.gold.fato_pedidos AS
        WITH itens_agregados AS (
            SELECT
                order_id,
                COUNT(*) AS quantidade_itens,
                SUM(COALESCE(price, 0)) AS valor_produtos,
                SUM(COALESCE(freight_value, 0)) AS valor_frete,
                SUM(COALESCE(price, 0) + COALESCE(freight_value, 0)) AS valor_total
            FROM iceberg.silver.olist_order_items
            GROUP BY order_id
        )
        SELECT
            o.order_id AS pedido_id,
            o.customer_id AS cliente_id,
            CAST(o.order_purchase_timestamp AS DATE) AS data_compra_id,
            o.order_status AS status_pedido_id,

            c.customer_state AS estado_cliente,
            c.customer_city AS cidade_cliente,

            COALESCE(i.quantidade_itens, 0) AS quantidade_itens,
            COALESCE(i.valor_produtos, 0) AS valor_produtos,
            COALESCE(i.valor_frete, 0) AS valor_frete,
            COALESCE(i.valor_total, 0) AS valor_total,

            o.order_purchase_timestamp AS data_hora_compra,
            o.order_approved_at AS data_hora_aprovacao,
            o.order_delivered_carrier_date AS data_hora_envio_transportadora,
            o.order_delivered_customer_date AS data_hora_entrega_cliente,
            o.order_estimated_delivery_date AS data_hora_entrega_estimada,

            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                     AND o.order_purchase_timestamp IS NOT NULL
                THEN date_diff(
                    'day',
                    CAST(o.order_purchase_timestamp AS DATE),
                    CAST(o.order_delivered_customer_date AS DATE)
                )
                ELSE NULL
            END AS dias_para_entrega,

            CASE
                WHEN o.order_status = 'delivered' THEN true
                ELSE false
            END AS pedido_entregue,

            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                     AND o.order_estimated_delivery_date IS NOT NULL
                     AND o.order_delivered_customer_date > o.order_estimated_delivery_date
                THEN true
                ELSE false
            END AS pedido_atrasado,

            CASE
                WHEN h.holiday_date IS NOT NULL THEN true
                ELSE false
            END AS compra_em_feriado

        FROM iceberg.silver.olist_orders o
        LEFT JOIN itens_agregados i
            ON o.order_id = i.order_id
        LEFT JOIN iceberg.silver.olist_customers c
            ON o.customer_id = c.customer_id
        LEFT JOIN iceberg.silver.brazil_holidays h
            ON CAST(o.order_purchase_timestamp AS DATE) = h.holiday_date
        WHERE o.order_id IS NOT NULL
        """
    )


def build_all_gold_tables() -> None:
    create_gold_schema()
    build_dim_cliente()
    build_dim_tempo()
    build_dim_status_pedido()
    build_fato_pedidos()


def validate_gold_counts() -> None:
    for table_name in GOLD_TABLES:
        row = fetch_one(f"SELECT COUNT(*) FROM iceberg.gold.{table_name}")
        count = row[0] if row else 0

        if count <= 0:
            raise ValueError(f"Tabela gold vazia: iceberg.gold.{table_name}")

        print(f"Validação OK: iceberg.gold.{table_name} possui {count} registros.")


def validate_gold_quality() -> None:
    checks = {
        "fato_pedidos_sem_pedido_id": """
            SELECT COUNT(*)
            FROM iceberg.gold.fato_pedidos
            WHERE pedido_id IS NULL
        """,
        "fato_pedidos_valor_total_negativo": """
            SELECT COUNT(*)
            FROM iceberg.gold.fato_pedidos
            WHERE valor_total < 0
        """,
        "dim_cliente_sem_cliente_id": """
            SELECT COUNT(*)
            FROM iceberg.gold.dim_cliente
            WHERE cliente_id IS NULL
        """,
        "dim_tempo_sem_data_id": """
            SELECT COUNT(*)
            FROM iceberg.gold.dim_tempo
            WHERE data_id IS NULL
        """,
    }

    for check_name, sql in checks.items():
        row = fetch_one(sql)
        invalid_count = row[0] if row else 0

        if invalid_count != 0:
            raise ValueError(
                f"Falha na qualidade Gold: {check_name} retornou {invalid_count}"
            )

        print(f"Qualidade OK: {check_name}")
