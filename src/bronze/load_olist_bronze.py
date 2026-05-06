from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from src.utils.trino_client import execute_sql


RAW_OLIST_DIR = Path("/opt/airflow/data/raw/olist")
BRONZE_OLIST_DIR = Path("/opt/airflow/data/bronze/olist")

TABLES = {
    "olist_orders": "olist_orders_dataset.csv",
    "olist_customers": "olist_customers_dataset.csv",
    "olist_order_items": "olist_order_items_dataset.csv",
}


def _normalize_column_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def _read_csv(filename: str) -> pd.DataFrame:
    path = RAW_OLIST_DIR / filename

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    df = pd.read_csv(path, dtype=str)
    df.columns = [_normalize_column_name(col) for col in df.columns]
    return df


def _write_bronze_parquet(df: pd.DataFrame, table_name: str, source_file: str) -> None:
    output_dir = BRONZE_OLIST_DIR / table_name
    output_path = output_dir / "data.parquet"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["_ingestion_source_file"] = source_file
    df["_ingestion_layer"] = "bronze"

    df.to_parquet(output_path, index=False)

    print(f"Arquivo bronze criado: {output_path}")
    print(f"Tabela: {table_name}")
    print(f"Registros gravados: {len(df)}")


def validate_raw_files_exist() -> None:
    missing = []

    for filename in TABLES.values():
        path = RAW_OLIST_DIR / filename
        if not path.exists():
            missing.append(str(path))

    if missing:
        raise FileNotFoundError(
            "Arquivos Olist não encontrados. Rode make download-olist. Faltando: "
            + ", ".join(missing)
        )


def create_bronze_schema() -> None:
    execute_sql(
        """
        CREATE SCHEMA IF NOT EXISTS iceberg.bronze
        WITH (location = 's3://lakehouse/warehouse/bronze')
        """
    )


def load_olist_related_sample_to_parquet(sample_limit: int = 2000) -> None:
    """
    Gera uma amostra relacional consistente da Olist.

    Em vez de pegar head(N) isolado de cada CSV, esta função:
    1. seleciona N pedidos em orders;
    2. filtra customers usando os customer_id desses pedidos;
    3. filtra order_items usando os order_id desses pedidos.

    Isso preserva os joins da camada Gold e evita métricas quebradas.
    """
    validate_raw_files_exist()

    orders = _read_csv(TABLES["olist_orders"])
    customers = _read_csv(TABLES["olist_customers"])
    order_items = _read_csv(TABLES["olist_order_items"])

    sampled_orders = orders.head(sample_limit).copy()

    sampled_order_ids = set(sampled_orders["order_id"].dropna().unique())
    sampled_customer_ids = set(sampled_orders["customer_id"].dropna().unique())

    sampled_customers = customers[
        customers["customer_id"].isin(sampled_customer_ids)
    ].copy()

    sampled_order_items = order_items[
        order_items["order_id"].isin(sampled_order_ids)
    ].copy()

    if sampled_orders.empty:
        raise ValueError("Amostra de orders ficou vazia.")

    if sampled_customers.empty:
        raise ValueError("Amostra de customers ficou vazia.")

    if sampled_order_items.empty:
        raise ValueError("Amostra de order_items ficou vazia.")

    _write_bronze_parquet(
        df=sampled_orders,
        table_name="olist_orders",
        source_file=TABLES["olist_orders"],
    )

    _write_bronze_parquet(
        df=sampled_customers,
        table_name="olist_customers",
        source_file=TABLES["olist_customers"],
    )

    _write_bronze_parquet(
        df=sampled_order_items,
        table_name="olist_order_items",
        source_file=TABLES["olist_order_items"],
    )


def load_csv_to_iceberg(
    table_name: str,
    filename: str,
    sample_limit: int | None = 2000,
) -> None:
    """
    Mantido por compatibilidade com versões antigas da DAG.

    Para garantir amostra relacional, prefira usar
    load_olist_related_sample_to_parquet().
    """
    df = _read_csv(filename)

    if sample_limit is not None:
        df = df.head(sample_limit)

    _write_bronze_parquet(
        df=df,
        table_name=table_name,
        source_file=filename,
    )


def validate_bronze_counts() -> None:
    for table_name in TABLES:
        path = BRONZE_OLIST_DIR / table_name / "data.parquet"

        if not path.exists():
            raise FileNotFoundError(f"Arquivo bronze não encontrado: {path}")

        df = pd.read_parquet(path)
        count = len(df)

        if count <= 0:
            raise ValueError(f"Arquivo bronze {table_name} está vazio.")

        print(f"Validação OK: bronze local {table_name} possui {count} registros.")