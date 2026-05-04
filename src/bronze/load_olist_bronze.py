from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from src.utils.trino_client import execute_sql, fetch_one


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


def csv_to_local_parquet(
    table_name: str,
    filename: str,
    sample_limit: int | None = 5000,
) -> None:
    input_path = RAW_OLIST_DIR / filename
    output_dir = BRONZE_OLIST_DIR / table_name
    output_path = output_dir / "data.parquet"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path, dtype=str)
    df.columns = [_normalize_column_name(col) for col in df.columns]

    df["_ingestion_source_file"] = filename
    df["_ingestion_layer"] = "bronze"

    if sample_limit is not None:
        df = df.head(sample_limit)

    df.to_parquet(output_path, index=False)

    print(f"Arquivo bronze criado: {output_path}")
    print(f"Registros gravados: {len(df)}")


def load_csv_to_iceberg(
    table_name: str,
    filename: str,
    sample_limit: int | None = 5000,
) -> None:
    """
    Primeira versão estável da bronze:
    - lê CSV bruto
    - normaliza nomes de colunas
    - adiciona metadados de ingestão
    - grava Parquet local em data/bronze/olist/<table>/data.parquet

    A materialização Iceberg definitiva será feita na próxima etapa,
    usando estes Parquets como base.
    """
    csv_to_local_parquet(
        table_name=table_name,
        filename=filename,
        sample_limit=sample_limit,
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
