from __future__ import annotations

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType
from pyiceberg.io.pyarrow import schema_to_pyarrow
import pyarrow.parquet as pq

from src.bronze.load_olist_bronze import TABLES, BRONZE_OLIST_DIR
from src.utils.trino_client import fetch_one


def _get_catalog():
    return load_catalog(
        "rest",
        **{
            "uri": "http://iceberg-rest:8181",
            "warehouse": "s3://lakehouse/warehouse/",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def _build_iceberg_schema(column_names: list[str]) -> Schema:
    fields = [
        NestedField(
            field_id=index,
            name=column_name,
            field_type=StringType(),
            required=False,
        )
        for index, column_name in enumerate(column_names, start=1)
    ]

    return Schema(*fields)


def register_parquet_as_iceberg_table(table_name: str) -> None:
    parquet_path = BRONZE_OLIST_DIR / table_name / "data.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet bronze não encontrado: {parquet_path}")

    arrow_table = pq.read_table(parquet_path)

    if arrow_table.num_rows <= 0:
        raise ValueError(f"Parquet bronze vazio: {parquet_path}")

    # Garante todas as colunas como string, coerente com bronze/raw.
    arrow_table = arrow_table.cast(
        schema_to_pyarrow(_build_iceberg_schema(arrow_table.column_names))
    )

    catalog = _get_catalog()
    namespace = "bronze"
    full_table_name = f"{namespace}.{table_name}"

    catalog.create_namespace_if_not_exists(namespace)

    try:
        catalog.drop_table(full_table_name)
    except NoSuchTableError:
        pass

    iceberg_schema = _build_iceberg_schema(arrow_table.column_names)

    iceberg_table = catalog.create_table(
        identifier=full_table_name,
        schema=iceberg_schema,
    )

    iceberg_table.append(arrow_table)

    print(f"Tabela Iceberg criada: iceberg.{full_table_name}")
    print(f"Registros inseridos: {arrow_table.num_rows}")


def register_all_bronze_tables() -> None:
    for table_name in TABLES:
        register_parquet_as_iceberg_table(table_name)


def validate_iceberg_bronze_counts() -> None:
    for table_name in TABLES:
        row = fetch_one(f"SELECT COUNT(*) FROM iceberg.bronze.{table_name}")
        count = row[0] if row else 0

        if count <= 0:
            raise ValueError(f"Tabela Iceberg bronze vazia: iceberg.bronze.{table_name}")

        print(f"Validação OK: iceberg.bronze.{table_name} possui {count} registros.")
        
def register_external_parquet_as_iceberg_table(
    table_name: str,
    parquet_path: str,
    namespace: str = "bronze",
) -> None:
    from pathlib import Path
    import pyarrow.parquet as pq
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    parquet_file = Path(parquet_path)

    if not parquet_file.exists():
        raise FileNotFoundError(f"Parquet não encontrado: {parquet_file}")

    arrow_table = pq.read_table(parquet_file)

    if arrow_table.num_rows <= 0:
        raise ValueError(f"Parquet vazio: {parquet_file}")

    arrow_table = arrow_table.cast(
        schema_to_pyarrow(_build_iceberg_schema(arrow_table.column_names))
    )

    catalog = _get_catalog()
    full_table_name = f"{namespace}.{table_name}"

    catalog.create_namespace_if_not_exists(namespace)

    try:
        catalog.drop_table(full_table_name)
    except NoSuchTableError:
        pass

    iceberg_schema = _build_iceberg_schema(arrow_table.column_names)

    iceberg_table = catalog.create_table(
        identifier=full_table_name,
        schema=iceberg_schema,
    )

    iceberg_table.append(arrow_table)

    print(f"Tabela Iceberg criada: iceberg.{full_table_name}")
    print(f"Registros inseridos: {arrow_table.num_rows}")