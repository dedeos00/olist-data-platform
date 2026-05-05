from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd

from src.utils.trino_client import fetch_one


BRONZE_API_DIR = Path("/opt/airflow/data/bronze/api")
HOLIDAYS_TABLE_NAME = "brazil_holidays"


def fetch_brazil_holidays(year: int) -> list[dict]:
    url = f"https://brasilapi.com.br/api/feriados/v1/{year}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError(f"Resposta inesperada da BrasilAPI para {year}: {data}")

    return data


def load_brazil_holidays_to_local_parquet(
    start_year: int = 2016,
    end_year: int = 2018,
) -> None:
    rows = []
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    for year in range(start_year, end_year + 1):
        holidays = fetch_brazil_holidays(year)

        for item in holidays:
            rows.append(
                {
                    "holiday_date": str(item.get("date")),
                    "holiday_name": str(item.get("name")),
                    "holiday_type": str(item.get("type")),
                    "year": str(year),
                    "_ingestion_source": "brasilapi_feriados",
                    "_ingestion_url": f"https://brasilapi.com.br/api/feriados/v1/{year}",
                    "_ingestion_layer": "bronze",
                    "_ingestion_timestamp_utc": ingestion_ts,
                }
            )

    if not rows:
        raise ValueError("Nenhum feriado retornado pela BrasilAPI.")

    df = pd.DataFrame(rows)

    output_dir = BRONZE_API_DIR / HOLIDAYS_TABLE_NAME
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "data.parquet"
    df.to_parquet(output_path, index=False)

    print(f"Arquivo bronze API criado: {output_path}")
    print(f"Registros gravados: {len(df)}")


def validate_brazil_holidays_local_parquet() -> None:
    path = BRONZE_API_DIR / HOLIDAYS_TABLE_NAME / "data.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Parquet da API não encontrado: {path}")

    df = pd.read_parquet(path)

    if df.empty:
        raise ValueError("Parquet de feriados está vazio.")

    required_columns = {
        "holiday_date",
        "holiday_name",
        "holiday_type",
        "year",
        "_ingestion_source",
        "_ingestion_layer",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")

    print(f"Validação OK: {len(df)} feriados carregados no bronze local.")


def validate_brazil_holidays_iceberg() -> None:
    row = fetch_one("SELECT COUNT(*) FROM iceberg.bronze.brazil_holidays")
    count = row[0] if row else 0

    if count <= 0:
        raise ValueError("Tabela Iceberg bronze.brazil_holidays está vazia.")

    print(f"Validação OK: iceberg.bronze.brazil_holidays possui {count} registros.")
