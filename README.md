# olist-data-platform

Plataforma analítica end-to-end para o dataset Brazilian E-Commerce Public Dataset by Olist, construída como teste prático para vaga de Engenheiro(a) de Dados Pleno.

## Objetivo

Construir uma solução local e reprodutível que ingere dados de fontes heterogêneas, organiza os dados em camadas bronze, silver e gold, expõe tabelas analíticas confiáveis e entrega um dashboard de negócio.

## Stack prevista

- Docker Compose
- Apache Airflow
- MinIO/S3
- Apache Iceberg
- Parquet
- Trino ou DuckDB
- Apache Superset
- Python

## Fontes de dados

- Olist Brazilian E-Commerce Public Dataset
- API pública adicional a definir, preferencialmente BrasilAPI de feriados nacionais

## Estrutura do projeto

```text
olist-data-platform/
├── dags/
├── data/
├── docs/
├── notebooks/
├── sql/
├── src/
├── tests/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile
├── README.md
└── requirements.txt
