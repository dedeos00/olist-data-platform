from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_olist_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "setup"],
)
def hello_olist_pipeline():
    @task
    def hello():
        print("Airflow funcionando para o projeto olist-data-platform.")

    hello()


hello_olist_pipeline()
