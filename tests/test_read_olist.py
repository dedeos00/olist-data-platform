from pathlib import Path
import pandas as pd


def test_read_orders_dataset():
    path = Path("data/raw/olist/olist_orders_dataset.csv")
    assert path.exists(), "Arquivo olist_orders_dataset.csv não encontrado."

    df = pd.read_csv(path)
    assert len(df) > 0
    assert "order_id" in df.columns
