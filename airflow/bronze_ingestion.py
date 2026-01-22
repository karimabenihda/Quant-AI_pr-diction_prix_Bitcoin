from airflow.decorators import dag , task
import pandas as pd
from datetime import datetime
import os




SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 600

@dag(
    dag_id="btc_bronze_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["bronze", "bitcoin"]
)
def bronze_ingestion_dag():

    @task
    def fetch_data():
    import requests
    import pandas as pd

    SYMBOL = 'BTCUSDT'
    INTERVAL = '1m'
    LIMIT = 600

        def data_collection_api():
            response = requests.get(
                url='https://api.binance.com/api/v3/klines',
                params={"symbol": SYMBOL, "interval": INTERVAL, "limit": LIMIT}
            )
            if response.status_code != 200:
                print(f"Erreur {response.status_code}")
                return None
            data = response.json()
            columns = [
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
            ]
            df = pd.DataFrame(data, columns=columns)
            numeric_cols = ["open", "high", "low", "close", "volume",
                            "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]
            df[numeric_cols] = df[numeric_cols].astype(float)
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms").astype('datetime64[us]')
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms").astype('datetime64[us]')
            return df

        return data_collection_api()

    @task
    def save_bronze(data):
        df = pd.DataFrame(data)

        path = "/opt/airflow/data/bronze"
        os.makedirs(path, exist_ok=True)

        file_path = f"{path}/btc_bronze.parquet"
        df.to_parquet(file_path, index=False)

    raw_data = fetch_data()
    save_bronze(raw_data)

bronze_ingestion_dag()
