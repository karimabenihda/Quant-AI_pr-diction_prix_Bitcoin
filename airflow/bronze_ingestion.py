from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
import requests
import os

@dag(
    dag_id="btc_bronze_load",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["bronze", "bitcoin"]
)
def load_btc_data():
    
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

            data_collection_api()

    @task
    def save_bronze(data):
        df = pd.DataFrame(data)

        path = "/opt/airflow/data"
        os.makedirs(path, exist_ok=True)
        df.to_parquet(f"{path}/btc_data_bronze.parquet", index=False)
        
        return len(df)
    
    # task bronze
    bronze_task = fetch_and_save()

    # TRIGGER GOLD DAG
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="btc_gold_processing"
    )

    bronze_task >> trigger_gold


load_btc_data()