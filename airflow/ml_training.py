from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from joblib import dump
import numpy as np

# =============================
# CONFIG
# =============================
FEATURE_COLS = [
    "open", "high", "low", "close", "volume",
    "quote_asset_volume", "number_of_trades",
    "taker_buy_base_volume", "taker_buy_quote_volume",
    "returns_col", "ma_05", "ma_10", "taker_ratio"
]

LABEL_COL = "close_t_plus_10"

# =============================
# DAG
# =============================
with DAG(
    dag_id="ml_random_forest_from_gold",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["ml", "random_forest", "gold"]
):

    # -----------------------------
    # 1. Charger les données GOLD
    # -----------------------------
    @task
    def load_gold_data():
        spark = SparkSession.builder.appName("ML-Gold").getOrCreate()

        df = spark.read.format("parquet").load(
            "/data/data_gold"
        )

        return df

    # ----------------------------------
    # 2. Split temporel train / test
    # ----------------------------------
    @task
    def train_test_split_time(df, train_ratio=0.8):
        total_rows = df.count()
        train_count = int(total_rows * train_ratio)

        train_df = df.limit(train_count)
        test_df = df.subtract(train_df)

        return train_df, test_df

    # ----------------------------------
    # 3. Spark → Pandas
    # ----------------------------------
    @task
    def spark_to_xy(train_df, test_df):
        train_pdf = train_df.select(FEATURE_COLS + [LABEL_COL]).toPandas()
        test_pdf = test_df.select(FEATURE_COLS + [LABEL_COL]).toPandas()

        X_train = train_pdf[FEATURE_COLS]
        y_train = train_pdf[LABEL_COL]

        X_test = test_pdf[FEATURE_COLS]
        y_test = test_pdf[LABEL_COL]

        return X_train, y_train, X_test, y_test

    # ----------------------------------
    # 4. Entraînement Random Forest
    # ----------------------------------
    @task
    def train_random_forest(X_train, y_train):
        model = RandomForestRegressor(
            n_estimators=300,
            max_depth=10,
            min_samples_leaf=20,
            random_state=42,
            n_jobs=-1
        )

        model.fit(X_train, y_train)
        return model

    # ----------------------------------
    # 5. Évaluation
    # ----------------------------------
    @task
    def evaluate_model(model, X_test, y_test):
        predictions = model.predict(X_test)

        mae = mean_absolute_error(y_test, predictions)
        rmse = np.sqrt(mean_squared_error(y_test, predictions))
        r2 = r2_score(y_test, predictions)

        print(f"MAE  = {mae}")
        print(f"RMSE = {rmse}")
        print(f"R²   = {r2}")

        return {
            "mae": mae,
            "rmse": rmse,
            "r2": r2
        }

    # ----------------------------------
    # 6. Sauvegarde du modèle
    # ----------------------------------
    @task
    def save_model(model):
        dump(model, "/models/random_forest_model.joblib")
        print("Modèle Random Forest sauvegardé")

    # =============================
    # ORCHESTRATION
    # =============================
    df = load_gold_data()
    train_df, test_df = train_test_split_time(df)
    X_train, y_train, X_test, y_test = spark_to_xy(train_df, test_df)
    model = train_random_forest(X_train, y_train)
    metrics = evaluate_model(model, X_test, y_test)
    save_model(model)
