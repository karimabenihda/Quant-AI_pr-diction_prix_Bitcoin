from airflow import dag
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as Fsum, avg, col




def dag_gold_processing():
    spark =SparkSession.builder \
        .appName("Bitcoin_Data")\
        .getOrCreate()
    
    #lire data bronze
    df = spark.read.parquet("/opt/airflow/data/bronze/btc_data_bronze.parquet")

    #fenetre temporelle
    window =Window.orderBy("open_time")

    #target T+10
    df = df.withColumn(
        "close_t_plus_10",
        F.lead("close",10)
    )

    #return
    df= df.withColumn(
        "returns_col",
        (F.col("close") - F.lag("close",1).over(window)) /
        F.lag("close",1).over(window)
    )

    #moyenns
    window_5 = window.rowsBetween(-4,0)
    window_10 = window.rowsBetween(-9,0)

    df=df.withColumn("ma_05",avg(col("close")).over(window_5))
    df=df.withColumn("ma_10",avg(col("close")).over(window_10))

    #taker ratio

    df= df.withColumn(
        "taker_ratio",
        F.col("taker_buy_base_volume") / F.col("volume")
    )

    # Check nulls (log)
    df.select([
        Fsum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

    # Drop NA critiques
    df = df.dropna(subset=["close_t_plus_10", "returns_col"])

    #drop column ignore
    df = df.drop("ignore")

    # 🔹 Sauvegarde SILVER
    df.write.mode("overwrite").parquet(
        "/opt/airflow/data/silver/btc_silver.parquet"
    )

    spark.stop()


@dag(
    dag_id="btc_gold_processing",
    start_date=datetime(2026, 1, 1),
    schedule=None,   
    catchup=False,
    tags=["silver", "bitcoin"]
)

#l'orchestration d execution 
def silver_dag():

    gold_task = PythonOperator(
        task_id="spark_gold_processing",
        python_callable=dag_gold_processing
    )

    trigger_ml = TriggerDagRunOperator(
        task_id="trigger_ml_dag",
        trigger_dag_id="btc_ml_training"
    )

    gold_task >> trigger_ml


silver_dag()