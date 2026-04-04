from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="spark_submit",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    # check_source = SqlSensor(
    #     task_id="check_source_data",
    #     conn_id="postgres_default",
    #     sql="""
    #         SELECT 1
    #         FROM src_transactions
    #         WHERE load_date = '{{ data_interval_start.in_timezone("Asia/Baku").strftime("%Y-%m-%d") }}'
    #         LIMIT 1
    #     """,
    #     poke_interval=60,
    #     timeout=600,
    #     mode="reschedule",
    # )

    bronze_load = SparkSubmitOperator(
        task_id="test",
        conn_id="spark_conn",
        application="/opt/airflow/files/test.py",
        # application_args=[
        #     "--process-date",
        #     '{{ data_interval_start.in_timezone("Asia/Baku").strftime("%Y-%m-%d") }}'
        # ],
        # packages="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.7",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.session.timeZone": "Asia/Baku",
            "spark.sql.shuffle.partitions": "50",
            "spark.sql.adaptive.enabled": "true",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent"
        },
        executor_memory="4G",
        executor_cores=2,
        num_executors=3,
    )

    end = EmptyOperator(task_id="end")

    start >> bronze_load >> end