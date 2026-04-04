from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 3),
    "retries": 1,
}

with DAG(
    dag_id="credit_risk_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["exam", "credit_risk"],
) as dag:
    with TaskGroup("tg_precheck") as tg_precheck:
        check_credit_data = SqlSensor(
            task_id="check_credit_data",
            conn_id="postgres_default",
            sql="SELECT 1 FROM credit_applications WHERE application_date = '2026-04-03' LIMIT 1;",
            mode="poke",
            poke_interval=30,
            timeout=300,
        )

        check_blacklist_file = FileSensor(
            task_id="check_blacklist_file",
            filepath="/opt/airflow/files/blacklist_customers.csv",
            mode="poke",
            poke_interval=30,
            timeout=300,
        )
        [check_credit_data, check_blacklist_file]

    with TaskGroup("tg_ingestion_processing") as tg_ingestion_processing:
        upload_blacklist = BashOperator(
            task_id="upload_blacklist_to_minio",
            bash_command="""
            mc alias set myminio http://minio:9000 matrix matrix123 && \
            mc cp /opt/airflow/files/blacklist_customers.csv myminio/raw-data/
            """,
        )

        process_data_spark = BashOperator(
            task_id="process_data_spark",
            bash_command="spark-submit --jars /opt/spark/jars/postgresql-42.7.3.jar /opt/airflow/files/process_credit.py",
        )

        upload_blacklist >> process_data_spark

    with TaskGroup("tg_reporting") as tg_reporting:
        generate_report = BashOperator(
            task_id="generate_report",
            bash_command="spark-submit /opt/airflow/files/generate_report.py",
        )

        upload_report = BashOperator(
            task_id="upload_report_to_minio",
            bash_command="""
            mc alias set myminio http://minio:9000 matrix matrix123 && \
            mc cp /opt/airflow/files/summary_report/*.csv myminio/reports/daily_summary_{{ ds }}.csv
            """,
        )

        generate_report >> upload_report

    tg_precheck >> tg_ingestion_processing >> tg_reporting
