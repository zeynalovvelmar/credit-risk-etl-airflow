from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("CreditRiskReporting").getOrCreate()

df = spark.read.parquet("/opt/airflow/files/processed_data/")

summary = df.groupBy("risk_status").agg(
    F.count("*").alias("total_applications"),
    F.sum("requested_amount").alias("total_requested_amount"),
)

summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "/opt/airflow/files/summary_report/"
)

spark.stop()
