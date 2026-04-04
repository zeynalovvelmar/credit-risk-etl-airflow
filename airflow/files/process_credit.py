from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = (
    SparkSession.builder.appName("CreditRiskProcessing")
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
    .getOrCreate()
)

df_credit = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/airflow")
    .option("dbtable", "credit_applications")
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .load()
)

df_blacklist = (
    spark.read.option("header", "true")
    .csv("/opt/airflow/files/blacklist_customers.csv")
    .withColumn("in_blacklist", lit(True))
)

df_joined = df_credit.join(df_blacklist, on="customer_id", how="left")

df_status = df_joined.withColumn(
    "risk_status",
    when(col("in_blacklist") == True, "rejected")
    .when(
        (col("requested_amount") > col("monthly_income") * 12)
        | (col("existing_debt") > col("monthly_income") * 0.7)
        | (col("employment_years") < 1)
        | (col("age") < 21),
        "high_risk",
    )
    .otherwise("approved"),
).drop("in_blacklist")

df_status.write.mode("overwrite").parquet("/opt/airflow/files/processed_data/")

spark.stop()
