# Credit Risk ETL Pipeline

An automated Data Engineering pipeline orchestrating credit risk data transformations using Apache Airflow and PySpark.

## Tech Stack
- **Orchestration:** Apache Airflow
- **Processing:** Apache Spark (PySpark)
- **Infrastructure:** Docker Compose

## Project Structure
- \credit_risk_dag.py\: Airflow DAG scheduling ETL jobs.
- \process_credit.py\: PySpark script for risk data transformation.
- \credit_applications.sql\: Source data schema.
- \generate_report.py\: Reporting module.

## Setup
Deploy via \docker-compose up -d\.