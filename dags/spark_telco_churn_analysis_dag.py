from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
spark_dag = DAG(
    dag_id="spark_telco_churn_analysis_dag",
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual trigger
    dagrun_timeout=timedelta(minutes=60),
    description="DAG for telco customer churn analysis using Spark",
    start_date=days_ago(1),
    catchup=False,
)

# Define the SparkSubmitOperator task
spark_telco_analysis = SparkSubmitOperator(
    application="/spark-scripts/spark-telco-churn-analysis.py",  # Path to your Spark script
    conn_id="spark_main",
    task_id="spark_telco_churn_task",
    dag=spark_dag,
    packages="org.postgresql:postgresql:42.2.18"
)

# Add the task to the DAG
spark_telco_analysis
