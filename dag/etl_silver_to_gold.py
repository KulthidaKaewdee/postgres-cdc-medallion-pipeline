from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'gold_layer_pipeline',
    default_args=default_args,
    description='ETL Job: Silver to Gold (ClickHouse)',
    schedule='0 1 * * *',  # ตี 1 ของทุกวัน
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'clickhouse', 'gold'],
) as dag:

    run_gold_job = SSHOperator(
        task_id='trigger_gold_to_clickhouse',
        ssh_conn_id='ssh_ssbeat_host',
        command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/spark/jobs/clickhouse-jdbc-0.6.0-all.jar \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.driver.extraClassPath="/opt/spark/external-jars/*" \
        --conf spark.executor.extraClassPath="/opt/spark/external-jars/*" \
        /opt/spark/jobs/silver_to_gold.py
        """,
        cmd_timeout=300
    )