from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# 1. ตั้งค่า Default Arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,              # ถ้าพัง ให้ลองใหม่ 1 ครั้ง
    'retry_delay': timedelta(minutes=1), # รอ 1 นาทีก่อนลองใหม่
}

# 2. สร้าง DAG Object
with DAG(
    'silver_layer_pipeline',          # ชื่อ DAG ที่จะโชว์ในหน้าเว็บ
    default_args=default_args,
    description='ETL Job: Convert Bronze CDC data to Silver Layer',
    schedule='0 1 * * *',  # ตี 1 ของทุกวัน
    start_date=datetime(2024, 1, 1),  # วันที่เริ่ม (ใส่อดีตไว้เพื่อให้รันได้เลย)
    catchup=False,                    # ไม่ต้องย้อนหลังไปรันตั้งแต่อดีต
    tags=['spark', 'medallion', 'silver'],
) as dag:

    # 3. สร้าง Task
    # ใช้ SSHOperator สั่ง Docker ให้ทำงาน
    run_spark_job = SSHOperator(
            task_id='trigger_bronze_to_silver',
            ssh_conn_id='ssh_ssbeat_host',# ชื่อ Connection ที่เราสร้าง
            command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.driver.extraClassPath="/opt/spark/external-jars/*" \
            --conf spark.executor.extraClassPath="/opt/spark/external-jars/*" \
            /opt/spark/jobs/bronze_to_silver.py
            """,
            cmd_timeout=300
        )
