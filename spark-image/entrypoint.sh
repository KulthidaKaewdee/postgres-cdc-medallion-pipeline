#!/bin/bash
# entrypoint.sh

TABLE_NAME=$1

# เช็คว่ามีชื่อตารางส่งมาไหม
if [ -z "$TABLE_NAME" ]; then
    echo "Error: No table name provided. Usage: entrypoint.sh <table_name>"
    exit 1
fi

echo "🚀 Starting Spark Job for table: $TABLE_NAME"
echo "📦 MinIO Endpoint: ${MINIO_URL}"

# รันคำสั่ง Spark Submit
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=$HOSTNAME \
  --conf spark.driver.extraClassPath=$SPARK_EXTRA_CLASSPATH \
  --conf spark.executor.extraClassPath=$SPARK_EXTRA_CLASSPATH \
  --conf spark.hadoop.fs.s3a.endpoint=${MINIO_URL} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=512m \
  /opt/spark/jobs/cdc_to_bronze.py $TABLE_NAME