# ===============================
# cdc_to_bronze.py
# ใช้ไฟล์เดียว อ่าน CDC ได้ทีละ table (dynamic ด้วย parameter)
# ===============================

import sys
import urllib.request
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro


# =========================================================
# 0. รับชื่อ table จาก command line
# ตัวอย่าง:
# spark-submit cdc_to_bronze.py orders
# spark-submit cdc_to_bronze.py customers
# =========================================================
if len(sys.argv) < 2:
    raise Exception("Usage: spark-submit cdc_to_bronze.py <table_name>")

TABLE = sys.argv[1]   # เช่น orders, customers, items


# =========================================================
# 1. config หลัก
# =========================================================
KAFKA_BOOTSTRAP = "broker:29092"                  # Kafka broker ภายใน Docker network
SCHEMA_REGISTRY = "http://schema-registry:8081"   # Schema Registry (Avro)


# =========================================================
# 2. สร้างชื่อ topic และ subject แบบ dynamic
# Debezium topic format: <server>.<schema>.<table>
# =========================================================
TOPIC = f"pg_data.public.{TABLE}"
SUBJECT_NAME = f"{TOPIC}-value"


# =========================================================
# 3. สร้าง SparkSession (MinIO / S3A config)
# =========================================================
spark = (
    SparkSession.builder
    .appName(f"cdc-to-bronze-{TABLE}")                             # ตั้งชื่อ job ตาม table
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # MinIO S3 endpoint
    .config("spark.hadoop.fs.s3a.path.style.access", "true")      # ใช้ path-style (จำเป็นกับ MinIO)
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )  # ปิด SSL เพราะ MinIO ใช้ HTTP
    .getOrCreate()
)


# =========================================================
# 4. ดึง Avro schema จาก Schema Registry ตาม table ที่รัน
# =========================================================
SCHEMA_URL = f"{SCHEMA_REGISTRY}/subjects/{SUBJECT_NAME}/versions/latest"

schema_json = None
for i in range(10):
    try:
        with urllib.request.urlopen(SCHEMA_URL) as url:
            data = json.loads(url.read().decode())
            schema_json = data["schema"]
            print(f"Successfully fetched schema for table: {TABLE}")
            break
    except Exception as e:
        print(f"Schema not ready yet for {TABLE}, retry {i+1}/10 ...")
        time.sleep(5)

if schema_json is None:
    raise Exception(f"Cannot fetch schema for {TABLE} after retries")


# =========================================================
# 5. อ่าน CDC จาก Kafka (1 topic ต่อ 1 run)
# =========================================================
raw = (
    spark.readStream                                  # Structured Streaming
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)                       # อ่าน topic เดียว
    .option("startingOffsets", "earliest")            # อ่านตั้งแต่แรก (เหมาะกับ bronze)
    .load()
)


# =========================================================
# 6. decode Avro
# Confluent Avro มี header 5 bytes:
#   - magic byte (1)
#   - schema id (4)
# ต้องตัดออกก่อน decode
# =========================================================
decoded = (
    raw
    .filter(col("value").isNotNull())
    .withColumn(
        "value_no_header",
        expr("substring(value, 6, length(value)-5)")
    )
    .select(
        from_avro(
            col("value_no_header"),
            schema_json
        ).alias("v"),
        col("timestamp").alias("kafka_ts")
    )
)


# =========================================================
# 7. สร้าง Bronze DataFrame (raw CDC)
# =========================================================
bronze = (
    decoded
    .select(
        col("v.before").alias("before"),      # ค่าเดิม (update / delete)
        col("v.after").alias("after"),        # ค่าใหม่ (insert / update)
        col("v.op").alias("op"),               # c, u, d, r
        col("v.source").alias("source"),       # metadata จาก Debezium
        col("v.ts_ms").alias("event_ts_ms"),   # เวลา event จาก DB
        col("kafka_ts"),
    )
    .withColumn("table", expr(f"'{TABLE}'"))  # ใส่ชื่อ table ชัด ๆ
)


# =========================================================
# 8. เขียนลง MinIO (Bronze layer)
# - แยก path ตาม table
# - แยก checkpoint ตาม table (สำคัญมาก)
# =========================================================
query = (
    bronze.writeStream
    .format("parquet")
    .option("path", f"s3a://bronze/{TABLE}/")                  # bronze per table
    .option(
        "checkpointLocation",
        f"s3a://bronze/_checkpoints/{TABLE}/"
    )                                                          # checkpoint per table
    .outputMode("append")
    .start()
)

query.awaitTermination()   # job รันต่อเนื่อง