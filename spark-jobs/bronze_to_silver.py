# ==============================================================================
# bronze_to_silver.py
# สคริปต์แปลงข้อมูล Bronze (Raw) -> Silver (Clean & Current) โดยระบุ ID รายตาราง
# ==============================================================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException

# 1. ตั้งค่า Spark Session และเชื่อมต่อ MinIO
spark = (
    SparkSession.builder
    .appName("bronze-to-silver-dedup")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# 2. กำหนดชื่อตารางคู่กับ Primary Key (แก้ไขตามที่คุณแจ้งมา)
TABLE_CONFIG = {
    "customers": "customer_id",
    "orders":    "order_id",
    "items":     "item_id",
    "payments":  "payment_id",
    "shippings": "shipping_id"
}

print("🚀 Starting Silver Layer Processing...")

# วนลูปทีละตาราง พร้อมดึงชื่อ PK มาใช้
for table, pk_col in TABLE_CONFIG.items():
    print(f"------------------------------------------")
    print(f"📦 Processing Table: {table} (PK: {pk_col})")
    
    # 3. อ่านข้อมูล Bronze
    try:
        source_path = f"s3a://bronze/{table}/"
        table_bronze = spark.read.parquet(source_path)
    except Exception as e:
        print(f"⚠️  Skipping {table}: No data found or read error.")
        continue

    # 4. กรองเฉพาะ Insert/Update (after ไม่เป็น Null)
    clean = table_bronze.filter(col("after").isNotNull())

    if clean.count() == 0:
        print(f"⚠️  Skipping {table}: No valid records found.")
        continue

    # 5. แตกข้อมูล (Flatten) เอา field จาก struct 'after' ออกมา
    records = clean.select(
        col("after.*"),       
        col("event_ts_ms")
    )

    # 6. กำจัดข้อมูลซ้ำ (Deduplication) **จุดสำคัญที่แก้**
    # Logic: แบ่งกลุ่มตาม ID -> เรียงเวลาล่าสุดขึ้นก่อน -> เลือกแถวแรก
    try:
        w = Window.partitionBy(pk_col).orderBy(col("event_ts_ms").desc())

        latest = (
            records
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1) # เอาเฉพาะแถวที่ 1 (ล่าสุด)
            .drop("rn")             # ลบเลขลำดับทิ้ง
            .withColumn("updated_at", (col("event_ts_ms") / 1000).cast(TimestampType()))     # แปลง event_ts_ms (epoch หน่วย milliseconds) เป็น Timestamp
        )
    except AnalysisException:
        print(f"❌ Error: Column '{pk_col}' not found in table '{table}'. Check schema.")
        continue

    # 7. บันทึกลง Silver Layer (Overwrite เพื่อให้เป็นสถานะล่าสุดเสมอ)
    target_path = f"s3a://silver/{table}_current/"
    
    print(f"💾 Writing data to: {target_path}")
    latest.write.mode("overwrite").parquet(target_path)
    
    print(f"✅ Success: {table} is up-to-date.")

print("------------------------------------------")
print("🎉 All Silver jobs finished.")
spark.stop()