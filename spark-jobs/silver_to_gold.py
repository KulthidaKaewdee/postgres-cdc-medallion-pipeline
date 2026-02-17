import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField

# Setup Spark
spark = (
    SparkSession.builder
    .appName("silver-to-gold-clickhouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

print("🚀 Starting Gold Layer Processing (Incremental)...")

# Configuration สำหรับ ClickHouse
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/default?socket_timeout=600000&dataTransferTimeout=600000&http_connection_provider=HTTP_URL_CONNECTION&compress=0"
# CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:9000/default?socket_timeout=600000&dataTransferTimeout=600000&http_connection_provider=HTTP_URL_CONNECTION"
DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
TABLE_NAME = "olist_gold_analytics"

max_date = None

# หา Max Date จาก ClickHouse
try:
    print('🔍Checking latest data in ClickHouse...')
    # อ่านค่าวันที่มากที่สุดที่มีอยู่ในตาราง

    watermark_df = (
        spark.read.format("jdbc")
        .option("driver",DRIVER)
        .option("url", CLICKHOUSE_URL)
        .option("user", "default")
        .option("password", "")
        .option("query", f"SELECT max(updated_at) as max_val FROM {TABLE_NAME}")
        .load()
    )

    # # ดึงค่า max(updated_at) จากตารางมา 1 row ถ้าไม่มีแปลว่ารันครั้งแรก
    rows = watermark_df.collect()
    if rows and rows[0]["max_val"]:
        max_date = rows[0]["max_val"]
        print(f"🔄 Found Watermark: {max_date}")
    else:
        print("⚠️ No data found (First Run). Will load ALL data.")

except Exception as e:
    print(f"⚠️ Warning: Could not read watermark ({e}). Proceeding with Full Load.")    


print("🚀 Starting Gold Layer Processing...")

try:
    # อ่านข้อมูลจาก Silver (MinIO)
    df_orders   = spark.read.parquet("s3a://silver/orders_current/").alias("o")
    df_items    = spark.read.parquet("s3a://silver/items_current/").alias("i")
    df_customers = spark.read.parquet("s3a://silver/customers_current/").alias("c")
    df_payments  = spark.read.parquet("s3a://silver/payments_current/").alias("p")
    df_shippings = spark.read.parquet("s3a://silver/shippings_current/").alias("s")

    # กรองข้อมูลใหม่ (Incremental Filter)
    if max_date: 
        print(f"✂️ Filtering orders updated after: {max_date}")
        # กรองเอาเฉพาะที่ updated_at ของ Order ใหม่กว่าที่มีใน ClickHouse
        df_orders = df_orders.filter(col('o.updated_at') > max_date)

        if df_orders.count() == 0:
            print("💤 No new data found. Pipeline finished.")
            spark.stop()
            sys.exit(0)

    print("📦 Loaded Silver tables from MinIO.")

    # Join ข้อมูล (Star Schema)
    final_df = (
        df_orders.join(df_items, col("o.order_id") == col("i.order_id"), "inner")
              .join(df_customers, col("o.customer_id") == col("c.customer_id"), "left")
              .join(df_payments, col("o.order_id") == col("p.order_id"), "left")
              .join(df_shippings, col("o.order_id") == col("s.order_id"), "left")
    )

    # เลือกคอลัมน์ (Projection) ให้ตรงกับตาราง ClickHouse ที่สร้างไว้
    gold_table = final_df.select(
        col("o.order_id"),
        col("o.order_date"),
        col("o.total_amount").alias("order_total_amount"),
        
        col("i.item_name"),
        col("i.category").alias("item_category"),
        col("i.price").alias("item_price"),
        
        col("c.customer_name"),
        col("c.city").alias("customer_city"),
        col("c.state").alias("customer_state"),
        col("c.country").alias("customer_country"),
        col("c.registration_date").alias("customer_registration_date"),
        
        col("p.payment_amount"),
        col("p.payment_method"),
        
        col("s.shipping_method"),

        col("o.updated_at")
    )

    # บันทึกลง ClickHouse (Append ใส่ตารางเดิม)
    print("💾 Writing to ClickHouse...")

    (
        gold_table.write
        .format("jdbc")
        .option("driver", DRIVER)
        .option("url", CLICKHOUSE_URL)
        .option("user", "default")
        .option("password", "")
        .option("dbtable", TABLE_NAME)
        .option("isolateConnection", "false")

        #   Performance Tuning (ป้องกัน Error 1002)
        .option("batchsize", "10000") 
        .option("numPartitions", "4")
        
        # ใช้ Append (เพิ่มต่อท้าย) เพราะตารางเราเป็น ReplacingMergeTree
        .mode("append")
        .save()
    )
    print(f"✅ Success: Data appended to '{TABLE_NAME}'")

except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
finally:
    spark.stop()