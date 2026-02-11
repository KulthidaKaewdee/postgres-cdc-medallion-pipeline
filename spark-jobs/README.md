# Spark Jobs (Data Processing Layer)

Repo นี้รวบรวม PySpark Scripts สำหรับประมวลผลข้อมูลใน Data Lakehouse ตามสถาปัตยกรรม Medallion Architecture (Bronze -> Silver -> Gold)

---

## รายละเอียดสคริปต์ในแต่ละ Layer

### 1. Bronze Layer (Ingestion & Streaming)
**File:** `cdc_to_bronze.py`
ทำหน้าที่รับข้อมูล CDC (Change Data Capture) จาก Kafka แบบ Real-time และบันทึกลง MinIO ในรูปแบบ Parquet

* **Input:** Kafka Topics (Debezium format with Avro)
* **Process:**
    * อ่านข้อมูลจาก Kafka แบบ Structured Streaming
    * ถอดรหัสข้อมูล Avro โดยดึง Schema จาก Schema Registry
    * แยกส่วนข้อมูล `before`, `after`, `op`, และ `ts_ms`
* **Output:** Parquet Files (Partitioned by table) เก็บใน S3/MinIO bucket `bronze`
* **Usage:** รับ parameter เป็นชื่อตาราง เช่น `spark-submit cdc_to_bronze.py orders`

### 2. Silver Layer (Cleaning & Deduplication)
**File:** `bronze_to_silver.py`
ทำหน้าที่แปลงข้อมูลดิบจาก Bronze ให้เป็นตารางที่สะอาดและมีสถานะล่าสุด (Deduplicated)

* **Input:** Parquet Files จาก Bronze Layer (MinIO)
* **Process:**
    * กรองเฉพาะแถวที่มีข้อมูลใหม่ (`after` is not null)
    * **Flattening:** แตกโครงสร้าง JSON/Struct ในฟิลด์ `after` ออกเป็นคอลัมน์ปกติ
    * **Deduplication:** กำจัดข้อมูลซ้ำโดยใช้ Window Function เลือกข้อมูลที่มี `event_ts_ms` ล่าสุดของแต่ละ Primary Key
* **Output:** Parquet Files เก็บใน S3/MinIO bucket `silver` โดยใช้โหมด Overwrite (Snapshot ล่าสุด)
* **Tables Processed:** customers, orders, items, payments, shippings

### 3. Gold Layer (Aggregation & Warehouse Loading)
**File:** `silver_to_gold.py`
ทำหน้าที่รวมข้อมูลจากหลายตาราง (Join) เพื่อสร้างเป็น Analytical Table และโหลดเข้าสู่ Data Warehouse

* **Input:** Parquet Files จาก Silver Layer (MinIO) และ Watermark จาก ClickHouse
* **Process:**
    * **Incremental Load:** ตรวจสอบค่า `max(updated_at)` จาก ClickHouse เพื่อดึงเฉพาะข้อมูลใหม่จาก Silver Layer
    * **Star Schema Join:** เชื่อมโยงข้อมูลจากตาราง Fact (Orders) เข้ากับ Dimensions (Customers, Items, Payments, Shippings)
    * **Data Projection:** เลือกและเปลี่ยนชื่อคอลัมน์ให้ตรงกับ Schema ปลายทาง
* **Output:** บันทึกข้อมูลลงตาราง `olist_gold_analytics` ใน ClickHouse ผ่าน JDBC

---

## Requirements
* Apache Spark 3.x
* Python 3.x
* Connectors:
    * `spark-sql-kafka`
    * `spark-avro`
    * `hadoop-aws` (สำหรับ S3/MinIO)
    * `clickhouse-jdbc`
