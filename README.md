# PostgreSQL CDC Medallion Pipeline

โปรเจ็คนี้คือการสร้าง **Data Lakehouse** ที่สมบูรณ์แบบโดยใช้สถาปัตยกรรม **Medallion Architecture** (Bronze, Silver, Gold) เพื่อจัดการข้อมูลแบบ **Near Real-time** โดยใช้เทคนิค **Change Data Capture (CDC)** เพื่อดึงข้อมูลการเปลี่ยนแปลงจาก PostgreSQL ส่งผ่าน Kafka และประมวลผลด้วย Spark เพื่อนำไปใช้งานวิเคราะห์บน ClickHouse และ Power BI

---
### System Architecture
![System Architecture](https://github.com/KulthidaKaewdee/postgres-cdc-medallion-pipeline/blob/main/system%20architecture.jpg)

---

## Architecture & Technology Stack

ระบบทำงานในรูปแบบ Containerized บน Docker Compose โดยแบ่งหน้าที่ของแต่ละเทคโนโลยีดังนี้:

* **Source Database:** PostgreSQL (OLTP) เปิดใช้งาน Logical Replication และ WAL Level แบบ Logical
* **Data Ingestion:** Debezium (Kafka Connect) ดึงข้อมูลการเปลี่ยนแปลงจาก Postgres ส่งไปยัง Kafka ในรูปแบบ **Avro**
* **Event Streaming:** Apache Kafka (Confluent Platform) พร้อม Schema Registry สำหรับจัดการ Data Schema
* **Processing Engine:** Apache Spark (Structured Streaming & Batch) สำหรับการประมวลผลข้อมูลในแต่ละชั้น
* **Orchestration:** Apache Airflow จัดลำดับและควบคุมการทำงานของเลเยอร์ต่างๆ
* **Data Lake (Storage):** MinIO (S3-Compatible) สำหรับจัดเก็บข้อมูล Bronze และ Silver
* **Data Warehouse:** ClickHouse สำหรับการทำ Serving Layer และ Analytical Query ความเร็วสูง

---

## Medallion Architecture Details

### 1. Bronze Layer (Raw Data)
* **Ingestion:** รับข้อมูลจาก Kafka Topic (Avro) และ Decode ด้วย Schema Registry
* **Storage:** บันทึกข้อมูลดิบลงใน MinIO แยกตามรายตารางในรูปแบบ **Parquet**
* **Checkpointing:** มีการทำ Checkpointing รายตารางเพื่อความแม่นยำในการทำ Streaming

### 2. Silver Layer (Cleaned & Curated)
* **Deduplication:** นำข้อมูลจาก Bronze มาทำการ Clean และเลือกเฉพาะสถานะล่าสุดของแต่ละ ID (Latest State)
* **Flattening:** แปลงโครงสร้าง CDC (Before/After) ให้เป็นตารางรูปแบบปกติ
* **Scheduling:** รันผ่าน Airflow DAG ตามรอบที่กำหนด เพื่อให้ข้อมูลใน Silver พร้อมใช้งานเสมอ

### 3. Gold Layer (Business Ready)
* **Incremental Load:** ใช้เทคนิค **Watermark** โดยอ่านค่า `updated_at` ล่าสุดจาก ClickHouse เพื่อดึงเฉพาะข้อมูลใหม่จาก Silver ไปเติม
* **Data Aggregation:** ทำการ Join ตารางต่างๆ (Orders, Customers, Items, Payments, Shippings) เข้าด้วยกันตามหลัก Star Schema
* **Serving:** จัดเก็บข้อมูลใน ClickHouse พร้อมให้ Power BI ดึงไปสร้าง Dashboard

---
## Dashboard
![executive-overview-report](https://github.com/KulthidaKaewdee/postgres-cdc-medallion-pipeline/blob/main/executive-overview-report.png)
![product-analytics-report](https://github.com/KulthidaKaewdee/postgres-cdc-medallion-pipeline/blob/main/product-analytics-report.png)
![customer&26geography-report](https://github.com/KulthidaKaewdee/postgres-cdc-medallion-pipeline/blob/main/customer%26geography-report.png)

---

## Getting Started

### 1. Build Image & Start Services
สร้าง Custom Spark Image เพื่อรวม Dependencies (S3A, Avro, JDBC) และเริ่มรันระบบทั้งหมด

Build Spark Image
```bash
docker build -t spark-custom:3.5.1 spark-image
```

เริ่มระบบทั้งหมด
```bash
docker compose up -d
```

### 2. PostgreSQL Source Setup
เข้าไปสร้าง User และกำหนดสิทธิ์สำหรับการทำ Replication ภายใน Container

เข้าสู่ Postgres Container
```bash
docker exec -it postgres-cdc-mel psql -U admin -d data
```

ตั้งค่าสิทธิ์สำหรับ Debezium
```sql
CREATE ROLE debezium LOGIN PASSWORD 'dbzium123' REPLICATION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER TABLE public.orders OWNER TO debezium;
```

### 3. Debezium Connector Installation
ติดตั้ง Connector ผ่าน REST API เพื่อเริ่มกระบวนการ CDC จากทุกตาราง

ติดตั้ง Connector
```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @connector-cdc-all-tables.json
```

ตรวจสอบสถานะ (ต้องขึ้น 'RUNNING')
```bash
curl http://localhost:8083/connectors/postgres-cdc-all-tables/status
```

### 4. Data Pipeline Execution
* **Bronze Layer:** รันอัตโนมัติเป็น Service แยกตามตารางผ่าน Docker Compose
* **Silver & Gold Layers:** ควบคุมการทำงานผ่าน Airflow DAGs โดยตั้งเวลาทำงานไว้ที่ 01:00 น. ของทุกวัน

---

## 🔑 Credentials & Web UI
* **MinIO Console:** http://localhost:7001 (User: `minioadmin` / Pass: `miniopassword`)
* **Spark Master UI:** http://localhost:18080
* **Confluent Control Center:** http://localhost:9021
* **ClickHouse UI:** http://localhost:8123

---

## 📊 Visualization (Power BI)
เชื่อมต่อ ClickHouse ผ่าน Web Connector เพื่อดึงข้อมูลไปแสดงผล:
* **URL:** `http://<HOST_IP>:8123/?query=SELECT * FROM default.olist_gold_analytics FORMAT CSVWithNames`
