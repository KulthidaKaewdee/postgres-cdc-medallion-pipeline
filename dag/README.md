# Airflow DAGs (Workflow Orchestration)

ไดเรกทอรีนี้รวบรวมไฟล์ **DAG (Directed Acyclic Graph)** ของ Apache Airflow ที่ทำหน้าที่ควบคุมและสั่งการ (Orchestrate) การประมวลผลข้อมูลในชั้น **Silver** และ **Gold** ของ Data Pipeline

> **Note:** การประมวลผลชั้น **Bronze** ทำงานแบบ Real-time Streaming (Always-on) จึงถูกรันผ่าน Docker Compose ไม่ได้ถูกสั่งงานผ่าน Airflow

---

## รายละเอียด DAGs

### 1. Silver Layer Pipeline
**File:** `etl_bronze_to_silver.py`
* **DAG Name:** `silver_layer_pipeline`
* **หน้าที่:** สั่งให้ Spark ทำการแปลงข้อมูลจาก Bronze (Raw) เป็น Silver (Cleaned & Deduplicated)
* **Schedule:** รันทุกวัน เวลา 01:00 น. (`0 1 * * *`)
* **Command:** ใช้ `SSHOperator` เชื่อมต่อไปยัง Host เพื่อสั่ง `docker exec` เข้าไปใน Spark Master และรันคำสั่ง `spark-submit` สำหรับไฟล์ `bronze_to_silver.py`

### 2. Gold Layer Pipeline
**File:** `etl_silver_to_gold.py`
* **DAG Name:** `gold_layer_pipeline`
* **หน้าที่:** สั่งให้ Spark ทำการ Join ตารางและทำ Incremental Load จาก Silver เข้าสู่ ClickHouse (Gold Layer)
* **Schedule:** รันทุกวัน เวลา 01:00 น. (`0 1 * * *`)
* **Command:** ใช้ `SSHOperator` เช่นเดียวกับ Silver แต่มีการเพิ่ม `--jars` สำหรับ `clickhouse-jdbc` เพื่อให้ Spark เขียนข้อมูลลง ClickHouse ได้

---

## เทคนิคการสั่งงาน (SSHOperator Pattern)

เนื่องจาก Airflow และ Spark รันอยู่บน Container แยกกัน ระบบจึงใช้ **SSHOperator** ในการข้าม Environment:

1. **Airflow** เชื่อมต่อ SSH ไปยังเครื่อง Host (ผ่าน Connection ID: `ssh_ssbeat_host`)
2. **Host** รับคำสั่งแล้วส่งต่อให้ Docker ผ่านคำสั่ง `docker exec spark-master ...`
3. **Spark Master** เริ่มต้นทำงาน `spark-submit` โดยดึงไฟล์ Python จาก Volume ที่เมาท์ไว้ (`/opt/spark/jobs/`)
