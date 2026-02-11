# 🛠️ Spark Custom Image (Dependencies Management)

[cite_start]โฟลเดอร์นี้บรรจุไฟล์สำหรับการสร้าง **Custom Spark Image** เพื่อจัดการ Dependencies ทั้งหมดที่จำเป็นต้องใช้ใน Pipeline (เช่น Connector สำหรับ Kafka, Avro, และ S3A/MinIO) โดยใช้กลยุทธ์ **Multi-stage Build** เพื่อให้ Image มีขนาดเหมาะสมและไม่มี Library ที่ซ้ำซ้อน [cite: 1, 2]

---

## 📄 รายละเอียดไฟล์ในโฟลเดอร์

### 1. `pom.xml` (Maven Project Object Model)
ทำหน้าที่เป็น **Manifest File** เพื่อกำหนด Library เสริมที่ Spark ต้องใช้:
* **S3A / MinIO:** ใช้ `hadoop-aws` และ `aws-java-sdk-bundle` เพื่อให้ Spark สามารถอ่าน/เขียนไฟล์บน MinIO ได้
* **Kafka Integration:** ใช้ `spark-sql-kafka` สำหรับเชื่อมต่อกับ Kafka Topic
* **Avro Format:** ใช้ `spark-avro` และ `kafka-schema-registry-client` จาก Confluent เพื่อรองรับการ Deserialize ข้อมูลแบบ Avro ร่วมกับ Schema Registry
* **GCP BigQuery:** มีการเตรียม `spark-bigquery-with-dependencies` สำหรับรองรับการขยายระบบไปยัง Google Cloud ในอนาคต

### 2. `Dockerfile` (Multi-stage Build)
[cite_start]ทำหน้าที่สร้างสภาพแวดล้อมสำหรับการรัน Spark Job[cite: 1]:
* [cite_start]**Stage 1 (Builder):** ใช้ Maven Image เพื่อดาวน์โหลด Dependencies ทั้งหมดตามที่ระบุใน `pom.xml` มาเก็บไว้ที่ `/app/jars` และทำการลบไฟล์ที่ซ้ำซ้อนกับระบบหลักของ Spark (เช่น `spark-core`, `hadoop-common`) เพื่อป้องกันปัญหา Jar Conflict [cite: 1, 2]
* [cite_start]**Stage 2 (Final Image):** ใช้ Base Image จาก `apache/spark:3.5.1-python3` แล้วคัดลอกเฉพาะ Jars ที่คัดกรองแล้วไปไว้ที่ `/opt/spark/external-jars` และตั้งค่า `SPARK_EXTRA_CLASSPATH` เพื่อให้ Spark โหลด Library เหล่านี้โดยอัตโนมัติ [cite: 1, 3]

### 3. `entrypoint.sh` (Job Launcher)
เป็นสคริปต์ควบคุมการเริ่มงานของ Container:
* รับพารามิเตอร์เป็น **ชื่อตาราง** (Table Name) เพื่อระบุว่า Job นี้จะทำ CDC สำหรับตารางใด
* สั่งรันคำสั่ง `spark-submit` ไปยัง Spark Master Cluster
* กำหนดค่า Resource เบื้องต้น (Cores=1, Memory=512m) และตั้งค่า S3A Endpoint ให้ชี้ไปยัง MinIO ผ่าน Environment Variable

---

## 🏗️ วิธีการสร้าง Image
รันคำสั่งนี้ที่ Root ของโปรเจ็ค (หรือในโฟลเดอร์ที่มี Dockerfile):
```bash
docker build -t spark-custom:3.5.1 .
