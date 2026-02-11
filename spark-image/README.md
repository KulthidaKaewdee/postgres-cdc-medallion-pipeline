# 🛠️ Spark Custom Image (Dependencies Management)

โฟลเดอร์นี้บรรจุไฟล์สำหรับการสร้าง **Custom Spark Image** เพื่อจัดการ Library เสริมทั้งหมดที่จำเป็นต้องใช้ใน Pipeline เช่น Connector สำหรับ Kafka, Avro, และ MinIO โดยใช้กลยุทธ์ **Multi-stage Build** เพื่อให้ Image มีขนาดเล็กและทำงานได้รวดเร็วที่สุด

---

## รายละเอียดไฟล์สำคัญ

### 1. `pom.xml` (Maven Project)
ทำหน้าที่เป็นศูนย์กลางในการกำหนด Library ที่ Spark ต้องใช้ (Manifest File):
* **MinIO (S3A):** ใช้ `hadoop-aws` เพื่อให้ Spark เขียนไฟล์ลง Object Storage ได้
* **Kafka Integration:** ใช้ `spark-sql-kafka` สำหรับดึงข้อมูลจาก Kafka
* **Avro & Schema Registry:** รองรับการถอดรหัสข้อมูล Avro ร่วมกับ Confluent Schema Registry
* **BigQuery:** เตรียมพร้อมสำหรับการขยายระบบไปยัง Google Cloud ในอนาคต

### 2. `Dockerfile` (Multi-stage Build)
ขั้นตอนการสร้าง Image เพื่อประสิทธิภาพสูงสุด:
* **ขั้นตอนที่ 1 (Builder):** ใช้ Maven ดาวน์โหลดไฟล์ `.jar` ทั้งหมดที่ระบุใน `pom.xml` และทำการลบไฟล์ที่ซ้ำซ้อนกับระบบหลักของ Spark ทิ้ง เพื่อป้องกันปัญหาโปรแกรมตีกัน (Jar Conflict)
* **ขั้นตอนที่ 2 (Final Image):** นำเฉพาะไฟล์ที่คัดกรองแล้วไปวางใน Image หลักของ Spark และตั้งค่า `SPARK_EXTRA_CLASSPATH` เพื่อให้ Spark เรียกใช้งาน Library เหล่านี้ได้ทันที

### 3. `entrypoint.sh` (Job Launcher)
สคริปต์สำหรับสั่งรันงาน (Spark Submit) แบบอัตโนมัติ:
* **Table Parameter:** รับชื่อตารางที่ต้องการประมวลผลเป็นพารามิเตอร์ (เช่น `orders`, `customers`)
* **Config Preset:** ตั้งค่าการเชื่อมต่อกับ MinIO (S3A) และจำกัดการใช้ทรัพยากร (Cores/Memory) ให้เหมาะสมกับการรันแบบ Container

---

## วิธีการสร้าง Image
รันคำสั่งนี้ภายในโฟลเดอร์ `spark-image` เพื่อสร้าง Image สำหรับใช้งานในโปรเจ็ค:

```bash
docker build -t spark-custom:3.5.1 .
```
