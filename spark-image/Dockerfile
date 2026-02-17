# --- Stage 1: Downloader ---
FROM maven:3.9-amazoncorretto-11 AS builder
WORKDIR /app
COPY pom.xml .

# 1. โหลดทุกอย่างมากองรวมกันก่อน

RUN mvn dependency:copy-dependencies -DoutputDirectory=/app/jars -DexcludeGroupIds=org.scala-lang

# เพิ่มบรรทัดนี้: ลบ Scala library ใดๆ ที่อาจหลุดรอดมา (เช่น scala-collection-compat, scala-java8-compat)
RUN rm -f /app/jars/scala-*.jar

# ... (ส่วนการลบ spark-core เหมือนเดิม) ...
RUN rm -f /app/jars/spark-core_*.jar \
          /app/jars/spark-sql_*.jar \
          /app/jars/spark-catalyst_*.jar \
          /app/jars/spark-tags_*.jar \
          /app/jars/hadoop-client-*.jar \
          /app/jars/hadoop-common-*.jar

# --- Stage 2: Final Image ---
FROM apache/spark:3.5.1-python3
USER root

# สร้าง External Folder
RUN mkdir -p /opt/spark/external-jars

# Copy ไฟล์ที่คัดกรองแล้ว (เหลือแต่ Connector และ Library เสริม)
COPY --from=builder /app/jars /opt/spark/external-jars/

# Entrypoint
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

# ชี้ Classpath ไปที่ Folder เสริม
ENV SPARK_EXTRA_CLASSPATH="/opt/spark/external-jars/*"

USER 185