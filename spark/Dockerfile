# spark/Dockerfile

# ✅ M1 arm64 호환: eclipse-temurin은 공식 multi-arch 지원
# openjdk 대신 temurin을 쓰는 이유: arm64 빌드가 안정적
FROM --platform=linux/arm64 eclipse-temurin:17-jdk-jammy

# 변수 설명:
# SPARK_VERSION: PySpark와 반드시 같은 버전이어야 함
# HADOOP_VERSION: S3(MinIO) 연동 시 필요한 hadoop-aws 버전과 맞춰야 함
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# Python 3.11 + 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Spark 다운로드 및 설치
# ⚠️ M1 경고: Spark 바이너리는 JVM 위에서 돌아 arm64에서도 동작하나
#   내부 native 라이브러리(snappy 등)가 arm64 미지원인 경우가 있음
#   → snappy 압축 대신 이 프로젝트는 gzip/uncompressed 사용
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# spark-sql-kafka 패키지 사전 다운로드
# 변수 설명:
# spark-sql-kafka-0-10: Kafka 0.10+ 프로토콜 사용 (현재 Kafka 표준)
# 2.12: Scala 버전 (Spark 3.5.x는 Scala 2.12 기반)
# 3.5.1: Spark 버전과 반드시 일치해야 함 ← 가장 흔한 실수!
RUN ${SPARK_HOME}/bin/spark-shell \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION} \
    --conf spark.jars.ivy=/opt/ivy \
    -e "System.exit(0)" 2>/dev/null || true

COPY spark/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

WORKDIR /app
COPY . /app

CMD ["python3", "ohlcv_streaming.py"]