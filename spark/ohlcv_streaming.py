# spark/ohlcv_streaming.py

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 1. 환경 변수 (docker-compose에서 주입)
# ──────────────────────────────────────────────
# 변수명 설명:
# KAFKA_BOOTSTRAP_SERVERS: Kafka 브로커 주소 (host:port)
#   docker 내부 통신이므로 서비스명:포트 사용 (localhost X)
# KAFKA_TOPIC: 구독할 토픽명. 쉼표로 여러 개 가능 "btcusdt-trades,ethusdt-trades"
# CHECKPOINT_DIR: 장애 복구를 위한 체크포인트 저장 경로
#   컨테이너 재시작 시에도 유지되려면 볼륨 마운트 필요

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "btcusdt-trades,ethusdt-trades")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")


# ──────────────────────────────────────────────
# 2. SparkSession 생성
# ──────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    """
    SparkSession: Spark 애플리케이션의 진입점. 딱 하나만 존재해야 함.

    설정 항목 설명:
    - spark.jars.packages: 런타임에 Maven에서 JAR 자동 다운로드
      → spark-sql-kafka: Kafka 소스/싱크 연결 지원
      → spark-token-provider: Kafka 인증 토큰 관련 (Kafka 3.x 필수)

    - spark.sql.shuffle.partitions: 집계(groupBy) 후 파티션 수
      기본값 200 → 로컬 환경에서 200개 태스크 생성 = 오버헤드 발생
      로컬 테스트에서는 2~4로 줄이는 것이 중요 (속도 10배 차이)

    - spark.executor.memory / driver.memory:
      executor: 실제 데이터 처리 담당 (연산 메모리)
      driver:   쿼리 계획, 결과 수집, 체크포인트 관리 담당
      합계 약 2GB → 16GB 중 2GB 사용

    - spark.sql.streaming.checkpointLocation: 글로벌 기본 체크포인트 경로

    - spark.network.timeout: 네트워크 타임아웃
      로컬 환경에서 GC pause 등으로 느릴 수 있어 여유 있게 설정
    """
    spark = (
        SparkSession.builder
        .appName("CryptoOHLCVStreaming")
        .master("local[2]")  # local[2]: 로컬 모드, 코어 2개 사용
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        # ⚠️ M1 arm64: Kryo 직렬화기 사용 (기본 Java 직렬화보다 빠름)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # INFO 로그가 너무 많아서 WARN으로 제한
    return spark


# ──────────────────────────────────────────────
# 3. Kafka 메시지 스키마 정의
# ──────────────────────────────────────────────
def get_trade_schema() -> StructType:
    """
    StructType: Spark DataFrame의 스키마 (테이블 구조 정의)

    Binance WebSocket trade 메시지 구조:
    {
        "symbol": "BTCUSDT",      ← 종목명
        "price": "43521.50",      ← 체결가
        "quantity": "0.00123",    ← 체결량
        "trade_time": 1704067200000,  ← 체결 시각 (Unix milliseconds)
        "is_buyer_maker": false   ← 매수자가 메이커인지 (시장 방향성)
    }

    ⚠️ 주의: Binance는 price/quantity를 문자열로 보냄
    → DoubleType으로 캐스팅 필요 (스키마에서 바로 적용)
    """
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True),  # Unix ms
        StructField("is_buyer_maker", StringType(), True),
    ])


# ──────────────────────────────────────────────
# 4. Kafka 스트림 읽기
# ──────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession):
    """
    Kafka 소스 DataFrame 생성.

    Kafka에서 읽으면 기본으로 오는 컬럼들:
    ┌─────────────────┬────────────────────────────────────────┐
    │ 컬럼명          │ 설명                                    │
    ├─────────────────┼────────────────────────────────────────┤
    │ key             │ 메시지 키 (binary) - 우리는 symbol 사용 │
    │ value           │ 메시지 본문 (binary) ← 우리가 쓸 것    │
    │ topic           │ 토픽명                                  │
    │ partition       │ 파티션 번호 (0~N)                       │
    │ offset          │ 파티션 내 메시지 순번                   │
    │ timestamp       │ Kafka 수신 시각 (처리 타임)             │
    │ timestampType   │ 0=CreateTime, 1=LogAppendTime           │
    └─────────────────┴────────────────────────────────────────┘

    옵션 설명:
    - subscribe: 구독할 토픽 (쉼표로 여러 개 가능)
    - startingOffsets: "latest"=지금부터, "earliest"=처음부터
      → 개발 중에는 "earliest"로 이미 쌓인 데이터부터 처리
    - failOnDataLoss: 오프셋 갭 발생 시 실패 여부
      → false: 경고만 출력하고 계속 진행 (개발 환경에 적합)
    - maxOffsetsPerTrigger: 한 번의 micro-batch에서 읽을 최대 메시지 수
      → 처음 실행 시 밀린 메시지가 많을 경우 메모리 폭발 방지
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


# ──────────────────────────────────────────────
# 5. JSON 파싱 및 이벤트 타임 변환
# ──────────────────────────────────────────────
def parse_trades(raw_stream):
    """
    Kafka value(binary) → 우리가 원하는 컬럼들로 변환하는 과정.

    변환 흐름:
    binary value
      → CAST to STRING (UTF-8 디코딩)
      → from_json으로 JSON 파싱
      → 개별 컬럼으로 펼치기 (*)
      → trade_time(ms) → TimestampType 변환
      → 워터마크 설정

    F.from_json(col, schema):
      JSON 문자열을 스키마에 맞는 StructType으로 파싱
      파싱 실패 시 null 반환 (예외 발생 X) → 데이터 품질 주의

    F.col("data.*"):
      StructType을 펼쳐서 개별 컬럼으로 만들기
      data.symbol, data.price ... → symbol, price ...

    (F.col("trade_time") / 1000).cast(TimestampType()):
      trade_time은 Unix milliseconds → seconds로 변환 후 TimestampType
      이 컬럼이 윈도우 집계의 이벤트 타임 기준이 됨
    """
    schema = get_trade_schema()

    return (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")  # binary → string
        .select(
            F.from_json(F.col("json_str"), schema).alias("data")
        )
        .select("data.*")  # StructType 컬럼 펼치기
        .withColumn(
            "event_time",
            (F.col("trade_time") / 1000).cast(TimestampType())
            # trade_time: 1704067200000 (ms) → 1704067200.0 (s) → Timestamp
        )
        .filter(F.col("price").isNotNull())  # 파싱 실패 행 제거
        .filter(F.col("symbol").isNotNull())
        # ────────────────────────────────
        # 워터마크 설정
        # "event_time" 컬럼 기준으로 10초 늦은 데이터까지 허용
        # 10초 이상 늦은 데이터는 드롭 + 해당 윈도우 상태 메모리에서 해제
        # ────────────────────────────────
        .withWatermark("event_time", "10 seconds")
    )


# ──────────────────────────────────────────────
# 6. 1분 텀블링 윈도우 OHLCV 집계
# ──────────────────────────────────────────────
def compute_ohlcv(parsed_stream):
    """
    OHLCV: Open, High, Low, Close, Volume — 캔들스틱 차트의 기본 단위

    집계 로직:
    - groupBy: (1분 윈도우, 종목명) 기준으로 그룹핑
    - window(event_time, "1 minute"): 텀블링 윈도우
        window.start: 윈도우 시작 시각 (예: 10:00:00)
        window.end:   윈도우 종료 시각 (예: 10:01:00)

    집계 함수:
    - first(price, ignorenulls=True): 윈도우 내 첫 번째 price → Open
      ⚠️ first()는 도착 순서 기반이라 완벽한 Open은 아님
         더 정확하게 하려면 trade_time 기준 min을 구해야 하지만
         지금은 학습 목적으로 first() 사용
    - max(price): High
    - min(price): Low
    - last(price, ignorenulls=True): Close
    - sum(quantity): Volume (총 체결량)
    - count(): 해당 윈도우의 체결 건수

    출력 컬럼:
    window_start, window_end, symbol, open, high, low, close, volume, trade_count
    """
    return (
        parsed_stream
        .groupBy(
            F.window(F.col("event_time"), "1 minute"),  # 텀블링 윈도우
            F.col("symbol")
        )
        .agg(
            F.first("price", ignorenulls=True).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price", ignorenulls=True).alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.round(F.col("volume"), 6).alias("volume"),
            F.col("trade_count")
        )
    )


# ──────────────────────────────────────────────
# 7. 콘솔 싱크로 출력
# ──────────────────────────────────────────────
def start_console_sink(ohlcv_stream):
    """
    싱크(Sink): Spark Structured Streaming이 결과를 쓰는 대상

    싱크 종류:
    ┌──────────────┬──────────────────────────────────────────────┐
    │ 싱크          │ 용도                                          │
    ├──────────────┼──────────────────────────────────────────────┤
    │ console      │ 터미널 출력 — 개발/디버깅용                    │
    │ memory       │ 메모리 임시 테이블 — 테스트용                  │
    │ kafka        │ Kafka 토픽으로 다시 쓰기                       │
    │ parquet/csv  │ 파일 시스템(MinIO 등) — Day 5에서 구현        │
    │ foreach      │ 커스텀 로직 (Postgres 등) — Day 6에서 구현    │
    └──────────────┴──────────────────────────────────────────────┘

    outputMode 설명:
    - "append":  윈도우가 완전히 닫힌 후 결과 출력 (워터마크 필요)
                 → 같은 윈도우 결과가 딱 한 번만 출력됨 ← 우리 선택
    - "update":  새 데이터가 들어올 때마다 변경된 윈도우 결과 출력
                 → 같은 윈도우 결과가 여러 번 출력될 수 있음
    - "complete": 매 배치마다 전체 집계 결과 출력 (상태 무한 증가 주의)

    trigger(processingTime="30 seconds"):
    - micro-batch 실행 주기 설정
    - 30초마다 Kafka에서 새 데이터를 읽어 집계 실행
    - "append" 모드 + 1분 윈도우이므로 30초 트리거가 적절

    truncate=False: 컬럼 내용이 길어도 자르지 않고 전부 출력
    """
    return (
        ohlcv_stream.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="30 seconds")
        .queryName("ohlcv_console")
        .start()
    )


# ──────────────────────────────────────────────
# 8. 메인 실행
# ──────────────────────────────────────────────
def main():
    logger.info("🚀 Starting OHLCV Streaming Job")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topics: {KAFKA_TOPIC}")
    logger.info(f"  Checkpoint: {CHECKPOINT_DIR}")

    spark = create_spark_session()

    try:
        # 파이프라인 구성 (실제 실행은 writeStream.start()에서 시작)
        raw_stream = read_kafka_stream(spark)
        parsed = parse_trades(raw_stream)
        ohlcv = compute_ohlcv(parsed)
        query = start_console_sink(ohlcv)

        logger.info("✅ Streaming query started. Waiting for data...")

        # awaitTermination(): 스트리밍 잡이 종료될 때까지 메인 스레드 블로킹
        # Ctrl+C 또는 query.stop() 호출 시 종료
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("⛔ Interrupted by user")
    finally:
        spark.stop()
        logger.info("✅ SparkSession stopped")


if __name__ == "__main__":
    main()