from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. SparkSession 설정
# S3A 및 Kafka 패키지 포함
spark = SparkSession.builder \
    .appName("CryptoMinioStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. Kafka UI에서 확인된 데이터 구조에 맞춘 스키마 정의
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_time", LongType(), True),
    StructField("trade_time_iso", StringType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("trade_id", LongType(), True)
])

# 3. Kafka 소스 읽기
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto-trades") \
    .load()

# 4. 데이터 파싱 및 시간 변환 (중요: trade_time 사용)
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("partition")
).select("data.*", "partition")

# 파티션 기반 종목 확정 및 시간 처리 로직
rich_stream = parsed_stream.withColumn(
    "final_symbol",
    when(col("partition") == 0, "BTC_USDT").otherwise("ETH_USDT")
).withColumn(
    # trade_time (ms)를 timestamp로 변환
    "event_time", (col("trade_time") / 1000).cast("timestamp")
).withColumn(
    "date", to_date(col("event_time"))
).withColumn(
    "hour", hour(col("event_time"))
)

# 5. Bronze 레이어: Raw 데이터 저장 (Parquet)
# 파티션 경로가 final_symbol/date/hour 형태로 생성됨
raw_query = rich_stream.writeStream \
    .format("parquet") \
    .option("path", "s3a://crypto-raw/ticks") \
    .option("checkpointLocation", "s3a://crypto-raw/checkpoints/ticks") \
    .partitionBy("final_symbol", "date", "hour") \
    .outputMode("append") \
    .start()

# 6. Silver 레이어: 1분 OHLCV 집계 및 저장
# 필드명을 price, quantity로 수정함
ohlcv_df = rich_stream \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("final_symbol")
    ).agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("quantity").alias("volume")
    ).select(
        "window.start", 
        "window.end", 
        col("final_symbol").alias("symbol"),
        "open", "high", "low", "close", "volume",
        to_date(col("window.start")).alias("date") # 집계 데이터용 파티션 컬럼
    )

ohlcv_query = ohlcv_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://crypto-processed/ohlcv") \
    .option("checkpointLocation", "s3a://crypto-processed/checkpoints/ohlcv") \
    .partitionBy("symbol", "date") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()