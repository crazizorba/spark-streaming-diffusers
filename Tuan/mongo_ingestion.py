from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session():
    """
    Tạo Spark Session với cấu hình cần thiết để kết nối Kafka và MongoDB.
    Lưu ý: Bạn cần chạy lệnh spark-submit với các packages phù hợp:
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1
    """
    return SparkSession.builder \
        .appName("SourceMetadataIngestion") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:password@localhost:27017/cpg_db.source_metadata?authSource=admin") \
        .getOrCreate()

def get_schema():
    """
    Định nghĩa Schema dựa trên source_metadata_event.schema.json do Tý cung cấp.
    """
    return StructType([
        StructField("schema_version", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("file_hash", StringType(), True),
        StructField("loc", IntegerType(), True),
        StructField("parse_status", StringType(), True),
        StructField("last_modified", StringType(), True)
    ])

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    kafka_bootstrap_servers = "localhost:9092"
    topic_name = "source_metadata_events"
    
    # Đọc dữ liệu stream từ Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
        
    # Chuyển đổi dữ liệu binary của Kafka thành chuỗi JSON
    json_df = df.selectExpr("CAST(value AS STRING) as json_value")
    
    # Parse JSON thành các cột với schema đã định nghĩa
    schema = get_schema()
    parsed_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")
    
    # Ghi dữ liệu vào MongoDB
    # Sử dụng chế độ "Append" vì Spark Structured Streaming hỗ trợ append.
    # Trong MongoDB connector, cấu hình idField hoặc upsert có thể giúp chống trùng lặp nếu cần,
    # nhưng mặc định Spark Structured Streaming kết hợp với Checkpoint đã cung cấp exactly-once processing semantics.
    checkpoint_location = "./spark_checkpoints/source_metadata"
    
    query = parsed_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", checkpoint_location) \
        .option("forceInsert", "false") \
        .outputMode("append") \
        .start()
        
    print(f"Started Structured Streaming query reading from {topic_name} to MongoDB...")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
