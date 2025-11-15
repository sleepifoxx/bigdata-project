"""
Spark Streaming Consumer - Fraud Detection Real-time
Nhận giao dịch từ Kafka, xử lý và dự đoán gian lận bằng ML model
Lưu kết quả vào Redis cho Dashboard
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import StandardScalerModel
import logging
import sys

# Add parent directory to path
sys.path.append('/home/jovyan/work')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "fraud_transactions"
CHECKPOINT_DIR = "/tmp/fraud_checkpoint"
MODEL_PATH = "gbt_fraud_model"
SCALER_PATH = "spark_scaler_model"
FRAUD_THRESHOLD = 0.45  # Ngưỡng tối ưu từ training


def extract_probability(probability_vector):
    """Extract probability cho class 1 (fraud) từ probability vector"""
    if probability_vector is not None:
        try:
            # Với ML models trong Spark, probability vector có thể là DenseVector hoặc SparseVector
            # Vector có 2 elements: [prob_class_0, prob_class_1]
            return float(probability_vector.toArray()[1])  # Index 1 = fraud probability
        except:
            return 0.0
    return 0.0


def create_spark_session():
    """Tạo Spark Session với Kafka package"""
    spark = SparkSession.builder \
        .appName("FraudDetectionRealtime") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session created successfully")
    return spark


def define_schema():
    """Define schema cho dữ liệu giao dịch"""
    # Schema cho dữ liệu gốc (có thể điều chỉnh theo CSV)
    data_schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True)
    ])
    
    # Schema cho message từ Kafka
    message_schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("data", data_schema, True)
    ])
    
    return message_schema


def preprocess_features(df):
    """Tạo features giống như trong preprocessing"""
    
    eps = 1e-9
    
    # 1. Mã hóa type
    df = df.withColumn(
        "type_encoded",
        when(col("data.type") == "CASH_OUT", 0).otherwise(1)
    )
    
    # 2. Error Balance Origin
    df = df.withColumn(
        "errorBalanceOrig",
        col("data.oldbalanceOrg") - col("data.newbalanceOrig") - col("data.amount")
    )
    
    # 3. Error Balance Destination
    df = df.withColumn(
        "errorBalanceDest",
        col("data.oldbalanceDest") + col("data.amount") - col("data.newbalanceDest")
    )
    
    # 4. Amount over old balance
    df = df.withColumn(
        "amount_over_oldbalance",
        col("data.amount") / (col("data.oldbalanceOrg") + eps)
    )
    
    # 5. Hour
    df = df.withColumn(
        "hour",
        (col("data.step") % 24).cast("int")
    )
    
    # 6. Log amount
    df = df.withColumn(
        "amount_log",
        log1p(col("data.amount"))
    )
    
    return df


def load_models(spark):
    """Load trained model và scaler"""
    try:
        logger.info(f"Loading GBT model from: {MODEL_PATH}")
        model = GBTClassificationModel.load(MODEL_PATH)
        logger.info("✅ GBT model loaded successfully")
        
        logger.info(f"Loading scaler from: {SCALER_PATH}")
        scaler = StandardScalerModel.load(SCALER_PATH)
        logger.info("✅ Scaler loaded successfully")
        
        return model, scaler
        
    except Exception as e:
        logger.error(f"Error loading models: {str(e)}")
        sys.exit(1)


def main():
    """Hàm chính"""
    logger.info("="*80)
    logger.info("FRAUD DETECTION REAL-TIME CONSUMER")
    logger.info("="*80)
    
    # Tạo Spark session
    spark = create_spark_session()
    
    # Register UDF cho extract probability
    from pyspark.sql.functions import udf, col, when, from_unixtime, window, count, sum, avg, round
    from pyspark.sql.types import DoubleType
    extract_prob_udf = udf(extract_probability, DoubleType())
    
    # Load models
    model, scaler = load_models(spark)
    
    # Define schema
    schema = define_schema()
    
    # Đọc stream từ Kafka
    logger.info(f"Connecting to Kafka topic: {KAFKA_TOPIC}")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("✅ Connected to Kafka")
    
    # Parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("json_data")
    ).select(
        col("json_data.transaction_id").alias("tx_id"),
        col("json_data.timestamp").alias("timestamp"),
        col("json_data.data").alias("data")
    )
    
    # Lọc chỉ lấy TRANSFER và CASH_OUT
    filtered_df = parsed_df.filter(
        (col("data.type") == "TRANSFER") | (col("data.type") == "CASH_OUT")
    )
    
    # Tạo features
    features_df = preprocess_features(filtered_df)
    
    # Feature columns
    feature_cols = ['type_encoded', 'amount_log', 'errorBalanceOrig', 
                   'errorBalanceDest', 'amount_over_oldbalance', 'hour']
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )
    
    assembled_df = assembler.transform(features_df)
    
    # Scale features
    scaled_df = scaler.transform(assembled_df)
    
    # Predict
    predictions_df = model.transform(scaled_df)
    
    # Extract probability và apply threshold
    predictions_final = predictions_df \
        .withColumn("fraud_probability", 
                   extract_prob_udf(col("probability"))) \
        .withColumn("predicted_fraud",
                   when(col("fraud_probability") >= FRAUD_THRESHOLD, 1).otherwise(0)) \
        .withColumn("actual_fraud", col("data.isFraud"))
    
    # Convert timestamp để sử dụng với watermark
    predictions_with_timestamp = predictions_final.withColumn(
        "event_time", 
        from_unixtime(col("timestamp")).cast("timestamp")
    )
    
    # Select output columns
    output_df = predictions_with_timestamp.select(
        col("tx_id"),
        col("event_time").alias("transaction_time"),
        col("data.type").alias("tx_type"),
        round(col("data.amount"), 2).alias("amount"),
        col("data.nameOrig").alias("from_account"),
        col("data.nameDest").alias("to_account"),
        col("actual_fraud"),
        col("predicted_fraud"),
        round(col("fraud_probability") * 100, 2).alias("fraud_prob_pct")
    )
    
    # Tính accuracy metrics
    accuracy_df = predictions_with_timestamp \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(window(col("event_time"), "30 seconds")) \
        .agg(
            count("*").alias("total_transactions"),
            sum(when(col("actual_fraud") == 1, 1).otherwise(0)).alias("actual_frauds"),
            sum(when(col("predicted_fraud") == 1, 1).otherwise(0)).alias("predicted_frauds"),
            sum(when((col("actual_fraud") == col("predicted_fraud")), 1).otherwise(0)).alias("correct_predictions"),
            sum(when((col("actual_fraud") == 1) & (col("predicted_fraud") == 1), 1).otherwise(0)).alias("true_positives"),
            sum(when((col("actual_fraud") == 0) & (col("predicted_fraud") == 1), 1).otherwise(0)).alias("false_positives"),
            sum(when((col("actual_fraud") == 1) & (col("predicted_fraud") == 0), 1).otherwise(0)).alias("false_negatives"),
            avg("fraud_probability").alias("avg_fraud_prob")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_transactions"),
            col("actual_frauds"),
            col("predicted_frauds"),
            round((col("correct_predictions") / col("total_transactions")) * 100, 2).alias("accuracy_pct"),
            col("true_positives"),
            col("false_positives"),
            col("false_negatives"),
            round(col("avg_fraud_prob") * 100, 2).alias("avg_fraud_prob_pct")
        )
    
    # Output 1: All transactions with predictions - Write to Redis
    def write_fraud_to_redis(batch_df, batch_id):
        """Write fraud predictions to Redis"""
        try:
            sys.path.insert(0, '/home/jovyan/work/redis')
            from redis_sink import write_to_redis_fraud
            batch_df.foreachPartition(write_to_redis_fraud)
            logger.info(f"Batch {batch_id}: Written {batch_df.count()} fraud predictions to Redis")
        except Exception as e:
            logger.error(f"Error writing fraud to Redis: {e}")
    
    # Stream to Redis
    redis_query = output_df \
        .writeStream \
        .foreachBatch(write_fraud_to_redis) \
        .trigger(processingTime="5 seconds") \
        .start()
    
    logger.info("✅ Fraud predictions Redis stream started")
    
    # Console output (for monitoring)
    query1 = output_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .trigger(processingTime="5 seconds") \
        .start()
    
    logger.info("✅ Transaction predictions console stream started")
    
    # Output 2: Fraud alerts only
    fraud_alerts = output_df.filter(col("predicted_fraud") == 1)
    
    query2 = fraud_alerts \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName("FRAUD_ALERTS") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    logger.info("✅ Fraud alerts stream started")
    
    # Output 3: Performance metrics - Write to Redis
    def write_fraud_stats_to_redis(batch_df, batch_id):
        """Write fraud statistics to Redis"""
        try:
            sys.path.insert(0, '/home/jovyan/work/redis')
            from redis_sink import write_fraud_statistics
            batch_df.foreachPartition(write_fraud_statistics)
        except Exception as e:
            logger.error(f"Error writing fraud stats to Redis: {e}")
    
    stats_redis_query = accuracy_df \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(write_fraud_stats_to_redis) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ Fraud stats Redis stream started")
    
    # Console output for metrics
    query3 = accuracy_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .queryName("PERFORMANCE_METRICS") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ Performance metrics console stream started")
    logger.info("="*80)
    logger.info(f"Fraud threshold: {FRAUD_THRESHOLD}")
    logger.info("Streaming queries are running. Press Ctrl+C to stop.")
    logger.info("="*80)
    
    # Wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Stopping streaming queries...")
        redis_query.stop()
        query1.stop()
        query2.stop()
        stats_redis_query.stop()
        query3.stop()
        logger.info("All queries stopped")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
