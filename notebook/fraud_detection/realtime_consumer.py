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
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark Session created successfully")
    return spark


def define_schema():
    """Define schema cho dữ liệu giao dịch"""
    # Schema cho dữ liệu gốc (fixed to match producer output)
    data_schema = StructType([
        StructField("step", DoubleType(), True),  # Fixed: was IntegerType
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", DoubleType(), True),  # Fixed: was IntegerType
        StructField("isFlaggedFraud", DoubleType(), True)  # Fixed: was IntegerType
    ])
    
    # Schema cho message từ Kafka
    message_schema = StructType([
        StructField("transaction_id", IntegerType(), True),  # Fixed: was LongType
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
        when(col("type") == "CASH_OUT", 0).otherwise(1)
    )
    
    # 2. Error Balance Origin
    df = df.withColumn(
        "errorBalanceOrig",
        col("oldbalanceOrg") - col("newbalanceOrig") - col("amount")
    )
    
    # 3. Error Balance Destination
    df = df.withColumn(
        "errorBalanceDest",
        col("oldbalanceDest") + col("amount") - col("newbalanceDest")
    )
    
    # 4. Amount over old balance
    df = df.withColumn(
        "amount_over_oldbalance",
        col("amount") / (col("oldbalanceOrg") + eps)
    )
    
    # 5. Hour
    df = df.withColumn(
        "hour",
        (col("step") % 24).cast("int")
    )
    
    # 6. Log amount
    df = df.withColumn(
        "amount_log",
        log1p(col("amount"))
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
    
    # Parse JSON - matching simple_consumer structure
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("tx_key"),
        from_json(col("value").cast("string"), schema).alias("json_data")
    ).select(
        col("tx_key"),
        col("json_data.transaction_id").alias("tx_id"),
        col("json_data.timestamp").alias("timestamp"),
        col("json_data.data.step").cast("double").alias("step"),
        col("json_data.data.type").alias("type"),
        col("json_data.data.amount").cast("double").alias("amount"),
        col("json_data.data.nameOrig").alias("nameOrig"),
        col("json_data.data.oldbalanceOrg").cast("double").alias("oldbalanceOrg"),
        col("json_data.data.newbalanceOrig").cast("double").alias("newbalanceOrig"),
        col("json_data.data.nameDest").alias("nameDest"),
        col("json_data.data.oldbalanceDest").cast("double").alias("oldbalanceDest"),
        col("json_data.data.newbalanceDest").cast("double").alias("newbalanceDest"),
        col("json_data.data.isFraud").cast("double").alias("isFraud"),
        col("json_data.data.isFlaggedFraud").cast("double").alias("isFlaggedFraud")
    )
    
    # Lọc chỉ lấy TRANSFER và CASH_OUT
    filtered_df = parsed_df.filter(
        (col("type") == "TRANSFER") | (col("type") == "CASH_OUT")
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
        .withColumn("actual_fraud", col("isFraud"))
    
    # Convert timestamp để sử dụng với watermark
    predictions_with_timestamp = predictions_final.withColumn(
        "event_time", 
        from_unixtime(col("timestamp")).cast("timestamp")
    )
    
    # Select output columns
    output_df = predictions_with_timestamp.select(
        col("tx_id"),
        col("event_time").alias("transaction_time"),
        col("type").alias("tx_type"),
        round(col("amount"), 2).alias("amount"),
        col("nameOrig").alias("from_account"),
        col("nameDest").alias("to_account"),
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
        """Embedded function to write fraud predictions to Redis"""
        import redis
        import json
        import time
        
        # Redis connection in worker process
        redis_client = redis.Redis(
            host='redis',
            port=6379,
            db=0,
            decode_responses=True
        )
        
        # Process each row in the batch
        for row in batch_df.collect():
            try:
                # Convert row to dictionary for safe access
                row_dict = row.asDict() if hasattr(row, 'asDict') else {}
                
                # Skip invalid rows
                if not row_dict.get('tx_id'):
                    pass  # Skip invalid row silently
                    continue
                
                # Prepare transaction data with safe null checks
                # Convert datetime to string if needed
                transaction_time = row_dict.get('transaction_time', '')
                if hasattr(transaction_time, 'strftime'):
                    transaction_time = transaction_time.strftime('%Y-%m-%d %H:%M:%S')
                elif transaction_time is None:
                    transaction_time = ''
                else:
                    transaction_time = str(transaction_time)
                
                tx_data = {
                    'tx_id': int(row_dict.get('tx_id', 0)) if row_dict.get('tx_id') is not None else 0,
                    'transaction_time': transaction_time,
                    'tx_type': row_dict.get('tx_type', ''),
                    'amount': float(row_dict.get('amount', 0)) if row_dict.get('amount') is not None else 0.0,
                    'from_account': row_dict.get('from_account', ''),
                    'to_account': row_dict.get('to_account', ''),
                    'actual_fraud': int(row_dict.get('actual_fraud', 0)) if row_dict.get('actual_fraud') is not None else 0,
                    'predicted_fraud': int(row_dict.get('predicted_fraud', 0)) if row_dict.get('predicted_fraud') is not None else 0,
                    'fraud_prob_pct': float(row_dict.get('fraud_prob_pct', 0)) if row_dict.get('fraud_prob_pct') is not None else 0.0,
                    'timestamp': int(time.time() * 1000)
                }
                
                # Save transaction with timestamped key
                ts = int(time.time() * 1000)
                tx_key = f"fraud:transaction:{tx_data['tx_id']}:{ts}"
                redis_client.setex(tx_key, 86400, json.dumps(tx_data))  # 24h expiry
                
                # Add to recent transactions
                redis_client.zadd('fraud:transactions:recent', {tx_key: ts})
                redis_client.zremrangebyrank('fraud:transactions:recent', 0, -201)  # Keep 200
                
                # Increment total transaction counter for all transactions
                redis_client.incr('fraud:counter:total')
                redis_client.incr(f'fraud:counter:type:{tx_data["tx_type"]}')
                
                # If fraud detected, create alert and increment fraud counter
                if tx_data['predicted_fraud'] == 1:
                    alert_key = f"fraud:alert:{tx_data['tx_id']}:{ts}"
                    alert = {
                        'tx_id': tx_data['tx_id'],
                        'tx_type': tx_data['tx_type'],
                        'amount': tx_data['amount'],
                        'from_account': tx_data['from_account'],
                        'to_account': tx_data['to_account'],
                        'fraud_prob_pct': tx_data['fraud_prob_pct'],
                        'transaction_time': tx_data['transaction_time'],
                        'timestamp': tx_data['timestamp']
                    }
                    redis_client.setex(alert_key, 86400, json.dumps(alert))
                    redis_client.zadd('fraud:alerts:recent', {alert_key: ts})
                    redis_client.zremrangebyrank('fraud:alerts:recent', 0, -101)  # Keep 100
                    
                    # Increment fraud-specific counters
                    redis_client.incr('fraud:counter:total_fraud')
                    redis_client.incr(f'fraud:counter:fraud_type:{tx_data["tx_type"]}')
                
                print(f"✓ Redis TX: {tx_data['tx_id']} | {tx_data['tx_type']} | ${tx_data['amount']:.2f} | Fraud: {tx_data['predicted_fraud']} ({tx_data['fraud_prob_pct']:.1f}%)")
                
            except Exception as e:
                import traceback
                print(f"Redis Write Error: {str(e)}")
                print(f"Full traceback: {traceback.format_exc()}")
                continue
        
        print(f"📦 Batch {batch_id}: Processed {len(batch_df.collect())} transactions to Redis")
        logger.info(f"Batch {batch_id}: Processed fraud predictions to Redis")
    
    # Stream to Redis
    redis_query = output_df \
        .writeStream \
        .foreachBatch(write_fraud_to_redis) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("✅ Fraud predictions Redis stream started")
    
    # Output 3: Performance metrics - Write to Redis
    def write_fraud_stats_to_redis(batch_df, batch_id):
        """Embedded function to write fraud statistics to Redis"""
        import redis
        import json
        import time
        
        # Redis connection in worker process
        redis_client = redis.Redis(
            host='redis',
            port=6379,
            db=0,
            decode_responses=True
        )
        
        # Process each row in the batch
        for row in batch_df.collect():
            try:
                # Convert row to dictionary for safe access
                row_dict = row.asDict() if hasattr(row, 'asDict') else {}
                
                # Convert datetime fields to strings if needed
                window_start = row_dict.get('window_start', '')
                if hasattr(window_start, 'strftime'):
                    window_start = window_start.strftime('%Y-%m-%d %H:%M:%S')
                elif window_start is None:
                    window_start = ''
                else:
                    window_start = str(window_start)
                    
                window_end = row_dict.get('window_end', '')
                if hasattr(window_end, 'strftime'):
                    window_end = window_end.strftime('%Y-%m-%d %H:%M:%S')
                elif window_end is None:
                    window_end = ''
                else:
                    window_end = str(window_end)
                
                # Prepare statistics data with safe null checks
                stats = {
                    'window_start': window_start,
                    'window_end': window_end,
                    'total_transactions': int(row_dict.get('total_transactions', 0)) if row_dict.get('total_transactions') is not None else 0,
                    'actual_frauds': int(row_dict.get('actual_frauds', 0)) if row_dict.get('actual_frauds') is not None else 0,
                    'predicted_frauds': int(row_dict.get('predicted_frauds', 0)) if row_dict.get('predicted_frauds') is not None else 0,
                    'accuracy_pct': float(row_dict.get('accuracy_pct', 0)) if row_dict.get('accuracy_pct') is not None else 0.0,
                    'true_positives': int(row_dict.get('true_positives', 0)) if row_dict.get('true_positives') is not None else 0,
                    'false_positives': int(row_dict.get('false_positives', 0)) if row_dict.get('false_positives') is not None else 0,
                    'false_negatives': int(row_dict.get('false_negatives', 0)) if row_dict.get('false_negatives') is not None else 0,
                    'avg_fraud_prob_pct': float(row_dict.get('avg_fraud_prob_pct', 0)) if row_dict.get('avg_fraud_prob_pct') is not None else 0.0,
                    'timestamp': int(time.time() * 1000)
                }
                
                # Save overall stats
                redis_client.setex('fraud:statistics', 300, json.dumps(stats))  # 5 min expiry
                
                # Save timestamped stats
                ts = int(time.time() * 1000)
                stats_key = f"fraud:stats:{ts}"
                redis_client.setex(stats_key, 3600, json.dumps(stats))  # 1h expiry
                
                print(f"✓ Redis Stats: Total: {stats['total_transactions']}, Frauds: {stats['predicted_frauds']}, Accuracy: {stats['accuracy_pct']:.1f}%")
                
            except Exception as e:
                import traceback
                print(f"Redis Stats Error: {str(e)}")
                print(f"Full traceback: {traceback.format_exc()}")
                continue
        
        print(f"📊 Batch {batch_id}: Processed fraud statistics to Redis")
        logger.info(f"Batch {batch_id}: Processed fraud statistics to Redis")
    
    stats_redis_query = accuracy_df \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(write_fraud_stats_to_redis) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("✅ Fraud stats Redis stream started")
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
        stats_redis_query.stop()
        logger.info("All queries stopped")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
