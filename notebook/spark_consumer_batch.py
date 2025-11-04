"""
Spark Streaming Consumer - NO KAFKA CONNECTOR VERSION
Dùng kafka-python để consume, xử lý với Spark, rồi produce lại
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.sql.types import *
import pandas as pd

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tạo Spark Session"""
    logger.info("🔧 Khởi tạo Spark Session...")
    
    spark = SparkSession.builder \
        .appName("Fraud Detection - Batch Processing") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"✅ Spark Session đã khởi tạo (version: {spark.version})")
    
    return spark


def load_models():
    """Load ML model và Scaler đã train"""
    logger.info("📦 Loading ML models from HDFS...")
    
    try:
        # Load GBT model từ HDFS
        gbt_model_path = "hdfs://namenode:9000/user/jovyan/gbt_fraud_model"
        gbt_model = GBTClassificationModel.load(gbt_model_path)
        logger.info(f"✅ Đã load GBT model từ: {gbt_model_path}")
        
        # Load scaler từ HDFS
        scaler_model_path = "hdfs://namenode:9000/user/jovyan/spark_scaler_model"
        scaler_model = StandardScalerModel.load(scaler_model_path)
        logger.info(f"✅ Đã load Scaler model từ: {scaler_model_path}")
        
        return gbt_model, scaler_model
    
    except Exception as e:
        logger.error(f"❌ Lỗi khi load models: {e}")
        raise


def create_features_pandas(df_pandas):
    """Tạo features từ pandas DataFrame"""
    import numpy as np
    
    # Type encoding
    type_map = {'PAYMENT': 0, 'TRANSFER': 1, 'CASH_OUT': 2, 'DEBIT': 3, 'CASH_IN': 4}
    df_pandas['type_encoded'] = df_pandas['type'].map(type_map).fillna(0)
    
    # Other features
    df_pandas['amount_log'] = np.log1p(df_pandas['amount'])
    df_pandas['errorBalanceOrig'] = df_pandas['oldbalanceOrg'] + df_pandas['amount'] - df_pandas['newbalanceOrig']
    df_pandas['errorBalanceDest'] = df_pandas['oldbalanceDest'] + df_pandas['amount'] - df_pandas['newbalanceDest']
    df_pandas['amount_over_oldbalance'] = df_pandas['amount'] / (df_pandas['oldbalanceOrg'] + 1)
    df_pandas['hour'] = df_pandas['step'] % 24
    
    return df_pandas


def predict_batch(data_list, spark, gbt_model, scaler_model, threshold=0.5):
    """Predict batch of transactions"""
    if not data_list:
        return []
    
    # Convert to pandas
    df_pandas = pd.DataFrame(data_list)
    
    # Create features
    df_pandas = create_features_pandas(df_pandas)
    
    # Feature columns
    feature_cols = ['type_encoded', 'amount_log', 'errorBalanceOrig', 
                   'errorBalanceDest', 'amount_over_oldbalance', 'hour']
    
    # Convert to Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas)
    
    # Assemble features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    df_assembled = assembler.transform(df_spark)
    
    # Scale
    df_scaled = scaler_model.transform(df_assembled)
    
    # Predict
    predictions = gbt_model.transform(df_scaled)
    
    # Extract results
    results = predictions.select(
        "step", "type", "amount", "nameOrig", "nameDest",
        "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
        "isFraud", "probability", "prediction"
    ).toPandas()
    
    # Get fraud probability and apply threshold
    results['fraud_probability'] = results['probability'].apply(lambda x: float(x[1]))
    results['fraud_prediction'] = (results['fraud_probability'] >= threshold).astype(int)
    results['prediction_time'] = pd.Timestamp.now().isoformat()
    
    # Drop probability column (can't serialize vector)
    results = results.drop(columns=['probability', 'prediction'])
    
    return results.to_dict('records')


def main():
    """Main function"""
    # Cấu hình
    KAFKA_BROKER = "kafka:9092"
    INPUT_TOPIC = "transactions"
    OUTPUT_TOPIC = "predictions"
    BATCH_SIZE = 10  # Process 10 messages at a time
    BEST_THRESHOLD = 0.50
    
    logger.info("="*80)
    logger.info("🚀 BẮT ĐẦU FRAUD DETECTION CONSUMER (BATCH MODE)")
    logger.info("="*80)
    logger.info(f"📥 Input topic: {INPUT_TOPIC}")
    logger.info(f"📤 Output topic: {OUTPUT_TOPIC}")
    logger.info(f"🎯 Batch size: {BATCH_SIZE}")
    logger.info(f"🎯 Fraud threshold: {BEST_THRESHOLD}")
    logger.info("")
    
    # Tạo Spark Session
    spark = create_spark_session()
    
    try:
        # Load models
        gbt_model, scaler_model = load_models()
        
        # Tạo Kafka Consumer
        logger.info("🔌 Connecting to Kafka consumer...")
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='spark-fraud-detection'
        )
        logger.info("✅ Kafka consumer connected")
        
        # Tạo Kafka Producer
        logger.info("🔌 Connecting to Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("✅ Kafka producer connected")
        
        logger.info("")
        logger.info("⏳ Đang chờ messages...")
        logger.info("💡 Nhấn Ctrl+C để dừng")
        logger.info("")
        
        batch = []
        message_count = 0
        
        for message in consumer:
            try:
                data = message.value
                batch.append(data)
                message_count += 1
                
                # Process batch
                if len(batch) >= BATCH_SIZE:
                    logger.info(f"📊 Processing batch of {len(batch)} messages...")
                    
                    # Predict
                    predictions = predict_batch(batch, spark, gbt_model, scaler_model, BEST_THRESHOLD)
                    
                    # Send to output topic
                    for pred in predictions:
                        producer.send(OUTPUT_TOPIC, value=pred)
                    
                    producer.flush()
                    
                    # Log stats
                    fraud_count = sum(1 for p in predictions if p['fraud_prediction'] == 1)
                    logger.info(f"✅ Processed {len(predictions)} transactions, {fraud_count} fraud detected")
                    
                    # Log sample
                    if predictions:
                        sample = predictions[0]
                        is_fraud = "🚨 FRAUD" if sample['fraud_prediction'] == 1 else "✅ Normal"
                        logger.info(f"   Sample: ${sample['amount']:,.2f} - {is_fraud} (prob={sample['fraud_probability']:.3f})")
                    
                    # Clear batch
                    batch = []
                    
                    # Progress
                    if message_count % 100 == 0:
                        logger.info(f"📈 Total processed: {message_count} messages")
                
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}", exc_info=True)
                continue
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Nhận Ctrl+C - Dừng consumer...")
    
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}", exc_info=True)
    
    finally:
        # Process remaining batch
        if batch:
            logger.info(f"📊 Processing final batch of {len(batch)} messages...")
            try:
                predictions = predict_batch(batch, spark, gbt_model, scaler_model, BEST_THRESHOLD)
                for pred in predictions:
                    producer.send(OUTPUT_TOPIC, value=pred)
                producer.flush()
                logger.info(f"✅ Processed final batch")
            except Exception as e:
                logger.error(f"❌ Error processing final batch: {e}")
        
        # Cleanup
        try:
            consumer.close()
            producer.close()
            logger.info("🛑 Đã đóng Kafka connections")
        except:
            pass
        
        spark.stop()
        logger.info("🛑 Đã dừng Spark Session")


if __name__ == "__main__":
    main()
