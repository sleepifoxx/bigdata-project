"""
Kafka Producer - Đọc CSV từ HDFS và gửi vào Kafka (giả lập realtime)
"""

import time
import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionProducer:
    def __init__(self, kafka_broker='kafka:9092', topic='transactions'):
        """
        Khởi tạo Kafka Producer
        
        Args:
            kafka_broker: Kafka broker address
            topic: Kafka topic để gửi dữ liệu
        """
        self.topic = topic
        
        # Khởi tạo Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        
        logger.info(f"✅ Kafka Producer đã kết nối tới {kafka_broker}")
        logger.info(f"📤 Sẽ gửi dữ liệu vào topic: {topic}")
        
    def send_transaction(self, transaction_dict):
        """Gửi 1 transaction vào Kafka"""
        try:
            future = self.producer.send(self.topic, value=transaction_dict)
            # Block cho đến khi gửi thành công
            record_metadata = future.get(timeout=10)
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi khi gửi transaction: {e}")
            return False
    
    def close(self):
        """Đóng producer"""
        self.producer.flush()
        self.producer.close()
        logger.info("🛑 Đã đóng Kafka Producer")


def read_csv_from_hdfs(hdfs_path, spark):
    """
    Đọc CSV từ HDFS
    
    Args:
        hdfs_path: Đường dẫn file CSV trên HDFS
        spark: SparkSession
    
    Returns:
        DataFrame
    """
    logger.info(f"📂 Đọc dữ liệu từ HDFS: {hdfs_path}")
    
    df = spark.read.csv(
        hdfs_path,
        header=True,
        inferSchema=True
    )
    
    total_records = df.count()
    logger.info(f"✅ Đã đọc {total_records:,} records từ HDFS")
    
    return df


def simulate_realtime_streaming(df_spark, producer, delay_seconds=1):
    """
    Giả lập streaming realtime bằng cách gửi từng record vào Kafka
    
    Args:
        df_spark: Spark DataFrame
        producer: TransactionProducer instance
        delay_seconds: Thời gian chờ giữa các lần gửi (giây)
    """
    logger.info("="*80)
    logger.info("🚀 BẮT ĐẦU STREAMING REALTIME")
    logger.info("="*80)
    logger.info(f"⏱️  Delay giữa các transactions: {delay_seconds} giây")
    logger.info("")
    
    # Chuyển sang Pandas để dễ iterate
    df_pandas = df_spark.toPandas()
    total = len(df_pandas)
    
    success_count = 0
    fail_count = 0
    
    try:
        for idx, row in df_pandas.iterrows():
            # Chuyển row sang dictionary
            transaction = row.to_dict()
            
            # Convert numpy types sang Python native types
            transaction = {k: (int(v) if hasattr(v, 'item') and 'int' in str(type(v)) 
                              else float(v) if hasattr(v, 'item') and 'float' in str(type(v))
                              else v) 
                          for k, v in transaction.items()}
            
            # Gửi vào Kafka
            if producer.send_transaction(transaction):
                success_count += 1
                
                # Log mỗi 100 records
                if (idx + 1) % 100 == 0:
                    logger.info(f"📊 Progress: {idx+1}/{total} ({(idx+1)/total*100:.1f}%) - "
                              f"Success: {success_count}, Failed: {fail_count}")
                
                # Log chi tiết cho một số record đầu
                if idx < 5:
                    is_fraud = "🚨 FRAUD" if transaction.get('isFraud', 0) == 1 else "✅ Normal"
                    logger.info(f"   └─> Transaction #{idx+1}: Amount=${transaction.get('amount', 0):,.2f} - {is_fraud}")
            else:
                fail_count += 1
            
            # Delay để giả lập realtime
            time.sleep(delay_seconds)
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Nhận Ctrl+C - Dừng streaming...")
    
    logger.info("")
    logger.info("="*80)
    logger.info("✅ KẾT THÚC STREAMING")
    logger.info("="*80)
    logger.info(f"📊 Tổng số records đã gửi: {success_count}/{total}")
    logger.info(f"❌ Số records thất bại: {fail_count}")
    logger.info("")


def main():
    """Main function"""
    # Cấu hình
    HDFS_PATH = "hdfs://namenode:9000/data/input/paysim_realtime.csv"
    KAFKA_BROKER = "kafka:9092"
    KAFKA_TOPIC = "transactions"
    DELAY_SECONDS = 0.1  # Delay giữa các transactions (giây)
    
    # Khởi tạo Spark Session
    logger.info("🔧 Khởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("Kafka Producer - Transaction Streaming") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"✅ Spark Session đã khởi tạo (version: {spark.version})")
    
    try:
        # Đọc dữ liệu từ HDFS
        df = read_csv_from_hdfs(HDFS_PATH, spark)
        
        # Hiển thị schema
        logger.info("\n📋 Schema của dữ liệu:")
        df.printSchema()
        
        # Hiển thị mẫu
        logger.info("\n📊 Mẫu dữ liệu:")
        df.show(5, truncate=False)
        
        # Khởi tạo Kafka Producer
        producer = TransactionProducer(
            kafka_broker=KAFKA_BROKER,
            topic=KAFKA_TOPIC
        )
        
        # Bắt đầu streaming
        simulate_realtime_streaming(df, producer, delay_seconds=DELAY_SECONDS)
        
        # Đóng producer
        producer.close()
        
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}", exc_info=True)
    
    finally:
        # Dừng Spark
        spark.stop()
        logger.info("🛑 Đã dừng Spark Session")


if __name__ == "__main__":
    main()
