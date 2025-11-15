"""
Kafka Producer - Fraud Detection Real-time Simulation
Đọc dữ liệu từ HDFS và gửi 5 giao dịch/giây vào Kafka
"""

import json
import time
import logging
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
KAFKA_TOPIC = 'fraud_transactions'
HDFS_INPUT_PATH = 'hdfs://namenode:9000/data/input/paysim_realtime.csv'
TRANSACTIONS_PER_SECOND = 1
BATCH_INTERVAL = 1.0  # giây


class FraudDataProducer:
    """Producer để giả lập dữ liệu giao dịch real-time"""
    
    def __init__(self, bootstrap_servers, topic, hdfs_path):
        self.topic = topic
        self.hdfs_path = hdfs_path
        
        # Khởi tạo Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            compression_type='gzip'
        )
        
        # Khởi tạo Spark Session
        self.spark = SparkSession.builder \
            .appName("FraudDataProducer") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Kafka Producer initialized. Topic: {topic}")
        logger.info(f"HDFS Path: {hdfs_path}")
    
    def load_data(self):
        """Đọc dữ liệu từ HDFS"""
        try:
            logger.info(f"Đang đọc dữ liệu từ HDFS: {self.hdfs_path}")
            
            # Đọc CSV từ HDFS
            df = self.spark.read.csv(
                self.hdfs_path,
                header=True,
                inferSchema=True
            )
            
            total_count = df.count()
            logger.info(f"Đã đọc {total_count:,} giao dịch từ HDFS")
            
            # Chuyển sang Pandas để xử lý tuần tự
            # Nếu file quá lớn, có thể lấy mẫu
            if total_count > 100000:
                logger.warning(f"File có {total_count:,} records, sẽ lấy 100,000 records đầu")
                df = df.limit(100000)
            
            pdf = df.toPandas()
            logger.info(f"Đã chuyển {len(pdf):,} giao dịch sang Pandas")
            
            return pdf
            
        except Exception as e:
            logger.error(f"Lỗi khi đọc dữ liệu từ HDFS: {str(e)}")
            sys.exit(1)
    
    def send_transaction(self, transaction, index):
        """Gửi một giao dịch đến Kafka"""
        try:
            # Chuyển đổi transaction thành dictionary
            tx_dict = transaction.to_dict()
            
            # Xử lý NaN values
            for key, value in tx_dict.items():
                if value != value:  # Check NaN
                    tx_dict[key] = None
                elif isinstance(value, (float, int)):
                    tx_dict[key] = float(value)
            
            # Thêm metadata
            message = {
                'transaction_id': index,
                'timestamp': time.time(),
                'data': tx_dict
            }
            
            # Gửi đến Kafka
            key = f"tx_{index}"
            future = self.producer.send(
                self.topic,
                key=key,
                value=message
            )
            
            # Chờ xác nhận (non-blocking)
            record_metadata = future.get(timeout=10)
            
            # Log mỗi 100 transactions
            if index % 100 == 0:
                logger.info(
                    f"Sent TX #{index} -> "
                    f"Partition: {record_metadata.partition}, "
                    f"Offset: {record_metadata.offset}"
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi gửi transaction #{index}: {str(e)}")
            return False
    
    def run(self, transactions_per_second=5):
        """Chạy producer với tốc độ cho trước"""
        logger.info("="*80)
        logger.info("FRAUD DETECTION REAL-TIME PRODUCER")
        logger.info("="*80)
        
        # Load dữ liệu
        df = self.load_data()
        total_transactions = len(df)
        
        logger.info(f"Tốc độ: {transactions_per_second} giao dịch/giây")
        logger.info(f"Tổng số giao dịch: {total_transactions:,}")
        logger.info(f"Thời gian ước tính: {total_transactions/transactions_per_second:.1f} giây")
        logger.info("="*80)
        logger.info("Bắt đầu gửi dữ liệu... (Ctrl+C để dừng)")
        logger.info("="*80)
        
        try:
            sent_count = 0
            batch_size = transactions_per_second
            batch_interval = 1.0  # 1 giây
            
            for i in range(0, total_transactions, batch_size):
                batch_start = time.time()
                
                # Lấy batch giao dịch
                batch_end = min(i + batch_size, total_transactions)
                batch = df.iloc[i:batch_end]
                
                # Gửi từng giao dịch trong batch
                for idx, row in batch.iterrows():
                    if self.send_transaction(row, idx):
                        sent_count += 1
                
                # Tính thời gian đã xử lý
                elapsed = time.time() - batch_start
                
                # Hiển thị progress
                progress = (batch_end / total_transactions) * 100
                logger.info(
                    f"Progress: {batch_end:,}/{total_transactions:,} ({progress:.1f}%) - "
                    f"Sent: {sent_count:,} - "
                    f"Batch time: {elapsed:.3f}s"
                )
                
                # Sleep để đạt tốc độ mong muốn
                sleep_time = max(0, batch_interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                # Flush producer định kỳ
                if sent_count % 100 == 0:
                    self.producer.flush()
            
            # Flush cuối cùng
            self.producer.flush()
            
            logger.info("="*80)
            logger.info(f"✅ HOÀN THÀNH!")
            logger.info(f"   Tổng giao dịch đã gửi: {sent_count:,}")
            logger.info(f"   Tổng thời gian: {time.time():.1f}s")
            logger.info("="*80)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️  Đã dừng producer bởi người dùng")
            logger.info(f"   Đã gửi: {sent_count:,} giao dịch")
        except Exception as e:
            logger.error(f"Lỗi trong quá trình gửi: {str(e)}")
        finally:
            self.close()
    
    def close(self):
        """Đóng kết nối"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Đã đóng Kafka producer")
        
        if self.spark:
            self.spark.stop()
            logger.info("Đã dừng Spark session")


def main():
    """Hàm chính"""
    producer = FraudDataProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        hdfs_path=HDFS_INPUT_PATH
    )
    
    producer.run(transactions_per_second=TRANSACTIONS_PER_SECOND)


if __name__ == "__main__":
    main()
