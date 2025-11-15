"""
Kafka Producer - Vietnamese Stock Market Data
Lấy dữ liệu chứng khoán từ vnstock và gửi đến Kafka
"""

import json
import time
from datetime import datetime
from kafka import KafkaProducer
from vnstock import Vnstock
import pandas as pd
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Danh sách 10 mã chứng khoán nổi bật của thị trường Việt Nam
SYMBOLS = [
    'VCB',  # Vietcombank
    'VHM',  # Vinhomes
    'VIC',  # Vingroup
    'HPG',  # Hòa Phát
    'TCB',  # Techcombank
    'ACB',  # Á Châu
    'VNM',  # Vinamilk
    'BID',  # BIDV
    'GAS',  # Gas Petrolimex
    'MSN'   # Masan Group
]

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
KAFKA_TOPIC = 'vietnam_stocks'
FETCH_INTERVAL = 15  # seconds


class StockDataProducer:
    """Producer để lấy và gửi dữ liệu chứng khoán đến Kafka"""
    
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        self.stock = Vnstock().stock(symbol='VCB', source='VCI')
        logger.info(f"Kafka Producer initialized. Topic: {topic}")
    
    def fetch_price_board(self, symbols):
        """Lấy dữ liệu bảng giá cho danh sách mã"""
        try:
            # Khởi tạo Trading object
            trading = self.stock.trading
            
            # Lấy dữ liệu bảng giá
            df = trading.price_board(symbols_list=symbols)
            
            if df is None or df.empty:
                logger.warning("Không nhận được dữ liệu từ vnstock")
                return None
            
            logger.info(f"Đã lấy được {len(df)} mã chứng khoán")
            return df
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu: {str(e)}")
            return None
    
    def process_and_send(self, df):
        """Xử lý DataFrame và gửi từng record đến Kafka"""
        if df is None or df.empty:
            return
        
        # Flatten multi-index columns nếu có
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip('_') for col in df.columns.values]
        
        timestamp = datetime.now().isoformat()
        
        for idx, row in df.iterrows():
            try:
                # Chuyển đổi row thành dictionary
                data = row.to_dict()
                
                # Xử lý các giá trị NaN, datetime và các kiểu dữ liệu
                for key, value in list(data.items()):
                    if pd.isna(value):
                        data[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        data[key] = value.isoformat()
                    elif isinstance(value, (int, float)):
                        data[key] = float(value) if not pd.isna(value) else None
                    elif value is None:
                        data[key] = None
                    else:
                        # Convert các kiểu khác thành string
                        data[key] = str(value)
                
                # Tạo message
                message = {
                    'timestamp': timestamp,
                    'data': data
                }
                
                # Lấy symbol làm key - tìm column có 'symbol' trong tên
                symbol = None
                for k in data.keys():
                    if 'symbol' in k.lower():
                        symbol = str(data[k])
                        break
                
                if not symbol:
                    symbol = 'UNKNOWN'
                
                # Gửi đến Kafka
                future = self.producer.send(
                    self.topic,
                    key=symbol,
                    value=message
                )
                
                # Chờ xác nhận
                record_metadata = future.get(timeout=10)
                logger.info(
                    f"Sent {symbol} -> Topic: {record_metadata.topic}, "
                    f"Partition: {record_metadata.partition}, "
                    f"Offset: {record_metadata.offset}"
                )
                
            except Exception as e:
                logger.error(f"Lỗi khi gửi dữ liệu cho row {idx}: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())
    
    def run(self, symbols, interval=15):
        """Chạy producer liên tục với interval cho trước"""
        logger.info(f"Bắt đầu thu thập dữ liệu cho {len(symbols)} mã chứng khoán")
        logger.info(f"Danh sách mã: {', '.join(symbols)}")
        logger.info(f"Interval: {interval}s")
        
        try:
            while True:
                start_time = time.time()
                
                # Lấy dữ liệu
                df = self.fetch_price_board(symbols)
                
                # Gửi đến Kafka
                if df is not None:
                    self.process_and_send(df)
                
                # Tính thời gian đã xử lý
                elapsed = time.time() - start_time
                sleep_time = max(0, interval - elapsed)
                
                if sleep_time > 0:
                    logger.info(f"Chờ {sleep_time:.2f}s trước khi lấy dữ liệu tiếp...")
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"Xử lý mất {elapsed:.2f}s, vượt quá interval {interval}s")
                    
        except KeyboardInterrupt:
            logger.info("Dừng producer...")
        finally:
            self.close()
    
    def close(self):
        """Đóng kết nối Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Đã đóng Kafka producer")


def main():
    """Hàm chính để chạy producer"""
    logger.info("="*60)
    logger.info("VIETNAM STOCK MARKET DATA PRODUCER")
    logger.info("="*60)
    
    # Khởi tạo producer
    producer = StockDataProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC
    )
    
    # Chạy producer
    producer.run(symbols=SYMBOLS, interval=FETCH_INTERVAL)


if __name__ == "__main__":
    main()
