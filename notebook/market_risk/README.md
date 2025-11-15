# Vietnam Stock Market Data Pipeline

Pipeline thu thập và xử lý dữ liệu chứng khoán Việt Nam real-time sử dụng Kafka và Spark.

## Tổng quan

Hệ thống thu thập dữ liệu từ 10 mã chứng khoán nổi bật của thị trường Việt Nam:
- **VCB** - Vietcombank
- **VHM** - Vinhomes
- **VIC** - Vingroup
- **HPG** - Hòa Phát
- **TCB** - Techcombank
- **ACB** - Á Châu
- **VNM** - Vinamilk
- **BID** - BIDV
- **GAS** - Gas Petrolimex
- **MSN** - Masan Group

## Kiến trúc

```
vnstock (API) --> Kafka Producer --> Kafka --> Spark Streaming --> Analytics
                  (get_finance_data.py)      (spark_consumer.py)
```

## Cài đặt

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 2. Khởi động Docker containers

```bash
docker-compose up -d
```

Đợi tất cả services khởi động (Kafka, Zookeeper, Spark, v.v.)

## Sử dụng

### Chạy Kafka Producer (Thu thập dữ liệu)

Producer sẽ lấy dữ liệu mỗi 15 giây và gửi đến Kafka topic `vietnam_stocks`:

```bash
cd /home/jovyan/work/market_risk
python get_finance_data.py
```

**Output mẫu:**
```
2025-11-15 10:00:00 - INFO - VIETNAM STOCK MARKET DATA PRODUCER
2025-11-15 10:00:00 - INFO - Kafka Producer initialized. Topic: vietnam_stocks
2025-11-15 10:00:00 - INFO - Bắt đầu thu thập dữ liệu cho 10 mã chứng khoán
2025-11-15 10:00:01 - INFO - Đã lấy được 10 mã chứng khoán
2025-11-15 10:00:01 - INFO - Sent VCB -> Topic: vietnam_stocks, Partition: 0, Offset: 1234
2025-11-15 10:00:01 - INFO - Sent VHM -> Topic: vietnam_stocks, Partition: 1, Offset: 567
...
```

### Chạy Spark Consumer (Xử lý dữ liệu)

Consumer sẽ nhận dữ liệu từ Kafka và xử lý trên Spark:

```bash
cd /home/jovyan/work/market_risk
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  spark_consumer.py
```

Hoặc chạy trực tiếp với Python (nếu đã cấu hình Spark):

```bash
python spark_consumer.py
```

## Dữ liệu được thu thập

### Dữ liệu gốc từ vnstock

Mỗi record chứa 36 trường dữ liệu:

**Listing Info:**
- symbol, ceiling, floor, ref_price
- stock_type, exchange, listed_share
- organ_name (tên công ty)

**Match Info:**
- match_price, match_vol
- accumulated_volume, accumulated_value
- avg_match_price
- highest, lowest
- foreign_buy_volume, foreign_sell_volume

**Bid/Ask:**
- bid_1_price, bid_1_volume
- bid_2_price, bid_2_volume
- bid_3_price, bid_3_volume
- ask_1_price, ask_1_volume
- ask_2_price, ask_2_volume
- ask_3_price, ask_3_volume

### Dữ liệu được xử lý

Spark consumer tính toán thêm các chỉ số:

- **price_change**: Thay đổi giá (VND)
- **price_change_percent**: % thay đổi giá
- **foreign_net**: Dòng tiền nước ngoài ròng
- **spread**: Chênh lệch bid-ask
- **avg_price**: Giá trung bình
- **price_position**: Vị trí giá (0-100%) so với floor-ceiling

### Aggregation theo thời gian

Dữ liệu được tổng hợp theo window 1 phút:

- Số lượng records
- Giá trung bình, max, min
- Tổng khối lượng
- % thay đổi giá trung bình
- Dòng tiền nước ngoài ròng

## Cấu hình

### get_finance_data.py

```python
# Danh sách mã chứng khoán
SYMBOLS = ['VCB', 'VHM', 'VIC', 'HPG', 'TCB', 'ACB', 'VNM', 'BID', 'GAS', 'MSN']

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
KAFKA_TOPIC = 'vietnam_stocks'

# Interval lấy dữ liệu (giây)
FETCH_INTERVAL = 15
```

### spark_consumer.py

```python
# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "vietnam_stocks"

# Checkpoint directory
CHECKPOINT_DIR = "/tmp/spark_checkpoint"
```

## Kiểm tra Kafka

### Xem danh sách topics

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Xem dữ liệu trong topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic vietnam_stocks \
  --from-beginning
```

## Monitoring

### Kafka UI
- Zookeeper: http://localhost:2181

### Spark UI
- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081

### Jupyter Notebook
- http://localhost:8888

## Troubleshooting

### Lỗi kết nối Kafka

Kiểm tra Kafka đã chạy:
```bash
docker ps | grep kafka
```

Restart Kafka:
```bash
docker-compose restart kafka zookeeper
```

### Lỗi vnstock API

- Kiểm tra kết nối internet
- Thị trường có thể đóng cửa (chỉ mở T2-T6, 9h-15h)
- API rate limit - tăng FETCH_INTERVAL

### Lỗi Spark

Kiểm tra Spark master:
```bash
docker logs spark-master
```

Kiểm tra Spark worker:
```bash
docker logs spark-worker
```

## Mở rộng

### Thêm mã chứng khoán

Sửa `SYMBOLS` trong `get_finance_data.py`:
```python
SYMBOLS = ['VCB', 'VHM', 'VIC', 'HPG', 'TCB', 'ACB', 'VNM', 'BID', 'GAS', 'MSN', 
           'FPT', 'MWG', 'VRE', 'PLX']  # Thêm FPT, MWG, VRE, PLX
```

### Thay đổi interval

Sửa `FETCH_INTERVAL` (tính bằng giây):
```python
FETCH_INTERVAL = 30  # Lấy dữ liệu mỗi 30 giây
```

### Lưu dữ liệu vào database

Thêm output sink trong `spark_consumer.py`:
```python
# Ví dụ: Ghi vào parquet
parquet_query = processed_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/data/stocks") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

## License

MIT
