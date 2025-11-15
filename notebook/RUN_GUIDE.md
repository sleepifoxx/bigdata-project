# 📖 Hướng dẫn Chạy Hệ thống - Run Guide

Hướng dẫn từng bước để chạy toàn bộ hệ thống Big Data Analytics Platform.

## ⚠️ Quan trọng - Về Container

**Bạn đang ở trong container `pyspark-notebook`**

- ✅ CÓ THỂ: Chạy Python scripts, Spark jobs, truy cập services
- ❌ KHÔNG THỂ: Chạy `docker` commands, quản lý containers
- 🔗 Kết nối: Dùng service names (kafka, redis, namenode, spark-master)

**Để quản lý Docker containers:**
```bash
# Exit khỏi container
exit

# Trên host machine
docker-compose ps
docker-compose logs <service>
docker-compose restart <service>

# Vào lại container
docker exec -it pyspark-notebook bash
```

## 🎯 Mục tiêu

Chạy đầy đủ hệ thống gồm:
1. Market Risk Monitoring (chứng khoán)
2. Fraud Detection (gian lận)
3. Real-time Dashboard

## 📋 Checklist Trước khi Bắt đầu

### 1. Kiểm tra Services (từ Host Machine)

```bash
# Trên host, không phải trong container
docker-compose ps

# Cần thấy các services running:
# ✓ zookeeper
# ✓ kafka
# ✓ redis
# ✓ namenode, datanode1, datanode2
# ✓ spark-master, spark-worker
# ✓ pyspark-notebook
```

Nếu chưa start:
```bash
docker-compose up -d
```

### 2. Kiểm tra Kết nối (trong Container)

```python
# Test Kafka
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
print("✅ Kafka OK")

# Test Redis
import redis
r = redis.Redis(host='redis', port=6379, decode_responses=True)
print(r.ping())  # True
print("✅ Redis OK")

# Test Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print("✅ Spark OK")
spark.stop()
```

### 3. Cài đặt Python Packages

```bash
cd /home/jovyan/work
pip install -r requirements.txt
```

Packages cần thiết:
- kafka-python
- redis
- vnstock3
- dash
- plotly
- pandas

## 🏃 Chạy Hệ thống

### OPTION 1: Chạy Từng Module (Recommended)

Mở nhiều terminal và chạy từng phần riêng biệt.

#### Terminal 1️⃣: Market Risk Producer

```bash
cd /home/jovyan/work/market_risk
python get_finance_data.py
```

**Output:**
```
VIETNAM STOCK MARKET DATA PRODUCER
Kafka Producer initialized. Topic: vietnam_stocks
Bắt đầu thu thập dữ liệu cho 10 mã chứng khoán
Đã lấy được 10 mã chứng khoán
Sent VCB -> Topic: vietnam_stocks, Partition: 0, Offset: 123
...
```

**Chức năng:**
- Lấy dữ liệu từ vnstock API
- 10 mã: VCB, VHM, VIC, HPG, TCB, ACB, VNM, BID, GAS, MSN
- Tần suất: Mỗi 15 giây
- Gửi vào Kafka topic: `vietnam_stocks`

**Để dừng:** Ctrl+C

#### Terminal 2️⃣: Market Risk Consumer

```bash
cd /home/jovyan/work/market_risk
python spark_consumer.py
```

**Output:**
```
SPARK STREAMING CONSUMER - VIETNAM STOCK MARKET
Spark Session created successfully
Connected to Kafka topic: vietnam_stocks
Redis output stream started
Console output stream started
Streaming queries are running. Press Ctrl+C to stop.
-------------------------------------------
Batch: 0
-------------------------------------------
|symbol|company_name|price|change_pct|volume|...
```

**Chức năng:**
- Nhận dữ liệu từ Kafka topic `vietnam_stocks`
- Tính toán risk metrics (volatility, foreign flow, price position)
- Lưu vào Redis database
- Hiển thị processed data trong console
- Simplified version để tránh timestamp watermark issues

**Để dừng:** Ctrl+C (sẽ stop tất cả queries gracefully)

**⚠️ Lưu ý**: Nếu không thấy dữ liệu, đảm bảo Market Risk Producer đang chạy và gửi data.

#### Terminal 3️⃣: Fraud Detection Producer

**⚠️ Cần chuẩn bị trước:**

1. **Upload dữ liệu lên HDFS** (từ host machine):

```bash
# Tạo thư mục
docker exec namenode hdfs dfs -mkdir -p /data/input

# Upload file (giả sử có file paysim_realtime.csv)
docker exec namenode hdfs dfs -put paysim_realtime.csv /data/input/

# Kiểm tra
docker exec namenode hdfs dfs -ls /data/input/
```

2. **Train ML Model** (trong container):

```bash
cd /home/jovyan/work/fraud_detection
jupyter notebook
# Mở và chạy preprocessing.ipynb
# Mở và chạy train.ipynb
# Sẽ tạo: gbt_fraud_model/ và spark_scaler_model/
```

3. **Chạy Producer**:

```bash
cd /home/jovyan/work/fraud_detection
python realtime_producer.py
```

**Output:**
```
FRAUD DETECTION REAL-TIME PRODUCER
Đã đọc 50,000 giao dịch từ HDFS
Tốc độ: 5 giao dịch/giây
Progress: 100/50,000 (0.2%) - Sent: 100
...
```

**Để dừng:** Ctrl+C

#### Terminal 4️⃣: Fraud Detection Consumer

```bash
cd /home/jovyan/work/fraud_detection
python realtime_consumer.py
```

**Output:**
```
FRAUD DETECTION REAL-TIME CONSUMER
Loading GBT model from: gbt_fraud_model
✅ GBT model loaded successfully
✅ Connected to Kafka
✅ Fraud predictions Redis stream started
FRAUD_ALERTS:
TX #12345 - CASH_OUT - $500,000 - Probability: 87.3%
...
```

**Để dừng:** Ctrl+C

#### Terminal 5️⃣: Dashboard

```bash
cd /home/jovyan/work
python dashboard.py
```

**Output:**
```
🚀 Starting Real-time Dashboard
Dashboard URL: http://localhost:8050
Press Ctrl+C to stop
Dash is running on http://0.0.0.0:8050/
```

**Truy cập:** 
- Trong container: http://localhost:8050
- Từ host: http://localhost:8050

**Để dừng:** Ctrl+C

### OPTION 2: Chạy Tự động với Screen/Tmux

Sử dụng `tmux` để chạy nhiều processes:

```bash
# Install tmux (nếu chưa có)
conda install -c conda-forge tmux -y

# Tạo session
tmux new -s bigdata

# Window 1: Market Producer
cd /home/jovyan/work/market_risk && python get_finance_data.py

# Tạo window mới: Ctrl+B, C
# Window 2: Market Consumer
cd /home/jovyan/work/market_risk && python spark_consumer.py

# Tiếp tục tạo windows cho fraud producer, consumer, dashboard
# Navigate: Ctrl+B, N (next) / P (previous)
# Detach: Ctrl+B, D
# Reattach: tmux attach -t bigdata
```

## 📊 Monitoring & Verification

### 1. Kiểm tra Kafka Topics

```python
from kafka import KafkaConsumer
import json

# Consumer để xem messages
consumer = KafkaConsumer(
    'vietnam_stocks',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Đọc 5 messages
for i, message in enumerate(consumer):
    if i >= 5:
        break
    print(message.value)
```

### 2. Kiểm tra Redis Data

```python
import redis
import json

r = redis.Redis(host='redis', port=6379, decode_responses=True)

# Xem tất cả keys
keys = r.keys('*')
print(f"Total keys: {len(keys)}")

# Xem market data
market_keys = r.keys('market:latest:*')
for key in market_keys[:5]:
    data = json.loads(r.get(key))
    print(f"{data['symbol']}: {data['price']}")

# Xem fraud alerts
fraud_alerts = r.keys('fraud:alert:*')
print(f"Total fraud alerts: {len(fraud_alerts)}")
```

### 3. Kiểm tra Spark Jobs

Truy cập Spark UI:
- Spark Master: http://localhost:8080
- Application UI: http://localhost:4040 (khi consumer đang chạy)

## 🎨 Sử dụng Dashboard

### Market Risk Tab

**Displays:**
- 📊 Summary cards (Total stocks, High risk, Avg volatility, Foreign flow)
- 📈 Price chart (top 5 stocks)
- ⚠️ Risk score chart (by symbol)
- 📉 Volatility chart
- 💰 Foreign flow chart
- 🚨 Risk alerts list

**Features:**
- Auto-refresh: 5s
- Interactive charts
- Real-time updates

### Fraud Detection Tab

**Displays:**
- 📊 Summary cards (Total TX, Fraud alerts, Fraud rate, Amount at risk)
- 📈 Fraud timeline chart
- 🥧 Distribution by TX type
- 📋 Recent transactions table
- 🚨 Fraud alerts list

**Features:**
- Auto-refresh: 5s
- Fraud highlighting
- Probability indicators

### Statistics Tab

**Displays:**
- Market statistics
- Fraud statistics
- Historical performance

## 🔧 Troubleshooting

### Problem 1: Kafka Connection Failed

```python
# Error: NoBrokersAvailable
```

**Solution:**
```bash
# Kiểm tra Kafka service (trên host)
docker-compose logs kafka

# Restart Kafka (trên host)
docker-compose restart kafka zookeeper

# Đợi 30s để Kafka khởi động

# Test lại trong container
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
```

### Problem 2: Redis Connection Failed

```python
# Error: ConnectionError
```

**Solution:**
```bash
# Kiểm tra Redis (trên host)
docker-compose logs redis

# Restart (trên host)
docker-compose restart redis

# Test trong container
import redis
r = redis.Redis(host='redis', port=6379)
r.ping()
```

### Problem 3: HDFS File Not Found

```
# Error: Path does not exist: hdfs://namenode:9000/data/input/...
```

**Solution:**
```bash
# Từ host machine
docker exec namenode hdfs dfs -ls /data/input/

# Nếu không có file
docker exec namenode hdfs dfs -mkdir -p /data/input
docker exec namenode hdfs dfs -put <local_file> /data/input/
```

### Problem 4: Model Not Found

```
# Error: Path does not exist: gbt_fraud_model
```

**Solution:**
```bash
cd /home/jovyan/work/fraud_detection

# Kiểm tra
ls -la | grep model

# Nếu không có, cần train:
# 1. Chạy preprocessing.ipynb
# 2. Chạy train.ipynb
```

### Problem 5: vnstock API Error

```
# Error: Cannot fetch stock data
```

**Possible causes:**
- Thị trường đóng cửa (chỉ mở T2-T6, 9h-15h)
- Internet connection
- API rate limit

**Solution:**
```python
# Test vnstock
from vnstock3 import Vnstock
stock = Vnstock().stock(symbol='VCB', source='VCI')
df = stock.trading.price_board(symbols_list=['VCB'])
print(df)
```

### Problem 6: Port Already in Use

```
# Error: Address already in use: 8050
```

**Solution:**
```bash
# Tìm process đang dùng port
lsof -i :8050

# Kill process
kill -9 <PID>

# Hoặc dùng port khác
python dashboard.py --port 8051
```

## 📈 Performance Tuning

### Tăng throughput Kafka Producer

```python
# Trong get_finance_data.py
TRANSACTIONS_PER_SECOND = 10  # Tăng từ 5 lên 10
```

### Tăng batch size Spark

```python
# Trong spark_consumer.py
.trigger(processingTime="5 seconds")  # Giảm từ 15s xuống 5s
```

### Tăng Redis memory

```bash
# Trong docker-compose.yml (trên host)
redis:
  command: redis-server --maxmemory 512mb
```

## 🎯 Testing Scenarios

### Scenario 1: Market Risk Alert

```python
# Tạo dữ liệu test với high risk
# Trong get_finance_data.py, modify data để risk_score > 2.5
# Dashboard sẽ hiển thị alert
```

### Scenario 2: Fraud Detection

```python
# Producer sẽ tự động đọc dữ liệu có fraud
# Xem fraud alerts trong Dashboard tab 2
# Kiểm tra accuracy trong console output
```

## 📝 Logs & Debugging

### Enable Debug Logs

```python
# Trong mỗi file .py, thêm:
import logging
logging.basicConfig(level=logging.DEBUG)
```

### View Spark Logs

```bash
# Application logs
tail -f /tmp/spark*.log

# Hoặc xem trong Spark UI
```

### View Redis Logs

```bash
# Từ host
docker-compose logs -f redis
```

## 🔄 Restart Everything

```bash
# Stop all Python processes
# Ctrl+C trong mỗi terminal

# Từ host, restart services
docker-compose restart

# Đợi 1 phút để services khởi động

# Vào lại container
docker exec -it pyspark-notebook bash

# Chạy lại từng component
```

## ✅ Verification Checklist

- [ ] Kafka producer đang chạy và gửi dữ liệu
- [ ] Spark consumer nhận được dữ liệu từ Kafka
- [ ] Redis có dữ liệu (check bằng `redis-cli KEYS *`)
- [ ] Dashboard hiển thị dữ liệu
- [ ] Dashboard auto-refresh hoạt động
- [ ] Fraud alerts hiển thị (nếu có fraud)
- [ ] Market risk alerts hiển thị (nếu có high risk)

## 🚀 Production Checklist

Nếu deploy production, cần:

- [ ] Authentication cho tất cả services
- [ ] SSL/TLS encryption
- [ ] Monitoring (Prometheus, Grafana)
- [ ] Alerting system
- [ ] Backup strategy
- [ ] High availability setup
- [ ] Load balancing
- [ ] Security hardening

---

**Chúc bạn chạy thành công! 🎉**

Có vấn đề gì, check lại từng bước hoặc xem logs để debug.
