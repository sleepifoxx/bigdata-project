# 🔍 Real-time Fraud Detection System

Hệ thống phát hiện giao dịch lừa đảo real-time với Kafka, Spark MLlib và Plotly Dash.

## 📊 Kiến trúc hệ thống

```
CSV File (HDFS)
    ↓
Kafka Producer (giả lập realtime)
    ↓
Kafka Topic: "transactions"
    ↓
Spark Streaming + MLlib (dự đoán fraud)
    ↓
Kafka Topic: "predictions"
    ↓
Dashboard (Plotly Dash - visualize realtime)
```

## 🗂️ Cấu trúc Files

```
├── kafka_producer.py          # Producer: CSV → Kafka
├── spark_consumer.py          # Consumer: Kafka → ML Model → Kafka
├── dashboard.py               # Dashboard: Kafka → Visualize
├── requirements_dashboard.txt # Python dependencies cho dashboard
├── run_system.sh             # Script chạy hệ thống
├── gbt_fraud_model/          # GBT model đã train (từ train.ipynb)
└── spark_scaler_model/       # Scaler model (từ train.ipynb)
```

## 🚀 Cách chạy

### Bước 1: Đảm bảo đã train model

Chạy notebook `train.ipynb` để train model và tạo:
- `gbt_fraud_model/`
- `spark_scaler_model/`

### Bước 2: Chuẩn bị dữ liệu

Upload file CSV lên HDFS:
```bash
# Copy file vào namenode container
docker cp paysim_realtime.csv namenode:/tmp/

# Upload lên HDFS
docker exec -it namenode bash
hdfs dfs -mkdir -p /data/input
hdfs dfs -put /tmp/paysim_realtime.csv /data/input/
hdfs dfs -ls /data/input
```

### Bước 3: Chạy hệ thống (3 terminals)

#### **Terminal 1: Kafka Producer**
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  kafka_producer.py
```

**Chức năng:**
- Đọc CSV từ HDFS (`/data/input/paysim_realtime.csv`)
- Gửi từng dòng vào Kafka topic `transactions` (giả lập realtime)
- Delay 0.1 giây giữa các transactions

#### **Terminal 2: Spark Consumer**
```bash
python spark_consumer_batch.py
```

**Chức năng:**
- Nhận transactions từ Kafka topic `transactions`
- Tạo features (giống preprocessing)
- Dự đoán fraud với GBT model đã train
- Gửi kết quả vào Kafka topic `predictions`

#### **Terminal 3: Dashboard**
```bash
# Install dependencies
pip install -r requirements.txt

# Run dashboard
python dashboard.py
```

**Chức năng:**
- Nhận predictions từ Kafka topic `predictions`
- Hiển thị dashboard realtime tại http://localhost:8050

### Hoặc dùng script tự động:
```bash
chmod +x run_system.sh
./run_system.sh
```

## 📊 Dashboard Features

Dashboard hiển thị:

### 1. **Metrics Cards**
- Total Transactions
- Fraud Detected
- Fraud Rate (%)
- Total Amount ($)

### 2. **Charts**
- **Transaction Timeline**: Biểu đồ scatter theo thời gian (phân biệt normal/fraud)
- **Fraud Distribution**: Số lượng fraud theo loại giao dịch
- **Amount Distribution**: Box plot so sánh amount của normal vs fraud
- **Confusion Matrix**: Ma trận đánh giá performance

### 3. **Recent Transactions Table**
- 20 giao dịch gần nhất
- Highlight màu đỏ cho fraud alerts
- Hiển thị prediction probability

## ⚙️ Cấu hình

### kafka_producer.py
```python
HDFS_PATH = "hdfs://namenode:9000/data/input/paysim_realtime.csv"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "transactions"
DELAY_SECONDS = 1  # Thời gian delay giữa các transactions
```

### spark_consumer.py
```python
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "transactions"
OUTPUT_TOPIC = "predictions"
BEST_THRESHOLD = 0.50  # Ngưỡng phân loại fraud (từ training)
```

### dashboard.py
```python
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "predictions"
MAX_POINTS = 100  # Số điểm tối đa trên biểu đồ
```

## 🔧 Troubleshooting

### 1. Lỗi kết nối Kafka
```bash
# Check Kafka container
docker ps | grep kafka

# Check Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Tạo topic thủ công (nếu cần)
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --partitions 1 \
  --replication-factor 1
```

### 2. Lỗi load model
```bash
# Đảm bảo model đã được train và lưu
ls -la gbt_fraud_model/
ls -la spark_scaler_model/

# Nếu chưa có, chạy train.ipynb
```

### 3. Dashboard không hiển thị dữ liệu
```bash
# Check logs của consumer
# Đảm bảo consumer đang gửi vào topic "predictions"

# Check Kafka messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic predictions \
  --from-beginning
```

### 4. Lỗi HDFS connection
```bash
# Check namenode
docker exec -it namenode hdfs dfsadmin -report

# Check file tồn tại
docker exec -it namenode hdfs dfs -ls /data/input/
```

## 📈 Performance Tips

1. **Tăng tốc độ streaming**: Giảm `DELAY_SECONDS` trong producer
2. **Tăng buffer size**: Tăng `MAX_POINTS` trong dashboard
3. **Batch processing**: Modify consumer để xử lý batch thay vì từng record

## 🎯 Mục tiêu đạt được

✅ **Real-time streaming**: Giả lập transactions từ CSV  
✅ **ML Prediction**: Dự đoán fraud với Spark MLlib GBT model  
✅ **Kafka Pipeline**: Producer → Consumer với 2 topics  
✅ **Dashboard**: Visualize realtime với Plotly Dash  
✅ **Alerts**: Cảnh báo fraud transactions  
✅ **Metrics**: Theo dõi performance (confusion matrix, fraud rate)  

## 📝 Notes

- System chạy trong Docker containers (theo docker-compose.yml)
- Producer giả lập realtime bằng cách delay 1s giữa các transactions
- Model đã được train với GBTClassifier (Spark MLlib)
- Dashboard tự động update mỗi 1 giây
- Hỗ trợ xử lý imbalanced data với weighted training

## 🔗 URLs

- **Dashboard**: http://localhost:8050
- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **HDFS NameNode UI**: http://localhost:9870

## 📧 Liên hệ

Nếu có vấn đề, check logs trong từng terminal để debug.

---
**Chúc bạn thành công! 🎉**
