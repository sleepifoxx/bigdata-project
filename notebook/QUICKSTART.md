# 🚀 QUICK START GUIDE

## Bước 1: Chuẩn bị model (chạy 1 lần duy nhất)

```bash
# Mở và chạy toàn bộ notebook train.ipynb
# Notebook này sẽ tạo:
# - gbt_fraud_model/
# - spark_scaler_model/
```

Hoặc mở Jupyter và chạy: http://localhost:8888

## Bước 2: Chuẩn bị dữ liệu

```bash
# Copy file CSV vào namenode container
docker cp /path/to/paysim_realtime.csv namenode:/tmp/

# Upload lên HDFS
docker exec -it namenode bash
hdfs dfs -mkdir -p /data/input
hdfs dfs -put /tmp/paysim_realtime.csv /data/input/
hdfs dfs -ls /data/input/
exit
```

## Bước 3: Kiểm tra models

```bash
python check_models.py
```

## Bước 4: Tạo Kafka topics (nếu chưa có)

```bash
chmod +x kafka_monitor.sh
./kafka_monitor.sh
# Chọn option 4 để tạo topics
```

## Bước 5: Chạy hệ thống

### Cách 1: Script tự động (dùng tmux)
```bash
chmod +x start_all.sh
./start_all.sh
```

### Cách 2: Chạy thủ công (3 terminals riêng biệt)

**Terminal 1 - Producer:**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 kafka_producer.py
```

**Terminal 2 - Consumer:**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_consumer.py
```

**Terminal 3 - Dashboard:**
```bash
pip install -r requirements_dashboard.txt
python dashboard.py
```

### Cách 3: Dùng menu script
```bash
chmod +x run_system.sh
./run_system.sh
# Chọn component muốn chạy
```

## Bước 6: Xem Dashboard

Mở trình duyệt và truy cập:
```
http://localhost:8050
```

## 🎯 Kết quả mong đợi

Dashboard sẽ hiển thị:
- ✅ Metrics: Total transactions, Fraud count, Fraud rate
- 📊 Charts: Timeline, Distribution, Confusion Matrix
- 📋 Table: Recent 20 transactions
- 🚨 Fraud alerts với highlight màu đỏ

## 🔧 Troubleshooting

### Lỗi: Models not found
```bash
# Chạy train.ipynb để tạo models
# Hoặc check:
ls -la gbt_fraud_model/
ls -la spark_scaler_model/
```

### Lỗi: Kafka connection refused
```bash
# Check Kafka đang chạy
docker ps | grep kafka

# Restart Kafka
docker-compose restart kafka zookeeper
```

### Lỗi: File not found on HDFS
```bash
# Check file trên HDFS
docker exec -it namenode hdfs dfs -ls /data/input/

# Upload lại nếu cần
docker exec -it namenode hdfs dfs -put /tmp/paysim_realtime.csv /data/input/
```

### Dashboard không nhận data
```bash
# Check Spark consumer có chạy không
# Check logs xem có messages trong Kafka không

# Monitor Kafka topic
./kafka_monitor.sh
# Chọn option 2 hoặc 3 để xem messages
```

## 📊 Monitor hệ thống

### Spark UI
- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081

### HDFS
- NameNode UI: http://localhost:9870

### Dashboard
- Fraud Detection: http://localhost:8050

## ⏹️ Dừng hệ thống

```bash
# Nếu dùng tmux
tmux kill-session -t fraud_detection

# Nếu chạy thủ công
# Nhấn Ctrl+C ở mỗi terminal
```

## 🎉 Hoàn thành!

Hệ thống realtime fraud detection đã sẵn sàng!

---
**Lưu ý:** 
- Producer sẽ gửi mỗi 1 giây 1 transaction (có thể thay đổi DELAY_SECONDS)
- Dashboard tự động update mỗi 1 giây
- Model dự đoán với threshold = 0.50 (có thể thay đổi BEST_THRESHOLD)
