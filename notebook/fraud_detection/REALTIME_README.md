# Fraud Detection Real-time System

Hệ thống phát hiện gian lận giao dịch theo thời gian thực sử dụng Kafka, Spark Streaming và Machine Learning.

## Kiến trúc

```
HDFS (paysim_realtime.csv)
    ↓
Kafka Producer (5 tx/s)
    ↓
Kafka Topic: fraud_transactions
    ↓
Spark Streaming Consumer
    ↓ (ML Model)
Fraud Detection Results
```

## Quy trình

### 1. Training Model (Đã hoàn thành)

Sử dụng notebook `train.ipynb` để train GBT model:
- **Model**: GBTClassifier (Gradient Boosted Trees)
- **Features**: 6 features
  - `type_encoded`: Loại giao dịch (0: CASH_OUT, 1: TRANSFER)
  - `amount_log`: Log(amount + 1)
  - `errorBalanceOrig`: Sai số cân bằng tài khoản nguồn
  - `errorBalanceDest`: Sai số cân bằng tài khoản đích
  - `amount_over_oldbalance`: Tỷ lệ amount/số dư cũ
  - `hour`: Giờ trong ngày (0-23)
- **Threshold tối ưu**: 0.45 (có thể điều chỉnh)
- **Output**: 
  - `gbt_fraud_model/`: Trained GBT model
  - `spark_scaler_model/`: StandardScaler model

### 2. Chuẩn bị dữ liệu Real-time

Upload file dữ liệu lên HDFS:

```bash
# Tải dữ liệu mẫu (nếu chưa có)
wget https://example.com/paysim_realtime.csv

# Upload lên HDFS
docker exec -it namenode hdfs dfs -mkdir -p /data/input
docker exec -it namenode hdfs dfs -put paysim_realtime.csv /data/input/

# Kiểm tra
docker exec -it namenode hdfs dfs -ls /data/input/
```

### 3. Chạy Kafka Producer

Producer đọc dữ liệu từ HDFS và gửi 5 giao dịch/giây vào Kafka:

```bash
cd /home/jovyan/work/fraud_detection
python realtime_producer.py
```

**Output mẫu:**
```
2025-11-15 10:00:00 - INFO - FRAUD DETECTION REAL-TIME PRODUCER
2025-11-15 10:00:00 - INFO - Kafka Producer initialized. Topic: fraud_transactions
2025-11-15 10:00:01 - INFO - Đã đọc 50,000 giao dịch từ HDFS
2025-11-15 10:00:01 - INFO - Tốc độ: 5 giao dịch/giây
2025-11-15 10:00:01 - INFO - Bắt đầu gửi dữ liệu...
2025-11-15 10:00:02 - INFO - Progress: 5/50,000 (0.0%) - Sent: 5
2025-11-15 10:00:03 - INFO - Progress: 10/50,000 (0.0%) - Sent: 10
...
```

### 4. Chạy Spark Streaming Consumer

Consumer nhận dữ liệu từ Kafka, xử lý và dự đoán gian lận:

```bash
cd /home/jovyan/work/fraud_detection
python realtime_consumer.py
```

Hoặc chạy với spark-submit:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  realtime_consumer.py
```

**Output:**

#### Stream 1: All Transactions with Predictions
```
tx_id  transaction_time     tx_type   amount    from_account  to_account  actual_fraud  predicted_fraud  fraud_prob_pct
1234   2025-11-15 10:00:01  TRANSFER  150000.0  C123456       C789012     0             0                12.5
1235   2025-11-15 10:00:01  CASH_OUT  500000.0  C234567       C890123     1             1                87.3
```

#### Stream 2: Fraud Alerts Only
```
🚨 FRAUD_ALERTS:
tx_id  transaction_time     tx_type   amount     from_account  to_account  fraud_prob_pct
1235   2025-11-15 10:00:01  CASH_OUT  500000.0   C234567       C890123     87.3
1298   2025-11-15 10:00:14  TRANSFER  2500000.0  C345678       C901234     92.1
```

#### Stream 3: Performance Metrics (Every 30s)
```
PERFORMANCE_METRICS:
window_start         window_end           total_tx  actual_frauds  predicted_frauds  accuracy_pct  true_pos  false_pos  false_neg  avg_fraud_prob_pct
2025-11-15 10:00:00  2025-11-15 10:00:30  150       8              9                 96.7          7         2          1          23.5
```

## Cấu hình

### realtime_producer.py

```python
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
KAFKA_TOPIC = 'fraud_transactions'
HDFS_INPUT_PATH = 'hdfs://namenode:9000/data/input/paysim_realtime.csv'
TRANSACTIONS_PER_SECOND = 5  # Điều chỉnh tốc độ
```

### realtime_consumer.py

```python
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "fraud_transactions"
MODEL_PATH = "gbt_fraud_model"
SCALER_PATH = "spark_scaler_model"
FRAUD_THRESHOLD = 0.45  # Điều chỉnh threshold (0.0-1.0)
```

## Các Chỉ Số Đánh Giá

### Performance Metrics

- **Accuracy**: Tỷ lệ dự đoán đúng tổng thể
- **True Positives (TP)**: Phát hiện đúng gian lận
- **False Positives (FP)**: Cảnh báo nhầm (không gian lận nhưng báo gian lận)
- **False Negatives (FN)**: Bỏ sót gian lận (gian lận nhưng không phát hiện)
- **Precision**: TP / (TP + FP) - Độ chính xác của cảnh báo
- **Recall**: TP / (TP + FN) - Tỷ lệ phát hiện được gian lận

### Threshold Tuning

Điều chỉnh `FRAUD_THRESHOLD` để cân bằng Precision/Recall:

- **Threshold cao (0.6-0.9)**: 
  - ✅ Precision cao (ít false positive)
  - ❌ Recall thấp (bỏ sót nhiều gian lận)
  - **Use case**: Chi phí false positive cao

- **Threshold thấp (0.2-0.4)**: 
  - ✅ Recall cao (phát hiện nhiều gian lận)
  - ❌ Precision thấp (nhiều false positive)
  - **Use case**: Chi phí bỏ sót gian lận cao

- **Threshold tối ưu (0.45)**: 
  - ⚖️ Cân bằng Precision/Recall
  - **Use case**: Đa số trường hợp

## Features Engineering

Hệ thống tự động tạo các features từ dữ liệu thô:

1. **type_encoded**: Mã hóa loại giao dịch
2. **errorBalanceOrig**: `oldBalanceOrig - newBalanceOrig - amount`
   - Phát hiện bất thường về số dư
3. **errorBalanceDest**: `oldBalanceDest + amount - newBalanceDest`
   - Phát hiện bất thường về số dư đích
4. **amount_over_oldbalance**: `amount / oldBalanceOrig`
   - Phát hiện rút tiền vượt số dư
5. **hour**: `step % 24`
   - Phát hiện giao dịch bất thường theo thời gian
6. **amount_log**: `log(amount + 1)`
   - Xử lý phân phối lệch của amount

## Monitoring

### Kafka Topics

```bash
# Xem messages trong topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraud_transactions \
  --from-beginning \
  --max-messages 10
```

### Spark UI

- Spark Master: http://localhost:8080
- Spark Application UI: http://localhost:4040 (khi consumer đang chạy)

### HDFS Web UI

- NameNode: http://localhost:9870

## Troubleshooting

### Lỗi: Model not found

```bash
# Kiểm tra model đã được train chưa
ls -la gbt_fraud_model/
ls -la spark_scaler_model/

# Nếu chưa có, chạy notebook train.ipynb
```

### Lỗi: HDFS file not found

```bash
# Kiểm tra file trên HDFS
docker exec -it namenode hdfs dfs -ls /data/input/

# Upload file
docker exec -it namenode hdfs dfs -put paysim_realtime.csv /data/input/
```

### Lỗi: Kafka connection refused

```bash
# Kiểm tra Kafka đang chạy
docker ps | grep kafka

# Restart Kafka
docker-compose restart kafka zookeeper
```

### Lỗi: Out of memory

```python
# Giảm tốc độ trong producer
TRANSACTIONS_PER_SECOND = 2  # Giảm từ 5 xuống 2

# Hoặc lấy mẫu dữ liệu nhỏ hơn trong producer
if total_count > 50000:
    df = df.limit(50000)  # Giảm từ 100,000 xuống 50,000
```

## Mở rộng

### 1. Lưu kết quả vào Database

Thêm sink vào consumer:

```python
# Ví dụ: Lưu fraud alerts vào Parquet
fraud_alerts.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/fraud_alerts") \
    .option("checkpointLocation", "/tmp/fraud_alerts_checkpoint") \
    .start()
```

### 2. Alert System

Tích hợp với hệ thống cảnh báo:

```python
def send_alert(row):
    if row['predicted_fraud'] == 1:
        # Gửi email, SMS, webhook, etc.
        send_notification(
            f"FRAUD ALERT: TX {row['tx_id']} - "
            f"Amount: {row['amount']} - "
            f"Probability: {row['fraud_prob_pct']}%"
        )
```

### 3. Dashboard Real-time

Sử dụng `dashboard.py` để visualize:
- Số lượng giao dịch theo thời gian
- Tỷ lệ gian lận
- Accuracy metrics
- Top suspicious transactions

### 4. Model Retraining

Định kỳ retrain model với dữ liệu mới:

```bash
# Thu thập dữ liệu mới từ Kafka/HDFS
# Chạy lại preprocessing.ipynb
# Chạy lại train.ipynb
# Model mới sẽ tự động được load ở lần restart consumer tiếp theo
```

## Best Practices

1. **Monitoring**: Luôn theo dõi metrics (accuracy, precision, recall)
2. **Threshold Tuning**: Điều chỉnh threshold theo business requirements
3. **Data Quality**: Đảm bảo dữ liệu input đầy đủ và chính xác
4. **Model Update**: Định kỳ retrain model với dữ liệu mới
5. **Alerting**: Thiết lập cảnh báo cho fraud probability cao
6. **Logging**: Lưu lại predictions để phân tích sau

## Performance

- **Throughput**: ~5-10 giao dịch/giây (có thể scale với Spark cluster)
- **Latency**: ~100-500ms mỗi prediction
- **Accuracy**: ~95-98% (tùy thuộc vào dữ liệu)

---

**Lưu ý**: Hệ thống này chỉ mang tính demo. Trong production cần:
- High availability setup (Kafka cluster, Spark cluster)
- Data backup và recovery
- Security (encryption, authentication)
- Comprehensive monitoring và alerting
