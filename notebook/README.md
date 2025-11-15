# 🚀 Real-time Big Data Analytics Platform

Hệ thống phân tích dữ liệu lớn theo thời gian thực với Market Risk Monitoring và Fraud Detection.

## 📋 Tổng quan

Platform này bao gồm 2 hệ thống chính:

1. **Market Risk Monitoring**: Theo dõi và phân tích rủi ro thị trường chứng khoán Việt Nam
2. **Fraud Detection**: Phát hiện gian lận giao dịch tài chính real-time

## 🏗️ Kiến trúc Hệ thống

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                  │
│  • vnstock API (Stock Market)                                   │
│  • HDFS (Transaction Data)                                      │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   KAFKA PRODUCERS                                │
│  • Market Data Producer (10 stocks/15s)                         │
│  • Fraud Data Producer (5 transactions/s)                       │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   KAFKA TOPICS                                   │
│  • vietnam_stocks                                               │
│  • fraud_transactions                                           │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│               SPARK STREAMING CONSUMERS                          │
│  • Market Risk Analyzer                                         │
│  • Fraud Detection ML Model                                     │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   REDIS DATABASE                                 │
│  • Market data & alerts                                         │
│  • Fraud predictions & alerts                                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                REAL-TIME DASHBOARD                               │
│  • Plotly Dash Web Interface                                    │
│  • Auto-refresh every 5 seconds                                 │
```

## 🛠️ Tech Stack

- **Big Data**: Apache Hadoop, Apache Spark
- **Streaming**: Apache Kafka
- **Database**: Redis
- **ML**: PySpark MLlib (GBT Classifier)
- **Dashboard**: Plotly Dash
- **Container**: Docker & Docker Compose
- **Language**: Python 3

## 📁 Cấu trúc Project

```
/home/jovyan/work/
├── README.md                          # File này
├── RUN_GUIDE.md                       # Hướng dẫn chạy chi tiết
├── docker-compose.yml                 # Docker services
├── requirements.txt                   # Python dependencies
├── dashboard.py                       # Main dashboard application
│
├── market_risk/                       # Market Risk Module
│   ├── README.md
│   ├── RISK_METRICS_GUIDE.md
│   ├── get_finance_data.py           # Kafka producer
│   └── spark_consumer.py             # Spark consumer
│
├── fraud_detection/                   # Fraud Detection Module
│   ├── REALTIME_README.md
│   ├── preprocessing.ipynb           # Data preprocessing
│   ├── train.ipynb                   # Model training
│   ├── realtime_producer.py          # Kafka producer
│   ├── realtime_consumer.py          # Spark consumer
│   └── start_fraud_detection.sh      # Helper script
│
└── redis/                             # Redis Module
    ├── README.md
    ├── redis_helper.py               # Helper functions
    ├── redis_sink.py                 # Spark sink
    └── start_redis.sh                # Management script
```

## 🚀 Quick Start

### ⚠️ Quan trọng - Về Container Environment

**Bạn đang ở trong container `pyspark-notebook`**

- ✅ CÓ THỂ: Chạy Python scripts, Spark jobs, truy cập services
- ❌ KHÔNG THỂ: Chạy `docker` commands, quản lý containers
- 🔗 Kết nối: Dùng service names (kafka, redis, namenode, spark-master)

Tất cả các file Python đã được cấu hình sẵn với connection strings phù hợp:
- Kafka: `kafka:9092`
- Redis: `redis:6379`
- HDFS: `hdfs://namenode:9000`
- Spark: `spark://spark-master:7077`

### Bước 1: Cài đặt Dependencies

```bash
cd /home/jovyan/work
pip install -r requirements.txt
```

### Bước 2: Chạy Market Risk Monitor

**Terminal 1: Producer**
```bash
cd /home/jovyan/work/market_risk
python get_finance_data.py
```

**Terminal 2: Consumer**
```bash
cd /home/jovyan/work/market_risk
python spark_consumer.py
```

### Bước 3: Chạy Fraud Detection

**Chuẩn bị:**
- Upload dữ liệu lên HDFS (từ host machine)
- Train model (chạy `train.ipynb`)

**Terminal 3: Producer**
```bash
cd /home/jovyan/work/fraud_detection
python realtime_producer.py
```

**Terminal 4: Consumer**
```bash
cd /home/jovyan/work/fraud_detection
python realtime_consumer.py
```

### Bước 4: Chạy Dashboard

**Terminal 5: Dashboard**
```bash
cd /home/jovyan/work
python dashboard.py
```

Truy cập: http://localhost:8050

## 📊 Chức năng Chính

### 1. Market Risk Monitoring

**Features:**
- ✅ Theo dõi 10 mã chứng khoán nổi bật VN
- ✅ Phân tích rủi ro real-time
- ✅ Tính toán 20+ chỉ số kỹ thuật
- ✅ Cảnh báo rủi ro cao
- ✅ Phân tích dòng tiền nước ngoài

**Metrics:**
- Volatility Risk Level
- Risk Score (1-3)
- Foreign Flow Analysis
- Price Position
- Risk/Reward Ratio

### 2. Fraud Detection

**Features:**
- ✅ Phát hiện gian lận real-time
- ✅ ML model accuracy ~96%
- ✅ Xử lý 5 giao dịch/giây
- ✅ Cảnh báo tức thì
- ✅ Performance tracking

**Metrics:**
- Fraud Probability (%)
- True/False Positives
- Accuracy, Precision, Recall
- Transaction Analysis

### 3. Real-time Dashboard

**Tabs:**
- 📈 Market Risk Monitor
- 🛡️ Fraud Detection
- 📊 Statistics & Analytics

**Features:**
- Auto-refresh (5s interval)
- Interactive charts
- Real-time alerts
- Historical data

## 🔗 Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| Dashboard | http://localhost:8050 | Main dashboard |
| Jupyter | http://localhost:8888 | Notebook interface |
| Spark Master UI | http://localhost:8080 | Spark cluster |
| Spark Worker UI | http://localhost:8081 | Worker status |
| HDFS NameNode | http://localhost:9870 | HDFS web UI |
| YARN ResourceManager | http://localhost:8089 | YARN UI |

## 📦 Docker Services

Các services đang chạy (quản lý từ host machine):

```yaml
# Services trong docker-compose.yml:
✓ zookeeper     - Kafka coordination (port 2181)
✓ kafka         - Message broker (port 9092)
✓ redis         - Real-time database (port 6379)
✓ namenode      - HDFS master (port 9870)
✓ datanode1/2   - HDFS storage
✓ spark-master  - Spark master (port 7077, 8080)
✓ spark-worker  - Spark executor (port 8081)
✓ pyspark       - Jupyter + PySpark (YOU ARE HERE)
```

**Để kiểm tra services** (từ host machine, không phải trong container):
```bash
docker-compose ps
docker-compose logs <service_name>
```

## 🔧 Connection Strings

Tất cả các file Python đã được cấu hình đúng cho môi trường container:

```python
# Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

# Redis  
REDIS_HOST = "redis"
REDIS_PORT = 6379

# HDFS
HDFS_PATH = "hdfs://namenode:9000/data/input/..."

# Spark Master
SPARK_MASTER = "spark://spark-master:7077"
```

✅ **Không cần sửa gì** - các kết nối đã đúng!

## 📝 Hướng dẫn Chi tiết

Xem file `RUN_GUIDE.md` để có hướng dẫn từng bước chi tiết.

## 🔧 Troubleshooting

### Lỗi kết nối Kafka
```python
# Kiểm tra từ Python trong container
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
# Nếu lỗi: Kafka chưa sẵn sàng, restart từ host
```

### Lỗi kết nối Redis
```python
import redis
r = redis.Redis(host='redis', port=6379)
r.ping()  # Nên return True
```

### Lỗi HDFS
```bash
# Kiểm tra từ host machine
docker exec namenode hdfs dfs -ls /data/input/
```

## 📚 Documentation

- [Market Risk Guide](market_risk/RISK_METRICS_GUIDE.md)
- [Fraud Detection Guide](fraud_detection/REALTIME_README.md)
- [Redis Module Guide](redis/README.md)
- [Run Guide](RUN_GUIDE.md) - Chi tiết cách chạy

## 🎯 Performance

- **Market Data**: ~10 stocks, 15s interval
- **Fraud Detection**: ~5 tx/s throughput
- **Dashboard**: <100ms latency
- **ML Model**: ~96% accuracy

## 🔐 Security Notes

⚠️ **Lưu ý**: Đây là môi trường development/demo
- Không có authentication
- Không mã hóa dữ liệu
- Không có access control

Cho production cần:
- Kafka SSL/SASL
- Redis authentication
- HDFS Kerberos
- Dashboard authentication

## 🤝 Contributing

Module structure cho việc mở rộng:
- Thêm data source mới vào producers
- Thêm metrics mới vào consumers
- Thêm visualizations vào dashboard

## 📄 License

MIT License - Educational/Demo Purpose

---

**Lưu ý quan trọng**: 
- Bạn đang ở trong container `pyspark-notebook`
- Không thể chạy `docker` commands từ đây
- Chỉ chạy Python scripts và Spark jobs
- Để quản lý containers, exit ra host machine

**Next Steps**: Xem `RUN_GUIDE.md` để bắt đầu!

## 🔧 Quản lý Hệ thống

### Kiểm tra Services

```bash
# Docker services
docker-compose ps

# Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Redis data
cd /home/jovyan/work/redis
./start_redis.sh
```

### Monitoring

#### Spark UI
- Master: http://localhost:8080
- Application: http://localhost:4040 (khi consumer chạy)

#### HDFS UI
- NameNode: http://localhost:9870

#### Kafka Consumer

```bash
# Xem messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic vietnam_stocks \
  --max-messages 5

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraud_transactions \
  --max-messages 5
```

#### Redis

```bash
# Redis CLI
docker exec -it redis redis-cli

# Commands:
# KEYS *
# GET market:latest:VCB
# ZRANGE fraud:alerts:recent 0 -1
```

### Dừng Hệ thống

```bash
# Dừng Python processes (Ctrl+C trong từng terminal)

# Dừng Docker services
docker-compose down

# Hoặc chỉ dừng specific services
docker-compose stop kafka redis spark-master spark-worker
```

## 📈 Chỉ số Hiệu suất

### Market Risk
- **Throughput**: 5-10 stocks/interval (15s)
- **Latency**: <500ms per stock
- **Metrics**: 15+ indicators per stock
- **Risk Levels**: LOW, MEDIUM, HIGH

### Fraud Detection
- **Throughput**: 5 transactions/second
- **Latency**: 100-500ms per prediction
- **Accuracy**: 95-98%
- **Model**: GBTClassifier with 6 features

### Dashboard
- **Refresh Rate**: 5 seconds
- **Data Retention**: 24 hours (Redis)
- **Concurrent Users**: 10+

## 🎯 Use Cases

### 1. Market Risk Management
- Theo dõi biến động thị trường real-time
- Phát hiện cổ phiếu rủi ro cao
- Phân tích dòng tiền nước ngoài
- Cảnh báo volatility bất thường

### 2. Fraud Prevention
- Phát hiện giao dịch gian lận tức thì
- Ngăn chặn tổn thất tài chính
- Phân tích pattern giao dịch
- Audit trail đầy đủ

### 3. Business Intelligence
- Dashboard trực quan, real-time
- Historical analysis
- Performance tracking
- Decision support

## 🔍 Troubleshooting

### Kafka Connection Error
```bash
docker-compose restart kafka zookeeper
docker logs kafka
```

### Spark Memory Error
```bash
# Tăng memory trong docker-compose.yml
# Hoặc giảm batch size trong producers
```

### Redis Out of Memory
```bash
# Clear old data
docker exec -it redis redis-cli FLUSHDB

# Hoặc dùng script
cd /home/jovyan/work/redis
./start_redis.sh --clear
```

### Model Not Found (Fraud Detection)
```bash
# Train model trước
# Chạy notebooks: preprocessing.ipynb -> train.ipynb
```

### HDFS File Not Found
```bash
# Upload file
docker exec -it namenode hdfs dfs -put <local_file> /data/input/
```

## 📚 Documentation

- **Market Risk**: [market_risk/README.md](market_risk/README.md)
- **Risk Metrics**: [market_risk/RISK_METRICS_GUIDE.md](market_risk/RISK_METRICS_GUIDE.md)
- **Fraud Detection**: [fraud_detection/REALTIME_README.md](fraud_detection/REALTIME_README.md)
- **Redis**: [redis/README.md](redis/README.md)

## 🛠️ Tech Stack

- **Message Broker**: Apache Kafka
- **Stream Processing**: Apache Spark (PySpark)
- **Storage**: HDFS, Redis
- **Machine Learning**: Spark MLlib (GBTClassifier)
- **Dashboard**: Plotly Dash
- **Data Source**: vnstock API
- **Container**: Docker, Docker Compose

## 📝 Configuration Files

### docker-compose.yml
Services: Kafka, Zookeeper, Redis, HDFS, Spark

### requirements.txt
Python dependencies

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS`: kafka:9092
- `REDIS_HOST`: redis
- `HDFS_NAMENODE`: hdfs://namenode:9000

## 🚨 Important Notes

1. **Market Hours**: vnstock API chỉ có dữ liệu khi thị trường mở (T2-T6, 9h-15h VN time)
2. **Rate Limits**: Tránh spam API, sử dụng interval >= 15s
3. **Memory**: Spark cần ít nhất 2GB RAM
4. **Data Retention**: Redis lưu 24h, sau đó tự động xóa
5. **Model Updates**: Định kỳ retrain model với dữ liệu mới

## 🔒 Security

- Redis không có authentication (development only)
- Kafka không có SSL (development only)
- Dashboard không có login (development only)

**⚠️ Không sử dụng trực tiếp trong production!**

## 📞 Support

Nếu gặp vấn đề:

1. Check logs: `docker logs <container_name>`
2. Check services: `docker-compose ps`
3. Check Redis: `./redis/start_redis.sh`
4. Check HDFS: http://localhost:9870
5. Check Spark: http://localhost:8080

## 🎓 Learning Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Plotly Dash](https://dash.plotly.com/)
- [Redis Documentation](https://redis.io/documentation)

## 📜 License

MIT License - For educational purposes

## 🙏 Acknowledgments

- vnstock API for Vietnam stock market data
- Apache Kafka for message streaming
- Apache Spark for stream processing
- Redis for fast data storage
- Plotly Dash for beautiful visualizations

---

**Version**: 1.0.0  
**Last Updated**: November 15, 2025  
**Maintained by**: Big Data Analytics Team
