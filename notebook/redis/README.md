# Redis Module - Data Storage for Real-time Dashboard

Module Redis để lưu trữ và quản lý dữ liệu cho Market Risk và Fraud Detection Dashboard.

## Cấu trúc Module

```
redis/
├── redis_helper.py      # Helper functions để tương tác với Redis
├── redis_sink.py        # Spark Streaming sink functions
├── README.md           # File này
└── start_redis.sh      # Script khởi động và kiểm tra Redis
```

## Các File

### 1. redis_helper.py

Helper class để tương tác với Redis một cách dễ dàng.

**Chức năng:**
- `save_market_stock()`: Lưu dữ liệu stock market
- `save_market_alert()`: Lưu cảnh báo rủi ro market
- `save_market_statistics()`: Lưu thống kê market
- `save_transaction()`: Lưu giao dịch
- `save_fraud_alert()`: Lưu cảnh báo gian lận
- `save_fraud_statistics()`: Lưu thống kê fraud
- `get_*()`: Các hàm lấy dữ liệu

**Sử dụng:**
```python
from redis_helper import get_redis_helper

redis = get_redis_helper()

# Lưu market data
redis.save_market_stock('VCB', {
    'price': 90200,
    'volume': 1000000,
    'risk_score': 1.5
})

# Lấy dữ liệu
stocks = redis.get_market_latest_stocks(limit=100)
alerts = redis.get_market_alerts(limit=50)
```

### 2. redis_sink.py

Spark Streaming sink functions để ghi dữ liệu từ Spark vào Redis.

**Functions:**
- `write_to_redis_market()`: Ghi market data từ Spark
- `write_to_redis_fraud()`: Ghi fraud predictions từ Spark
- `write_market_statistics()`: Ghi market statistics
- `write_fraud_statistics()`: Ghi fraud statistics

**Sử dụng trong Spark:**
```python
from redis_sink import write_to_redis_market

# Trong Spark Streaming
def write_batch(batch_df, batch_id):
    batch_df.foreachPartition(write_to_redis_market)

stream.writeStream \
    .foreachBatch(write_batch) \
    .start()
```

## Redis Data Schema

### Market Data Keys

```
# Individual stock data (expires in 24h)
market:stock:{symbol}:{timestamp}
{
    "symbol": "VCB",
    "price": 90200,
    "change_pct": 1.32,
    "volume": 1000000,
    "volatility_pct": 1.8,
    "risk_score": 1.2,
    "timestamp": "2025-11-15T10:00:00"
}

# Latest stock data (expires in 5min)
market:latest:{symbol}
{...same as above...}

# Market alerts (expires in 24h)
market:alert:{symbol}:{timestamp}
{
    "symbol": "VCB",
    "risk_score": 2.5,
    "risk_level": "HIGH",
    "message": "High volatility detected",
    "timestamp": "2025-11-15T10:00:00"
}

# Recent alerts sorted set
market:alerts:recent
(sorted by timestamp, keeps last 100)

# Market statistics (expires in 5min)
market:statistics
{
    "total_stocks": 10,
    "high_risk_count": 2,
    "avg_volatility": 2.5,
    "updated_at": "2025-11-15T10:00:00"
}

# Per-symbol statistics (expires in 5min)
market:stats:{symbol}
{
    "window_start": "2025-11-15T10:00:00",
    "avg_price": 90200,
    "total_volume": 5000000,
    "avg_risk_score": 1.5
}
```

### Fraud Detection Keys

```
# Individual transactions (expires in 24h)
fraud:transaction:{tx_id}
{
    "tx_id": 12345,
    "tx_type": "TRANSFER",
    "amount": 150000,
    "from_account": "C123456",
    "to_account": "C789012",
    "predicted_fraud": 0,
    "fraud_prob_pct": 12.5,
    "transaction_time": "2025-11-15T10:00:00"
}

# Recent transactions sorted set
fraud:transactions:recent
(sorted by timestamp, keeps last 200)

# Fraud alerts (expires in 24h)
fraud:alert:{tx_id}
{
    "tx_id": 12345,
    "tx_type": "CASH_OUT",
    "amount": 500000,
    "fraud_prob_pct": 87.3,
    "timestamp": "2025-11-15T10:00:00"
}

# Recent fraud alerts sorted set
fraud:alerts:recent
(sorted by timestamp, keeps last 100)

# Fraud statistics (expires in 5min)
fraud:statistics
{
    "total_transactions": 1000,
    "total_frauds": 10,
    "accuracy_pct": 96.7,
    "updated_at": "2025-11-15T10:00:00"
}

# Fraud counters (persistent)
fraud:counter:total           # Total fraud count
fraud:counter:type:TRANSFER   # Fraud count by type
fraud:counter:type:CASH_OUT
```

## Cài đặt

### 1. Kiểm tra Redis đang chạy

```bash
# Trong Docker
docker ps | grep redis

# Test kết nối
docker exec -it redis redis-cli ping
# Kết quả: PONG
```

### 2. Chạy script kiểm tra

```bash
cd /home/jovyan/work/redis
./start_redis.sh
```

Script sẽ:
- Kiểm tra Redis container
- Test kết nối
- Hiển thị thông tin Redis
- Tùy chọn clear dữ liệu cũ

## Sử dụng với Spark Streaming

### Market Risk Consumer

File: `/home/jovyan/work/market_risk/spark_consumer.py`

```python
# Đã tích hợp Redis sink
# Dữ liệu tự động được lưu vào Redis mỗi 15s
```

### Fraud Detection Consumer

File: `/home/jovyan/work/fraud_detection/realtime_consumer.py`

```python
# Đã tích hợp Redis sink
# Predictions tự động được lưu vào Redis mỗi 5s
```

## Sử dụng với Dashboard

File: `/home/jovyan/work/dashboard.py`

```python
# Dashboard tự động kết nối Redis
# Auto-refresh mỗi 5 giây
# Không cần cấu hình thêm
```

## Redis CLI Commands

### Xem dữ liệu

```bash
# Vào Redis CLI
docker exec -it redis redis-cli

# Xem tất cả keys
KEYS *

# Xem market keys
KEYS market:*

# Xem fraud keys
KEYS fraud:*

# Lấy giá trị
GET market:latest:VCB

# Xem sorted set (recent alerts)
ZRANGE market:alerts:recent 0 -1

# Đếm số keys
DBSIZE

# Xem thông tin
INFO

# Xem memory usage
INFO memory
```

### Quản lý dữ liệu

```bash
# Xóa tất cả dữ liệu
FLUSHDB

# Xóa keys theo pattern
KEYS "market:stock:*" | xargs redis-cli DEL

# Set expiry cho key
EXPIRE market:latest:VCB 300

# Xem TTL của key
TTL market:latest:VCB

# Xóa một key
DEL market:latest:VCB
```

## Monitoring

### 1. Theo dõi real-time

```bash
# Monitor commands real-time
docker exec -it redis redis-cli MONITOR

# Stats real-time
docker exec -it redis redis-cli --stat

# Latency monitoring
docker exec -it redis redis-cli --latency
```

### 2. Python monitoring

```python
from redis_helper import get_redis_helper

redis = get_redis_helper()
stats = redis.get_stats()
print(f"Memory: {stats['used_memory']}")
print(f"Keys: {stats['total_keys']}")
print(f"Clients: {stats['connected_clients']}")
```

## Troubleshooting

### Lỗi: Connection refused

```bash
# Kiểm tra Redis
docker ps | grep redis

# Restart Redis
docker-compose restart redis

# Xem logs
docker logs redis
```

### Lỗi: Out of Memory

```bash
# Xem memory usage
docker exec -it redis redis-cli INFO memory

# Clear old data
docker exec -it redis redis-cli FLUSHDB

# Hoặc increase memory limit trong docker-compose.yml
```

### Lỗi: Too many keys

```python
# Sử dụng clear_old_data trong redis_helper
from redis_helper import get_redis_helper

redis = get_redis_helper()
deleted = redis.clear_old_data('market:stock:*', keep_count=1000)
print(f"Deleted {deleted} old keys")
```

## Best Practices

1. **Expiry Times**: 
   - Real-time data: 5-10 minutes
   - Historical data: 24 hours
   - Statistics: 5 minutes

2. **Key Naming**:
   - Use namespace prefixes (market:, fraud:)
   - Include timestamps for versioning
   - Use sorted sets for time-series

3. **Memory Management**:
   - Set appropriate expiry times
   - Use sorted sets with ZREMRANGEBYRANK
   - Regular cleanup of old data

4. **Performance**:
   - Use pipelines for bulk operations
   - Use connection pooling
   - Monitor memory usage

## Integration Flow

```
┌─────────────────┐
│  Kafka Topics   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Spark Streaming │
│   Consumers     │
└────────┬────────┘
         │
         ▼
    redis_sink.py
         │
         ▼
┌─────────────────┐
│  Redis Database │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Dashboard.py  │
│  (Auto-refresh) │
└─────────────────┘
```

## Thông tin thêm

- **Redis Version**: 7-alpine
- **Host**: redis (trong Docker network)
- **Port**: 6379
- **Database**: 0
- **Persistence**: RDB snapshots (mặc định)

## Liên hệ & Support

Nếu có vấn đề, check:
1. Docker logs: `docker logs redis`
2. Redis CLI: `docker exec -it redis redis-cli`
3. Test connection: `redis.ping()` trong Python
