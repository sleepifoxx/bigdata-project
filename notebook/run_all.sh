#!/bin/bash

# Script to run all components of the Real-time Market Risk & Fraud Detection System
# Usage: ./run_all.sh
# Tác giả: Hệ thống Giám sát Thị trường & Gian lận

echo "🚀 Starting Real-time Market Risk & Fraud Detection System..."
echo "============================================================"

# Function to check if a process is running
check_process() {
    if pgrep -f "$1" > /dev/null; then
        echo "✅ $1 is already running"
        return 0
    else
        echo "❌ $1 is not running"
        return 1
    fi
}

# Function to start a process in background
start_process() {
    local script_path=$1
    local process_name=$2
    local log_file=$3
    
    echo "🔄 Starting $process_name..."
    
    # Check if already running
    if check_process "$script_path"; then
        echo "⚠️  $process_name is already running, skipping..."
        return 0
    fi
    
    # Start the process
    cd "$(dirname "$script_path")"
    nohup python "$(basename "$script_path")" > "$log_file" 2>&1 &
    local pid=$!
    
    # Wait a moment to check if process started successfully
    sleep 2
    
    if kill -0 $pid 2>/dev/null; then
        echo "✅ $process_name started successfully (PID: $pid)"
        echo "📝 Log file: $log_file"
    else
        echo "❌ Failed to start $process_name"
        return 1
    fi
}

# Create logs directory if it doesn't exist
mkdir -p logs

# Check Redis container connection using Python
echo "🔍 Checking Redis container connection..."
python3 -c "
import redis
import sys
try:
    client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    client.ping()
    print('✅ Redis container is accessible')
except Exception as e:
    print(f'❌ Cannot connect to Redis container: {e}')
    print('🔧 Make sure Redis container is running:')
    print('   docker run -d --name redis -p 6379:6379 redis:alpine')
    print('   or check your docker-compose.yml')
    sys.exit(1)
"

# Start Kafka (if needed - uncomment if using Docker)
# echo "🔍 Checking Kafka status..."
# docker-compose up -d kafka zookeeper

echo ""
echo "🎯 Starting application components..."
echo "=================================="

# 1. Start Fraud Detection Producer
start_process "/home/jovyan/work/fraud_detection/realtime_producer.py" \
    "Fraud Detection Producer" \
    "/home/jovyan/work/logs/fraud_producer.log"

# 2. Start Fraud Detection Consumer  
start_process "/home/jovyan/work/fraud_detection/realtime_consumer.py" \
    "Fraud Detection Consumer" \
    "/home/jovyan/work/logs/fraud_consumer.log"

# 3. Start Market Data Producer
start_process "/home/jovyan/work/market_risk/get_finance_data.py" \
    "Market Data Producer" \
    "/home/jovyan/work/logs/market_producer.log"

# 4. Start Market Risk Consumer (Spark)
start_process "/home/jovyan/work/market_risk/spark_consumer.py" \
    "Market Risk Spark Consumer" \
    "/home/jovyan/work/logs/market_consumer.log"

# Wait a moment for all services to initialize
echo ""
echo "⏳ Waiting for services to initialize..."
sleep 5

# 5. Start Dashboard (foreground)
echo ""
echo "🎨 Starting Dashboard..."
echo "======================"
echo "📱 Dashboard will be available at: http://localhost:8501"
echo "🛑 Press Ctrl+C to stop all services"
echo ""

# Start dashboard in foreground so we can easily stop it
cd /home/jovyan/work
python start_dashboard.py

echo ""
echo "🛑 Dashboard stopped. Cleaning up background processes..."

# Function to stop background processes
stop_processes() {
    echo "🧹 Stopping background processes..."
    
    # Stop specific Python processes
    pkill -f "realtime_producer.py" && echo "✅ Stopped Fraud Producer"
    pkill -f "realtime_consumer.py" && echo "✅ Stopped Fraud Consumer"  
    pkill -f "get_finance_data.py" && echo "✅ Stopped Market Producer"
    pkill -f "spark_consumer.py" && echo "✅ Stopped Market Consumer"
    
    echo "🏁 All processes stopped."
}

# Trap Ctrl+C and cleanup
trap stop_processes EXIT

echo ""
echo "📊 System Status Summary:"
echo "========================"
echo "📝 Check log files in ./logs/ for detailed information"
redis_status=$(python3 -c "
import redis
try:
    client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    client.ping()
    print('Connected')
except:
    print('Not accessible')
" 2>/dev/null)
echo "🔧 Redis status: $redis_status"
echo ""
echo "🎯 Available endpoints:"
echo "  - Dashboard: http://localhost:8501"
echo "  - Redis: redis:6379 (container)"
echo ""
echo "🔍 To monitor processes manually:"
echo "  ps aux | grep python"
echo "  tail -f logs/*.log"
echo ""
echo "Thank you for using Real-time Market Risk & Fraud Detection System! 🚀"