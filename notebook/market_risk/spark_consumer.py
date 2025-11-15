"""
Fixed Spark Consumer - Vietnamese Stock Market Data
Với column mapping chính xác
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import json
import time

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "vietnam_stocks"
CHECKPOINT_DIR = "/tmp/spark-checkpoint-market-fixed"

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Spark session
spark = SparkSession.builder \
    .appName("FixedStockConsumer") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def main():
    logger.info("=" * 60)
    logger.info("FIXED SPARK STREAMING CONSUMER - VIETNAM STOCK MARKET")
    logger.info("=" * 60)
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    logger.info(f"Connected to Kafka topic: {KAFKA_TOPIC}")

    # Parse JSON data with correct structure
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("symbol_key"),
        from_json(col("value").cast("string"), 
                 StructType([
                     StructField("timestamp", StringType(), True),
                     StructField("data", MapType(StringType(), StringType()), True)
                 ])).alias("json_data")
    ).select(
        col("symbol_key"),
        col("json_data.timestamp").alias("timestamp"),
        col("json_data.data").alias("data")
    )

    # Extract fields with correct column names
    processed_df = parsed_df.select(
        col("symbol_key").alias("symbol"),
        col("timestamp"),
        col("data.listing_symbol").alias("listing_symbol"),
        col("data.listing_organ_name").alias("company_name"),
        col("data.match_match_price").cast("double").alias("price"),
        col("data.match_match_vol").cast("double").alias("volume"),
        col("data.match_reference_price").cast("double").alias("ref_price"),
        col("data.match_ceiling_price").cast("double").alias("ceiling"),
        col("data.match_floor_price").cast("double").alias("floor"),
        col("data.match_highest").cast("double").alias("high"),
        col("data.match_lowest").cast("double").alias("low"),
        col("data.match_foreign_buy_volume").cast("double").alias("foreign_buy"),
        col("data.match_foreign_sell_volume").cast("double").alias("foreign_sell")
    ).withColumn(
        "price_change", col("price") - col("ref_price")
    ).withColumn(
        "price_change_percent", 
        when(col("ref_price") > 0, 
             (col("price") - col("ref_price")) / col("ref_price") * 100).otherwise(0)
    ).withColumn(
        "foreign_net", col("foreign_buy") - col("foreign_sell")
    ).withColumn(
        "volatility_pct",
        when(col("ref_price") > 0,
             (col("high") - col("low")) / col("ref_price") * 100).otherwise(0)
    ).withColumn(
        "volatility_risk_level",
        when(col("volatility_pct") > 5, "HIGH")
        .when(col("volatility_pct") > 2, "MEDIUM")
        .otherwise("LOW")
    ).withColumn(
        "foreign_flow_risk",
        when(col("foreign_net") < -1000000, "OUTFLOW")
        .when(col("foreign_net") > 1000000, "INFLOW")
        .otherwise("NEUTRAL")
    ).withColumn(
        "risk_score",
        col("volatility_pct") * 0.6 + abs(col("price_change_percent")) * 0.4
    ).withColumn(
        "price_risk_position",
        when(col("risk_score") > 8, "HIGH_RISK")
        .when(col("risk_score") > 5, "MEDIUM_RISK")
        .when(col("risk_score") > 2, "LOW_RISK")
        .otherwise("SAFE")
    )

    # Final output selection
    final_output = processed_df.select(
        col("listing_symbol").alias("symbol"),  # Use direct column mapping
        col("company_name"),
        col("price"),
        round(col("price_change_percent"), 2).alias("change_pct"),
        col("volume"),
        col("volatility_pct"),
        col("volatility_risk_level"),
        col("foreign_net"),
        col("foreign_flow_risk"),
        round(col("risk_score"), 2).alias("risk_score"),
        col("price_risk_position")
    )

    # Redis output with embedded function
    def write_batch_to_redis(batch_df, batch_id):
        """Embedded function to write batch data to Redis"""
        import redis
        import json
        import time
        
        # Redis connection in worker process
        redis_client = redis.Redis(
            host='redis',
            port=6379,
            db=0,
            decode_responses=True
        )
        
        # Process each row in the batch
        for row in batch_df.collect():
            try:
                # Convert row to dictionary for safe access
                row_dict = row.asDict() if hasattr(row, 'asDict') else {}
                
                # Skip invalid rows
                if not row_dict.get('symbol') or row_dict.get('symbol') == 'null':
                    print(f"Skipping invalid row: {row_dict}")
                    continue
                
                # Prepare data structure with safe null checks
                data = {
                    'symbol': row_dict.get('symbol', 'UNKNOWN'),
                    'company_name': row_dict.get('company_name', 'N/A'),
                    'price': float(row_dict.get('price', 0)) if row_dict.get('price') is not None else 0.0,
                    'change_pct': float(row_dict.get('change_pct', 0)) if row_dict.get('change_pct') is not None else 0.0,
                    'volume': int(row_dict.get('volume', 0)) if row_dict.get('volume') is not None else 0,
                    'volatility_pct': float(row_dict.get('volatility_pct', 0)) if row_dict.get('volatility_pct') is not None else 0.0,
                    'volatility_risk_level': row_dict.get('volatility_risk_level', 'LOW'),
                    'foreign_net': int(row_dict.get('foreign_net', 0)) if row_dict.get('foreign_net') is not None else 0,
                    'foreign_flow_risk': row_dict.get('foreign_flow_risk', 'NEUTRAL'),
                    'risk_score': float(row_dict.get('risk_score', 0)) if row_dict.get('risk_score') is not None else 0.0,
                    'price_risk_position': row_dict.get('price_risk_position', 'SAFE'),
                    'timestamp': int(time.time() * 1000)
                }
                
                # Save to Redis with timestamped key
                ts = int(time.time() * 1000)
                key = f"market:stock:{data['symbol']}:{ts}"
                redis_client.setex(key, 86400, json.dumps(data))  # 24h expiry
                
                # Update latest snapshot
                latest_key = f"market:latest:{data['symbol']}"
                redis_client.setex(latest_key, 86400, json.dumps(data))
                
                # Add to active symbols set
                redis_client.sadd("market:active_symbols", data['symbol'])
                
                print(f"✓ Redis: Saved {data['symbol']} - Price: {data['price']} - Change: {data['change_pct']}%")
                
            except Exception as e:
                import traceback
                print(f"Redis Write Error: {str(e)}")
                print(f"Full traceback: {traceback.format_exc()}")
                continue

    # Redis output stream
    redis_query = final_output \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_batch_to_redis) \
        .option("checkpointLocation", CHECKPOINT_DIR + "_redis") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Console output for monitoring
    console_query = final_output \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime="15 seconds") \
        .start()

    logger.info("Both Redis and console output streams started")
    logger.info("Streaming queries are running. Press Ctrl+C to stop.")

    # Wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Stopping streaming queries...")
        redis_query.stop()
        console_query.stop()
        logger.info("All queries stopped")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()