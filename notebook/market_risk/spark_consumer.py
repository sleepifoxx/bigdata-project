"""
Fixed Spark Consumer - Vietnamese Stock Market Data
Với column mapping chính xác
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.types import *
import logging
import json
import time

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "vietnam_stocks"
CHECKPOINT_DIR = "/tmp/spark-checkpoint-market-fixed"

# Risk calculation functions
def calculate_liquidity_risk_score(liquidity_value):
    """Calculate liquidity risk score (1-3) based on value in billions VND"""
    if liquidity_value < 10:  # < 10 billion VND
        return 3  # High risk
    elif liquidity_value < 50:  # 10-50 billion VND
        return 2  # Medium risk
    else:  # > 50 billion VND
        return 1  # Low risk

def calculate_spread_risk_score(spread_percent):
    """Calculate spread risk score (1-3) based on spread percentage"""
    if spread_percent > 1.0:  # > 1%
        return 3  # High risk
    elif spread_percent > 0.5:  # 0.5-1%
        return 2  # Medium risk
    else:  # < 0.5%
        return 1  # Low risk

def get_price_risk_position(price_position_pct):
    """Determine price risk position based on price position percentage"""
    if price_position_pct > 95:
        return "OVERBOUGHT"
    elif price_position_pct < 5:
        return "OVERSOLD"
    else:
        return "NORMAL"

def get_foreign_flow_risk(foreign_net, total_volume):
    """Determine foreign flow risk based on net volume"""
    if foreign_net < -1000000:
        return "HEAVY_SELL"
    elif foreign_net < -100000:
        return "SELLING"
    elif foreign_net > 1000000:
        return "HEAVY_BUY"
    elif foreign_net > 100000:
        return "BUYING"
    else:
        return "NEUTRAL"

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
    # Basic calculations
    ).withColumn(
        "price_change", col("price") - col("ref_price")
    ).withColumn(
        "price_change_percent", 
        when(col("ref_price") > 0, 
             (col("price") - col("ref_price")) / col("ref_price") * 100).otherwise(0)
    ).withColumn(
        "foreign_net", col("foreign_buy") - col("foreign_sell")
    ).withColumn(
        "avg_price", 
        when(col("volume") > 0, col("price")).otherwise(col("price"))
    ).withColumn(
        "liquidity_value", col("volume") * col("price") / 1e9  # in billions VND
    ).withColumn(
        "spread", col("ceiling") - col("floor")
    ).withColumn(
        "spread_percent",
        when(col("price") > 0, (col("high") - col("low")) / col("price") * 100).otherwise(0)  # Daily spread %
    ).withColumn(
        "price_position_pct",
        when((col("ceiling") - col("floor")) > 0,
             (col("price") - col("floor")) / (col("ceiling") - col("floor")) * 100).otherwise(50)
    # Market Metrics
    ).withColumn(
        "volatility_intraday",
        when(col("ref_price") > 0,
             (col("high") - col("low")) / col("ref_price") * 100).otherwise(0)
    ).withColumn(
        "price_momentum",
        when(col("ref_price") > 0,
             (col("price") - col("ref_price")) / col("ref_price") * 100).otherwise(0)
    ).withColumn(
        "foreign_pressure",
        when(col("volume") > 0, col("foreign_net") / col("volume") * 100).otherwise(0)
    ).withColumn(
        "liquidity_score", col("volume") / 1e6  # in millions of shares
    # VaR Metrics (assuming 1000 shares position)
    ).withColumn(
        "max_loss_potential", (col("price") - col("floor")) * 1000
    ).withColumn(
        "max_gain_potential", (col("ceiling") - col("price")) * 1000
    ).withColumn(
        "downside_risk_pct",
        when(col("price") > 0, (col("price") - col("floor")) / col("price") * 100).otherwise(0)
    ).withColumn(
        "upside_potential_pct",
        when(col("price") > 0, (col("ceiling") - col("price")) / col("price") * 100).otherwise(0)
    ).withColumn(
        "risk_reward_ratio",
        when(col("downside_risk_pct") > 0, 
             col("upside_potential_pct") / col("downside_risk_pct")).otherwise(0)
    # Risk Classifications
    ).withColumn(
        "volatility_risk_level",
        when(col("volatility_intraday") > 5, "HIGH")
        .when(col("volatility_intraday") > 2, "MEDIUM")
        .otherwise("LOW")
    ).withColumn(
        "liquidity_risk_score",
        when(col("liquidity_value") < 0.1, 3)  # < 100 million VND (very low)
        .when(col("liquidity_value") < 1.0, 2)  # < 1 billion VND (low)
        .otherwise(1)                            # >= 1 billion VND (normal)
    ).withColumn(
        "spread_risk_score",
        when(col("spread_percent") > 5.0, 3)  # > 5% daily spread (high volatility)
        .when(col("spread_percent") > 2.0, 2)  # > 2% daily spread (medium volatility)
        .otherwise(1)                          # <= 2% daily spread (normal)
    ).withColumn(
        "price_risk_position",
        when(col("price_position_pct") > 95, "OVERBOUGHT")
        .when(col("price_position_pct") < 5, "OVERSOLD")
        .otherwise("NORMAL")
    ).withColumn(
        "foreign_flow_risk",
        when(col("foreign_net") < -1000000, "HEAVY_SELL")
        .when(col("foreign_net") < -100000, "SELLING")
        .when(col("foreign_net") > 1000000, "HEAVY_BUY")
        .when(col("foreign_net") > 100000, "BUYING")
        .otherwise("NEUTRAL")
    ).withColumn(
        "composite_risk_score",
        # Comprehensive weighted risk calculation (1.0 to 3.0 scale)
        (
            col("liquidity_risk_score") * 0.4 +          # Liquidity is most important
            col("spread_risk_score") * 0.3 +             # Spread risk
            when(col("volatility_intraday") > 3.0, 3.0)  # Volatility component
            .when(col("volatility_intraday") > 1.5, 2.0)
            .otherwise(1.0) * 0.2 +
            when(spark_abs(col("price_change_percent")) > 7.0, 3.0)  # Price change component
            .when(spark_abs(col("price_change_percent")) > 3.0, 2.0)
            .otherwise(1.0) * 0.1
        )
    )

    # Final output selection with comprehensive metrics
    final_output = processed_df.select(
        col("listing_symbol").alias("symbol"),
        col("company_name"),
        
        # Price & Basic Info
        round(col("price"), 2).alias("price"),
        round(col("price_change"), 2).alias("price_change"),
        round(col("price_change_percent"), 2).alias("change_pct"),
        col("volume"),
        round(col("ref_price"), 2).alias("ref_price"),
        round(col("ceiling"), 2).alias("ceiling"),
        round(col("floor"), 2).alias("floor"),
        round(col("high"), 2).alias("high"),
        round(col("low"), 2).alias("low"),
        
        # Market Metrics
        round(col("volatility_intraday"), 2).alias("volatility_pct"),
        round(col("price_momentum"), 2).alias("price_momentum"),
        round(col("liquidity_value"), 2).alias("liquidity_value"),
        round(col("spread_percent"), 2).alias("spread_percent"),
        round(col("price_position_pct"), 1).alias("price_position_pct"),
        round(col("foreign_pressure"), 2).alias("foreign_pressure"),
        round(col("liquidity_score"), 2).alias("liquidity_score"),
        
        # Foreign Flow
        col("foreign_buy"),
        col("foreign_sell"),
        col("foreign_net"),
        
        # VaR Metrics
        round(col("max_loss_potential"), 0).alias("max_loss_potential"),
        round(col("max_gain_potential"), 0).alias("max_gain_potential"),
        round(col("downside_risk_pct"), 2).alias("downside_risk_pct"),
        round(col("upside_potential_pct"), 2).alias("upside_potential_pct"),
        round(col("risk_reward_ratio"), 2).alias("risk_reward_ratio"),
        
        # Risk Classifications
        col("volatility_risk_level"),
        col("liquidity_risk_score"),
        col("spread_risk_score"),
        col("price_risk_position"),
        col("foreign_flow_risk"),
        round(col("composite_risk_score"), 2).alias("composite_risk_score")
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
                
                # Prepare comprehensive data structure
                data = {
                    # Basic Info
                    'symbol': row_dict.get('symbol', 'UNKNOWN'),
                    'company_name': row_dict.get('company_name', 'N/A'),
                    'price': float(row_dict.get('price', 0)) if row_dict.get('price') is not None else 0.0,
                    'price_change': float(row_dict.get('price_change', 0)) if row_dict.get('price_change') is not None else 0.0,
                    'change_pct': float(row_dict.get('change_pct', 0)) if row_dict.get('change_pct') is not None else 0.0,
                    'volume': int(row_dict.get('volume', 0)) if row_dict.get('volume') is not None else 0,
                    'ref_price': float(row_dict.get('ref_price', 0)) if row_dict.get('ref_price') is not None else 0.0,
                    'ceiling': float(row_dict.get('ceiling', 0)) if row_dict.get('ceiling') is not None else 0.0,
                    'floor': float(row_dict.get('floor', 0)) if row_dict.get('floor') is not None else 0.0,
                    'high': float(row_dict.get('high', 0)) if row_dict.get('high') is not None else 0.0,
                    'low': float(row_dict.get('low', 0)) if row_dict.get('low') is not None else 0.0,
                    
                    # Market Metrics
                    'volatility_pct': float(row_dict.get('volatility_pct', 0)) if row_dict.get('volatility_pct') is not None else 0.0,
                    'price_momentum': float(row_dict.get('price_momentum', 0)) if row_dict.get('price_momentum') is not None else 0.0,
                    'liquidity_value': float(row_dict.get('liquidity_value', 0)) if row_dict.get('liquidity_value') is not None else 0.0,
                    'spread_percent': float(row_dict.get('spread_percent', 0)) if row_dict.get('spread_percent') is not None else 0.0,
                    'price_position_pct': float(row_dict.get('price_position_pct', 50)) if row_dict.get('price_position_pct') is not None else 50.0,
                    'foreign_pressure': float(row_dict.get('foreign_pressure', 0)) if row_dict.get('foreign_pressure') is not None else 0.0,
                    'liquidity_score': float(row_dict.get('liquidity_score', 0)) if row_dict.get('liquidity_score') is not None else 0.0,
                    
                    # Foreign Flow
                    'foreign_buy': int(row_dict.get('foreign_buy', 0)) if row_dict.get('foreign_buy') is not None else 0,
                    'foreign_sell': int(row_dict.get('foreign_sell', 0)) if row_dict.get('foreign_sell') is not None else 0,
                    'foreign_net': int(row_dict.get('foreign_net', 0)) if row_dict.get('foreign_net') is not None else 0,
                    
                    # VaR Metrics
                    'max_loss_potential': float(row_dict.get('max_loss_potential', 0)) if row_dict.get('max_loss_potential') is not None else 0.0,
                    'max_gain_potential': float(row_dict.get('max_gain_potential', 0)) if row_dict.get('max_gain_potential') is not None else 0.0,
                    'downside_risk_pct': float(row_dict.get('downside_risk_pct', 0)) if row_dict.get('downside_risk_pct') is not None else 0.0,
                    'upside_potential_pct': float(row_dict.get('upside_potential_pct', 0)) if row_dict.get('upside_potential_pct') is not None else 0.0,
                    'risk_reward_ratio': float(row_dict.get('risk_reward_ratio', 0)) if row_dict.get('risk_reward_ratio') is not None else 0.0,
                    
                    # Risk Classifications
                    'volatility_risk_level': row_dict.get('volatility_risk_level', 'LOW'),
                    'liquidity_risk_score': int(row_dict.get('liquidity_risk_score', 1)) if row_dict.get('liquidity_risk_score') is not None else 1,
                    'spread_risk_score': int(row_dict.get('spread_risk_score', 1)) if row_dict.get('spread_risk_score') is not None else 1,
                    'price_risk_position': row_dict.get('price_risk_position', 'NORMAL'),
                    'foreign_flow_risk': row_dict.get('foreign_flow_risk', 'NEUTRAL'),
                    'composite_risk_score': float(row_dict.get('composite_risk_score', 1)) if row_dict.get('composite_risk_score') is not None else 1.0,
                    
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
                
                # Track high-risk symbols (composite_risk_score > 2.0)
                if data['composite_risk_score'] > 2.0:
                    risk_key = f"market:high_risk:{data['symbol']}"
                    risk_data = {
                        'symbol': data['symbol'],
                        'composite_risk_score': data['composite_risk_score'],
                        'volatility_risk_level': data['volatility_risk_level'],
                        'price_risk_position': data['price_risk_position'],
                        'foreign_flow_risk': data['foreign_flow_risk'],
                        'liquidity_value': data['liquidity_value'],
                        'risk_reward_ratio': data['risk_reward_ratio'],
                        'timestamp': ts
                    }
                    redis_client.setex(risk_key, 3600, json.dumps(risk_data))  # 1h expiry
                    redis_client.zadd('market:high_risk_sorted', {data['symbol']: data['composite_risk_score']})
                
                # Market statistics
                redis_client.hset('market:stats', mapping={
                    f"symbol_count": redis_client.scard('market:active_symbols'),
                    f"last_update": ts,
                    f"high_risk_count": redis_client.zcount('market:high_risk_sorted', 2.0, '+inf')
                })
                
                print(f"✓ Redis: {data['symbol']} | Price: {data['price']} | Risk: {data['composite_risk_score']:.2f} | RR: {data['risk_reward_ratio']:.2f} | Change: {data['change_pct']}%")
                
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