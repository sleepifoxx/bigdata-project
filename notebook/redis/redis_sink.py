"""
Redis Sink for Spark Streaming
Ghi dữ liệu từ Spark Streaming vào Redis
"""

import json
import redis


def write_to_redis_market(partition):
    """
    Function để ghi từng partition của market data vào Redis
    Sử dụng với foreachPartition
    """
    redis_client = redis.Redis(
        host='redis',
        port=6379,
        db=0,
        decode_responses=True
    )
    
    for row in partition:
        try:
            # Prepare data
            data = {
                'symbol': row.symbol,
                'price': float(row.price) if row.price else None,
                'change_pct': float(row.change_pct) if row.change_pct else None,
                'volume': int(row.volume) if row.volume else None,
                'liquidity_value': float(row.liquidity_value) if row.liquidity_value else None,
                'volatility_pct': float(row.volatility_pct) if row.volatility_pct else None,
                'volatility_risk_level': row.volatility_risk_level,
                'foreign_net': int(row.foreign_net) if row.foreign_net else None,
                'foreign_flow_risk': row.foreign_flow_risk,
                'spread_pct': float(row.spread_pct) if row.spread_pct else None,
                'risk_score': float(row.risk_score) if row.risk_score else None,
                'price_risk_position': row.price_risk_position,
                'risk_reward': float(row.risk_reward) if row.risk_reward else None,
                'timestamp': row.timestamp,
                'company_name': row.company_name
            }
            
            # Save to Redis
            import time
            ts = int(time.time() * 1000)
            key = f"market:stock:{row.symbol}:{ts}"
            redis_client.setex(key, 86400, json.dumps(data))  # 24h expiry
            
            # Update latest
            latest_key = f"market:latest:{row.symbol}"
            redis_client.setex(latest_key, 300, json.dumps(data))  # 5min
            
            # Check for high risk and create alert
            if row.risk_score and float(row.risk_score) > 2.0:
                alert_key = f"market:alert:{row.symbol}:{ts}"
                alert = {
                    'symbol': row.symbol,
                    'risk_score': float(row.risk_score),
                    'risk_level': 'HIGH' if float(row.risk_score) > 2.5 else 'MEDIUM',
                    'message': f"High risk detected: {row.volatility_risk_level} volatility, {row.foreign_flow_risk} foreign flow",
                    'volatility': float(row.volatility_pct) if row.volatility_pct else None,
                    'timestamp': row.timestamp
                }
                redis_client.setex(alert_key, 86400, json.dumps(alert))
                redis_client.zadd('market:alerts:recent', {alert_key: ts})
                
        except Exception as e:
            print(f"Error writing to Redis: {e}")
    
    redis_client.close()


def write_to_redis_fraud(partition):
    """
    Function để ghi từng partition của fraud data vào Redis
    Sử dụng với foreachPartition
    """
    redis_client = redis.Redis(
        host='redis',
        port=6379,
        db=0,
        decode_responses=True
    )
    
    for row in partition:
        try:
            # Prepare transaction data
            tx_data = {
                'tx_id': int(row.tx_id) if row.tx_id else None,
                'transaction_time': row.transaction_time,
                'tx_type': row.tx_type,
                'amount': float(row.amount) if row.amount else None,
                'from_account': row.from_account,
                'to_account': row.to_account,
                'actual_fraud': int(row.actual_fraud) if row.actual_fraud else 0,
                'predicted_fraud': int(row.predicted_fraud) if row.predicted_fraud else 0,
                'fraud_prob_pct': float(row.fraud_prob_pct) if row.fraud_prob_pct else None
            }
            
            # Save transaction
            import time
            ts = int(time.time() * 1000)
            tx_key = f"fraud:transaction:{row.tx_id}"
            redis_client.setex(tx_key, 86400, json.dumps(tx_data))  # 24h
            
            # Add to recent transactions
            redis_client.zadd('fraud:transactions:recent', {tx_key: ts})
            redis_client.zremrangebyrank('fraud:transactions:recent', 0, -201)  # Keep 200
            
            # If fraud detected, create alert
            if row.predicted_fraud and int(row.predicted_fraud) == 1:
                alert_key = f"fraud:alert:{row.tx_id}"
                alert = {
                    'tx_id': int(row.tx_id),
                    'tx_type': row.tx_type,
                    'amount': float(row.amount) if row.amount else None,
                    'from_account': row.from_account,
                    'to_account': row.to_account,
                    'fraud_prob_pct': float(row.fraud_prob_pct) if row.fraud_prob_pct else None,
                    'transaction_time': row.transaction_time,
                    'timestamp': str(time.time())
                }
                redis_client.setex(alert_key, 86400, json.dumps(alert))
                redis_client.zadd('fraud:alerts:recent', {alert_key: ts})
                redis_client.zremrangebyrank('fraud:alerts:recent', 0, -101)  # Keep 100
                
                # Increment counters
                redis_client.incr('fraud:counter:total')
                redis_client.incr(f'fraud:counter:type:{row.tx_type}')
                
        except Exception as e:
            print(f"Error writing fraud to Redis: {e}")
    
    redis_client.close()


def write_market_statistics(partition):
    """Ghi thống kê market vào Redis"""
    redis_client = redis.Redis(
        host='redis',
        port=6379,
        db=0,
        decode_responses=True
    )
    
    for row in partition:
        try:
            stats = {
                'window_start': row.window_start,
                'window_end': row.window_end,
                'symbol': row.symbol,
                'record_count': int(row.record_count) if row.record_count else 0,
                'avg_price': float(row.avg_price) if row.avg_price else None,
                'max_price': float(row.max_price) if row.max_price else None,
                'min_price': float(row.min_price) if row.min_price else None,
                'total_volume': int(row.total_volume) if row.total_volume else 0,
                'avg_change_pct': float(row.avg_change_pct) if row.avg_change_pct else None,
                'net_foreign_flow': int(row.net_foreign_flow) if row.net_foreign_flow else 0,
                'avg_volatility_pct': float(row.avg_volatility_pct) if row.avg_volatility_pct else None,
                'avg_risk_score': float(row.avg_risk_score) if row.avg_risk_score else None,
                'volatility_stddev': float(row.volatility_stddev) if row.volatility_stddev else None
            }
            
            # Save per symbol
            key = f"market:stats:{row.symbol}"
            redis_client.setex(key, 300, json.dumps(stats))  # 5min
            
        except Exception as e:
            print(f"Error writing market stats: {e}")
    
    redis_client.close()


def write_fraud_statistics(partition):
    """Ghi thống kê fraud vào Redis"""
    redis_client = redis.Redis(
        host='redis',
        port=6379,
        db=0,
        decode_responses=True
    )
    
    for row in partition:
        try:
            stats = {
                'window_start': row.window_start,
                'window_end': row.window_end,
                'total_transactions': int(row.total_transactions) if row.total_transactions else 0,
                'actual_frauds': int(row.actual_frauds) if row.actual_frauds else 0,
                'predicted_frauds': int(row.predicted_frauds) if row.predicted_frauds else 0,
                'accuracy_pct': float(row.accuracy_pct) if row.accuracy_pct else None,
                'true_positives': int(row.true_positives) if row.true_positives else 0,
                'false_positives': int(row.false_positives) if row.false_positives else 0,
                'false_negatives': int(row.false_negatives) if row.false_negatives else 0,
                'avg_fraud_prob_pct': float(row.avg_fraud_prob_pct) if row.avg_fraud_prob_pct else None
            }
            
            # Save overall stats
            redis_client.setex('fraud:statistics', 300, json.dumps(stats))
            
        except Exception as e:
            print(f"Error writing fraud stats: {e}")
    
    redis_client.close()
