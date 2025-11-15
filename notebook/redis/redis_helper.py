"""
Redis Helper Module
Lưu và quản lý dữ liệu trong Redis cho Market Risk và Fraud Detection
"""

import redis
import json
from datetime import datetime
import time

class RedisHelper:
    """Helper class để tương tác với Redis"""
    
    def __init__(self, host='redis', port=6379, db=0):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )
        # Set default expiry cho keys (24 hours)
        self.default_expiry = 86400
    
    # ========================================================================
    # MARKET DATA METHODS
    # ========================================================================
    
    def save_market_stock(self, symbol, data):
        """Lưu dữ liệu stock vào Redis"""
        timestamp = int(time.time() * 1000)
        key = f"market:stock:{symbol}:{timestamp}"
        
        # Thêm timestamp vào data
        data['timestamp'] = datetime.now().isoformat()
        data['symbol'] = symbol
        
        self.client.setex(
            key,
            self.default_expiry,
            json.dumps(data)
        )
        
        # Cập nhật latest stock data
        latest_key = f"market:latest:{symbol}"
        self.client.setex(latest_key, 300, json.dumps(data))  # 5 minutes
        
        return key
    
    def save_market_alert(self, symbol, risk_score, message, risk_level='MEDIUM'):
        """Lưu cảnh báo rủi ro thị trường"""
        timestamp = int(time.time() * 1000)
        key = f"market:alert:{symbol}:{timestamp}"
        
        alert = {
            'symbol': symbol,
            'risk_score': risk_score,
            'message': message,
            'risk_level': risk_level,
            'timestamp': datetime.now().isoformat()
        }
        
        self.client.setex(
            key,
            self.default_expiry,
            json.dumps(alert)
        )
        
        # Add to sorted set for recent alerts
        self.client.zadd('market:alerts:recent', {key: timestamp})
        self.client.zremrangebyrank('market:alerts:recent', 0, -101)  # Keep last 100
        
        return key
    
    def save_market_statistics(self, stats):
        """Lưu thống kê thị trường"""
        stats['updated_at'] = datetime.now().isoformat()
        self.client.setex(
            'market:statistics',
            300,  # 5 minutes
            json.dumps(stats)
        )
    
    def get_market_latest_stocks(self, limit=100):
        """Lấy dữ liệu stock mới nhất"""
        keys = self.client.keys('market:latest:*')
        stocks = []
        
        for key in keys[:limit]:
            data = self.client.get(key)
            if data:
                stocks.append(json.loads(data))
        
        return stocks
    
    def get_market_alerts(self, limit=50):
        """Lấy cảnh báo rủi ro gần nhất"""
        alert_keys = self.client.zrevrange('market:alerts:recent', 0, limit-1)
        alerts = []
        
        for key in alert_keys:
            data = self.client.get(key)
            if data:
                alerts.append(json.loads(data))
        
        return alerts
    
    # ========================================================================
    # FRAUD DETECTION METHODS
    # ========================================================================
    
    def save_transaction(self, tx_id, tx_data):
        """Lưu giao dịch vào Redis"""
        timestamp = int(time.time() * 1000)
        key = f"fraud:transaction:{tx_id}"
        
        tx_data['tx_id'] = tx_id
        tx_data['stored_at'] = datetime.now().isoformat()
        
        self.client.setex(
            key,
            self.default_expiry,
            json.dumps(tx_data)
        )
        
        # Add to sorted set
        self.client.zadd('fraud:transactions:recent', {key: timestamp})
        self.client.zremrangebyrank('fraud:transactions:recent', 0, -201)  # Keep last 200
        
        return key
    
    def save_fraud_alert(self, tx_id, tx_data, fraud_probability):
        """Lưu cảnh báo gian lận"""
        timestamp = int(time.time() * 1000)
        key = f"fraud:alert:{tx_id}"
        
        alert = {
            'tx_id': tx_id,
            'tx_type': tx_data.get('tx_type'),
            'amount': tx_data.get('amount'),
            'from_account': tx_data.get('from_account'),
            'to_account': tx_data.get('to_account'),
            'fraud_prob_pct': fraud_probability,
            'transaction_time': tx_data.get('transaction_time'),
            'timestamp': datetime.now().isoformat()
        }
        
        self.client.setex(
            key,
            self.default_expiry,
            json.dumps(alert)
        )
        
        # Add to sorted set
        self.client.zadd('fraud:alerts:recent', {key: timestamp})
        self.client.zremrangebyrank('fraud:alerts:recent', 0, -101)  # Keep last 100
        
        # Increment fraud counter
        self.client.incr('fraud:counter:total')
        
        # Increment by type
        tx_type = tx_data.get('tx_type', 'UNKNOWN')
        self.client.incr(f'fraud:counter:type:{tx_type}')
        
        return key
    
    def save_fraud_statistics(self, stats):
        """Lưu thống kê fraud detection"""
        stats['updated_at'] = datetime.now().isoformat()
        self.client.setex(
            'fraud:statistics',
            300,  # 5 minutes
            json.dumps(stats)
        )
    
    def get_recent_transactions(self, limit=100):
        """Lấy giao dịch gần nhất"""
        tx_keys = self.client.zrevrange('fraud:transactions:recent', 0, limit-1)
        transactions = []
        
        for key in tx_keys:
            data = self.client.get(key)
            if data:
                transactions.append(json.loads(data))
        
        return transactions
    
    def get_fraud_alerts(self, limit=50):
        """Lấy cảnh báo gian lận gần nhất"""
        alert_keys = self.client.zrevrange('fraud:alerts:recent', 0, limit-1)
        alerts = []
        
        for key in alert_keys:
            data = self.client.get(key)
            if data:
                alerts.append(json.loads(data))
        
        return alerts
    
    def get_fraud_statistics(self):
        """Lấy thống kê fraud"""
        stats = self.client.get('fraud:statistics')
        if stats:
            return json.loads(stats)
        
        # Return default stats
        return {
            'total_frauds': int(self.client.get('fraud:counter:total') or 0),
            'updated_at': datetime.now().isoformat()
        }
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def clear_old_data(self, pattern, keep_count=1000):
        """Xóa dữ liệu cũ để tránh tràn bộ nhớ"""
        keys = self.client.keys(pattern)
        if len(keys) > keep_count:
            # Sort by timestamp in key name and delete oldest
            sorted_keys = sorted(keys)
            delete_count = len(keys) - keep_count
            for key in sorted_keys[:delete_count]:
                self.client.delete(key)
            return delete_count
        return 0
    
    def get_stats(self):
        """Lấy thống kê Redis"""
        info = self.client.info()
        return {
            'used_memory': info.get('used_memory_human'),
            'total_keys': self.client.dbsize(),
            'connected_clients': info.get('connected_clients')
        }
    
    def ping(self):
        """Kiểm tra kết nối Redis"""
        try:
            return self.client.ping()
        except:
            return False


# Singleton instance
_redis_helper = None

def get_redis_helper():
    """Get singleton Redis helper instance"""
    global _redis_helper
    if _redis_helper is None:
        _redis_helper = RedisHelper()
    return _redis_helper
