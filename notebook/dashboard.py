"""
Real-time Dashboard - Market Risk & Fraud Detection
Sử dụng Streamlit và Redis để hiển thị thông tin real-time
"""

import streamlit as st
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd
import redis
import json
import time
import sys
import numpy as np

# Page config
st.set_page_config(
    page_title="Bảng Điều Khiển Giám Sát Thị Trường & Gian Lận",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        padding-top: 1rem;
    }
    .stMetric {
        background-color: #262730;
        border: 1px solid #464853;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12), 0 1px 2px rgba(0, 0, 0, 0.24);
        color: white !important;
    }
    .stMetric > div {
        color: white !important;
    }
    .stMetric label {
        color: white !important;
    }
    .stMetric [data-testid="metric-container"] {
        color: white !important;
    }
    .st-emotion-cache-1q82h82 {
        color: white !important;
    }
    .e1wr3kle3 {
        color: white !important;
    }
    .st-emotion-cache-1q82h82 * {
        color: white !important;
    }
    .e1wr3kle3 * {
        color: white !important;
    }
    .fraud-alert {
        background-color: #2d1f1f;
        border: 2px solid #ff4444;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
        animation: slideIn 0.5s ease-out;
    }
    @keyframes slideIn {
        from { transform: translateX(-100%); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Redis connection
@st.cache_resource
def init_redis():
    return redis.Redis(
        host='redis',
        port=6379,
        db=0,
        decode_responses=True
    )

redis_client = init_redis()

# Test Redis connection
def test_redis_connection():
    try:
        redis_client.ping()
        return True
    except Exception as e:
        st.error(f"Redis connection error: {e}")
        return False

# Data fetching functions
@st.cache_data(ttl=10)  # Cache for 10 seconds
def get_transactions_from_redis():
    """Lấy giao dịch gần nhất từ Redis"""
    try:
        # Get recent transactions from sorted set
        recent_keys = redis_client.zrevrange('fraud:transactions:recent', 0, 49)  # Latest 50
        transactions = []
        
        for key in recent_keys:
            value = redis_client.get(key)
            if value:
                data = json.loads(value)
                data['redis_key'] = key
                transactions.append(data)
        
        # If no sorted set data, fallback to pattern search
        if not transactions:
            keys = redis_client.keys('fraud:transaction:*')
            for key in sorted(keys, reverse=True)[:20]:
                value = redis_client.get(key)
                if value:
                    data = json.loads(value)
                    data['redis_key'] = key
                    transactions.append(data)
        
        return transactions
    except Exception as e:
        st.error(f"Error getting transactions: {e}")
        return []

@st.cache_data(ttl=10)
def get_fraud_alerts_from_redis():
    """Lấy cảnh báo gian lận từ Redis - bao gồm cả transactions có fraud probability cao"""
    try:
        alerts = []
        
        # First try to get actual fraud alerts
        alert_keys = redis_client.zrevrange('fraud:alerts:recent', 0, 29)  # Latest 30
        for key in alert_keys:
            value = redis_client.get(key)
            if value:
                data = json.loads(value)
                data['redis_key'] = key
                data['alert_type'] = 'FRAUD_DETECTED'
                alerts.append(data)
        
        # If no dedicated alerts, get transactions with high fraud probability or predictions
        if not alerts:
            # Get recent transactions and filter for suspicious ones
            tx_keys = redis_client.zrevrange('fraud:transactions:recent', 0, 99)  # Latest 100
            for key in tx_keys:
                value = redis_client.get(key)
                if value:
                    data = json.loads(value)
                    # Include if predicted fraud or high probability
                    fraud_prob = data.get('fraud_prob_pct', 0)
                    predicted_fraud = data.get('predicted_fraud', 0)
                    
                    if predicted_fraud == 1 or fraud_prob > 45:  # 45% threshold
                        data['redis_key'] = key
                        data['alert_type'] = 'HIGH_RISK' if fraud_prob > 45 else 'PREDICTED_FRAUD'
                        alerts.append(data)
            
            # Limit to 30 most recent
            alerts = alerts[:30]
        
        # If still no alerts, fallback to pattern search
        if not alerts:
            keys = redis_client.keys('fraud:alert:*')
            for key in sorted(keys, reverse=True)[:15]:
                value = redis_client.get(key)
                if value:
                    data = json.loads(value)
                    data['redis_key'] = key
                    data['alert_type'] = 'FRAUD_DETECTED'
                    alerts.append(data)
        
        return alerts
    except Exception as e:
        st.error(f"Error getting fraud alerts: {e}")
        return []

def get_total_transaction_count():
    """Lấy tổng số giao dịch đã xử lý từ Redis counters"""
    try:
        # Get total count from Redis counter
        total_count = redis_client.get('fraud:counter:total')
        if total_count:
            return int(total_count)
        
        # Fallback: count all transaction keys
        keys = redis_client.keys('fraud:transaction:*')
        return len(keys)
    except Exception as e:
        return 0

@st.cache_data(ttl=10)
def get_fraud_statistics_from_redis():
    """Lấy thống kê fraud từ Redis"""
    try:
        stats = redis_client.get('fraud:statistics')
        return json.loads(stats) if stats else {}
    except Exception as e:
        st.error(f"Error getting fraud stats: {e}")
        return {}

@st.cache_data(ttl=10)
def get_market_data_from_redis(_redis_client):
    """Lấy dữ liệu market từ Redis"""
    try:
        # Get active symbols first
        active_symbols = _redis_client.smembers('market:active_symbols')
        data = []
        
        for symbol in active_symbols:
            # Get latest data for each symbol
            latest_key = f'market:latest:{symbol}'
            value = _redis_client.get(latest_key)
            if value:
                market_data = json.loads(value)
                data.append(market_data)
        
        # If no latest data, fallback to timestamped keys
        if not data:
            keys = _redis_client.keys('market:stock:*')
            for key in sorted(keys, reverse=True)[:50]:
                value = _redis_client.get(key)
                if value:
                    data.append(json.loads(value))
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Error getting market data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=10)
def get_high_risk_stocks(_redis_client):
    """Lấy danh sách cổ phiếu có rủi ro cao"""
    try:
        # Get high risk symbols sorted by risk score
        high_risk_symbols = _redis_client.zrevrange('market:high_risk_sorted', 0, 19, withscores=True)  # Top 20
        high_risk_data = []
        
        for symbol, risk_score in high_risk_symbols:
            risk_key = f'market:high_risk:{symbol}'
            data = _redis_client.get(risk_key)
            if data:
                risk_info = json.loads(data)
                risk_info['risk_score'] = float(risk_score)
                high_risk_data.append(risk_info)
        
        return high_risk_data
    except Exception as e:
        st.error(f"Error getting high risk stocks: {e}")
        return []

@st.cache_data(ttl=10)
def get_market_statistics(_redis_client):
    """Lấy thống kê tổng quan thị trường"""
    try:
        stats = _redis_client.hgetall('market:stats')
        return {
            'symbol_count': int(stats.get('symbol_count', 0)),
            'high_risk_count': int(stats.get('high_risk_count', 0)),
            'last_update': int(stats.get('last_update', 0)) if stats.get('last_update') else 0
        }
    except Exception as e:
        return {'symbol_count': 0, 'high_risk_count': 0, 'last_update': 0}

def create_risk_heatmap(market_data):
    """Tạo heatmap rủi ro theo composite risk score"""
    if market_data.empty:
        fig = go.Figure()
        fig.update_layout(title='Risk Heatmap - No Data', template='plotly_dark')
        return fig
    
    try:
        # Create risk level categories
        market_data['risk_category'] = market_data['composite_risk_score'].apply(
            lambda x: 'HIGH' if x > 2.5 else 'MEDIUM' if x > 1.5 else 'LOW'
        )
        
        # Count by risk category and volatility level
        heatmap_data = market_data.groupby(['volatility_risk_level', 'risk_category']).size().reset_index(name='count')
        
        # Create pivot table for heatmap
        pivot_data = heatmap_data.pivot(index='volatility_risk_level', columns='risk_category', values='count').fillna(0)
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_data.values,
            x=pivot_data.columns,
            y=pivot_data.index,
            colorscale='Reds',
            text=pivot_data.values,
            texttemplate="%{text}",
            textfont={"size": 12}
        ))
        
        fig.update_layout(
            title='Bản Đồ Nhiệt Phân Bố Rủi Ro',
            xaxis_title='Mức Rủi Ro',
            yaxis_title='Mức Biến Động',
            template='plotly_dark'
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating risk heatmap: {e}")
        fig = go.Figure()
        fig.update_layout(title='Risk Heatmap - Error', template='plotly_dark')
        return fig

def create_risk_reward_scatter(market_data):
    """Tạo scatter plot Risk vs Reward"""
    if market_data.empty:
        fig = go.Figure()
        fig.update_layout(title='Risk vs Reward - No Data', template='plotly_dark')
        return fig
    
    try:
        # Filter valid data with more lenient conditions
        valid_data = market_data[
            (market_data['risk_reward_ratio'].notna()) & 
            (market_data['composite_risk_score'].notna()) &
            (market_data['risk_reward_ratio'] >= 0) & 
            (market_data['composite_risk_score'] > 0)
        ]
        
        if valid_data.empty:
            fig = go.Figure()
            fig.update_layout(title='Risk vs Reward - No Valid Data', template='plotly_dark')
            return fig
        
        # Data summary for scatter plot
        # st.write(f"Điểm dữ liệu hợp lệ: {len(valid_data)}, Phạm vi điểm rủi ro: {valid_data['composite_risk_score'].min():.2f}-{valid_data['composite_risk_score'].max():.2f}")
        
        # Color by foreign flow risk
        color_map = {
            'HEAVY_BUY': '#00ff00',
            'BUYING': '#90ee90', 
            'NEUTRAL': '#ffff00',
            'SELLING': '#ffa500',
            'HEAVY_SELL': '#ff0000'
        }
        
        # Handle missing foreign_flow_risk data
        if 'foreign_flow_risk' in valid_data.columns:
            colors = [color_map.get(risk, '#888888') for risk in valid_data['foreign_flow_risk']]
        else:
            colors = ['#888888'] * len(valid_data)
        
        # Calculate marker sizes - scale liquidity appropriately
        liquidity_sizes = valid_data['liquidity_value'] * 100  # Scale up for visibility
        liquidity_sizes = liquidity_sizes.clip(lower=5, upper=50)  # Limit size range
        
        fig = go.Figure(data=go.Scatter(
            x=valid_data['composite_risk_score'],
            y=valid_data['risk_reward_ratio'],
            mode='markers',
            marker=dict(
                size=liquidity_sizes,  # Size by liquidity (scaled)
                color=colors,
                opacity=0.7,
                line=dict(width=1, color='white')
            ),
            text=valid_data['symbol'],
            hovertemplate='<b>%{text}</b><br>Risk Score: %{x:.2f}<br>Risk/Reward: %{y:.2f}<extra></extra>'
        ))
        
        # Add diagonal line for risk/reward = 1
        fig.add_shape(
            type="line",
            x0=0, y0=0, x1=4, y1=1,
            line=dict(color="white", width=2, dash="dash"),
        )
        
        fig.update_layout(
            title='Phân Tích Rủi Ro vs Lợi Nhuận',
            xaxis_title='Điểm Rủi Ro Tổng Hợp',
            yaxis_title='Tỷ Lệ Rủi Ro/Lợi Nhuận',
            template='plotly_dark'
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating risk/reward chart: {e}")
        fig = go.Figure()
        fig.update_layout(title='Risk vs Reward - Error', template='plotly_dark')
        return fig

def create_liquidity_analysis(market_data):
    """Tạo biểu đồ phân tích thanh khoản"""
    if market_data.empty:
        fig = go.Figure()
        fig.update_layout(title='Liquidity Analysis - No Data', template='plotly_dark')
        return fig
    
    try:
        # Bin liquidity values (convert to millions)
        liquidity_millions = market_data['liquidity_value'] * 1000
        market_data['liquidity_bin'] = pd.cut(
            liquidity_millions, 
            bins=[0, 10, 50, 100, float('inf')], 
            labels=['<10M', '10-50M', '50-100M', '>100M']
        )
        
        # Count by liquidity bin
        liquidity_counts = market_data['liquidity_bin'].value_counts()
        
        # Color coding
        colors = ['#ff4444', '#ffaa00', '#90ee90', '#00ff00']
        
        fig = go.Figure(data=[
            go.Bar(
                x=liquidity_counts.index,
                y=liquidity_counts.values,
                marker_color=colors,
                text=liquidity_counts.values,
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Phân Bố Thanh Khoản Thị Trường (Triệu VND)',
            xaxis_title='Khoảng Thanh Khoản',
            yaxis_title='Số Lượng Cổ Phiếu',
            template='plotly_dark'
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating liquidity analysis: {e}")
        fig = go.Figure()
        fig.update_layout(title='Liquidity Analysis - Error', template='plotly_dark')
        return fig

# Chart creation functions
def create_fraud_timeline_chart(alerts):
    """Tạo biểu đồ timeline fraud"""
    if not alerts:
        # Try to get some transaction data for timeline
        try:
            transactions = get_transactions_from_redis()
            if transactions:
                # Filter transactions with fraud predictions
                fraud_tx = [tx for tx in transactions if tx.get('predicted_fraud', 0) == 1 or tx.get('fraud_prob_pct', 0) > 30]
                if fraud_tx:
                    alerts = fraud_tx  # Use fraud transactions as alerts
        except:
            pass
    
    if not alerts:
        fig = go.Figure()
        fig.update_layout(
            title='Fraud Timeline - No Data Available',
            xaxis_title='Time',
            yaxis_title='Fraud Count',
            template='plotly_dark'
        )
        return fig
    
    df = pd.DataFrame(alerts)
    
    try:
        # Handle different timestamp formats
        timestamps = []
        for alert in alerts:
            tx_time = alert.get('transaction_time', '')
            if tx_time:
                if isinstance(tx_time, str):
                    try:
                        timestamps.append(pd.to_datetime(tx_time))
                    except:
                        timestamps.append(pd.Timestamp.now())
                else:
                    timestamps.append(pd.to_datetime(tx_time))
            else:
                timestamps.append(pd.Timestamp.now())
        
        df['timestamp'] = timestamps
        
        # Group by 5-minute intervals
        df['time_bin'] = df['timestamp'].dt.floor('5T')
        hourly_count = df.groupby('time_bin').size().reset_index(name='count')
        
        fig = go.Figure(data=[
            go.Scatter(
                x=hourly_count['time_bin'],
                y=hourly_count['count'],
                mode='lines+markers',
                line=dict(color='#ff4444', width=3),
                marker=dict(size=8, color='#ff4444'),
                fill='tonexty',
                fillcolor='rgba(255, 68, 68, 0.3)'
            )
        ])
        
        fig.update_layout(
            title='Dòng Thời Gian Cảnh Báo Gian Lận (khoảng 5 phút)',
            xaxis_title='Thời Gian',
            yaxis_title='Số Lượng Gian Lận',
            template='plotly_dark',
            showlegend=False,
            height=400
        )
        
        return fig
        
    except Exception as e:
        st.error(f"Error creating timeline chart: {e}")
        fig = go.Figure()
        fig.update_layout(title='Fraud Timeline - Error loading data', template='plotly_dark')
        return fig

def create_fraud_distribution_chart(alerts):
    """Tạo biểu đồ phân bố fraud"""
    if not alerts:
        # Try to get transaction data for distribution
        try:
            transactions = get_transactions_from_redis()
            if transactions:
                df = pd.DataFrame(transactions)
                if not df.empty and 'tx_type' in df.columns:
                    type_counts = df['tx_type'].value_counts()
                    
                    fig = go.Figure(data=[
                        go.Pie(
                            labels=type_counts.index,
                            values=type_counts.values,
                            hole=0.4,
                            textinfo='label+percent+value'
                        )
                    ])
                    
                    fig.update_layout(
                        title='Transaction Type Distribution',
                        template='plotly_dark'
                    )
                    return fig
        except:
            pass
            
        fig = go.Figure()
        fig.update_layout(title='Fraud Distribution - No Data', template='plotly_dark')
        return fig
    
    df = pd.DataFrame(alerts)
    
    try:
        # Distribution by transaction type
        if 'tx_type' in df.columns:
            type_dist = df['tx_type'].value_counts()
        else:
            type_dist = pd.Series({'UNKNOWN': len(alerts)})
        
        fig = go.Figure(data=[
            go.Pie(
                labels=type_dist.index,
                values=type_dist.values,
                hole=0.4,
                marker=dict(colors=['#ff4444', '#ff8844', '#ffaa44']),
                textinfo='label+percent+value',
                textfont_size=12
            )
        ])
        
        fig.update_layout(
            title='Phân Bố Gian Lận Theo Loại Giao Dịch',
            template='plotly_dark',
            showlegend=True,
            font=dict(color='white'),
            height=400
        )
        
        return fig
        
    except Exception as e:
        st.error(f"Error creating distribution chart: {e}")
        fig = go.Figure()
        fig.update_layout(title='Fraud Distribution - Error loading data', template='plotly_dark')
        return fig

def create_amount_distribution_chart(alerts):
    """Tạo biểu đồ phân bố amount"""
    data_source = alerts
    
    # If no alerts, try to use transaction data
    if not alerts:
        try:
            transactions = get_transactions_from_redis()
            if transactions:
                data_source = transactions
        except:
            pass
    
    if not data_source:
        fig = go.Figure()
        fig.update_layout(title='Amount Distribution - No Data', template='plotly_dark')
        return fig
    
    df = pd.DataFrame(data_source)
    amounts = df.get('amount', pd.Series([0] * len(data_source)))
    
    # Create histogram
    fig = go.Figure(data=[
        go.Histogram(
            x=amounts,
            nbinsx=20,
            marker_color='#667eea',
            opacity=0.7
        )
    ])
    
    fig.update_layout(
        title='Phân Bố Số Tiền Gian Lận',
        xaxis_title='Số Tiền ($)',
        yaxis_title='Số Lượng',
        template='plotly_dark',
        height=400
    )
    
    return fig

def create_market_price_chart(market_data):
    """Tạo biểu đồ giá cổ phiếu"""
    if market_data.empty:
        fig = go.Figure()
        fig.update_layout(title='Stock Prices - No Data', template='plotly_dark')
        return fig
    
    try:
        # Sort by price for better visualization
        df_sorted = market_data.sort_values('price', ascending=False).head(10)
        
        # Create color coding based on change_pct
        colors = ['#00cc66' if x > 0 else '#ff4444' if x < 0 else '#ffaa00' 
                 for x in df_sorted['change_pct']]
        
        fig = go.Figure(data=[
            go.Bar(
                x=df_sorted['symbol'],
                y=df_sorted['price'],
                marker_color=colors,
                text=[f"{x:,.0f}" for x in df_sorted['price']],
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Giá Cổ Phiếu Hàng Đầu',
            xaxis_title='Mã Cổ Phiếu',
            yaxis_title='Giá (VND)',
            template='plotly_dark',
            height=400
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating price chart: {e}")
        fig = go.Figure()
        fig.update_layout(title='Stock Prices - Error', template='plotly_dark')
        return fig

def create_market_change_chart(market_data):
    """Tạo biểu đồ thay đổi giá"""
    if market_data.empty:
        fig = go.Figure()
        fig.update_layout(title='Price Changes - No Data', template='plotly_dark')
        return fig
    
    try:
        # Sort by change percentage
        df_sorted = market_data.sort_values('change_pct', ascending=True)
        
        # Create color coding
        colors = ['#ff4444' if x < 0 else '#00cc66' for x in df_sorted['change_pct']]
        
        fig = go.Figure(data=[
            go.Bar(
                x=df_sorted['change_pct'],
                y=df_sorted['symbol'],
                orientation='h',
                marker_color=colors,
                text=[f"{x:.2f}%" for x in df_sorted['change_pct']],
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Thay Đổi Giá Cổ Phiếu (%)',
            xaxis_title='Thay Đổi (%)',
            yaxis_title='Mã Cổ Phiếu',
            template='plotly_dark',
            height=400
        )
        
        fig.add_vline(x=0, line_dash="dash", line_color="white")
        
        return fig
    except Exception as e:
        st.error(f"Error creating change chart: {e}")
        fig = go.Figure()
        fig.update_layout(title='Price Changes - Error', template='plotly_dark')
        return fig

def create_risk_distribution_chart(market_data):
    """Tạo biểu đồ phân bố risk"""
    if market_data.empty:
        fig = go.Figure()
        fig.update_layout(title='Risk Distribution - No Data', template='plotly_dark')
        return fig
    
    try:
        # Risk level distribution
        risk_dist = market_data['price_risk_position'].value_counts()
        
        # Color mapping for risk levels
        color_map = {
            'SAFE': '#00cc66',
            'LOW_RISK': '#ffaa00', 
            'MEDIUM_RISK': '#ff8844',
            'HIGH_RISK': '#ff4444'
        }
        colors = [color_map.get(level, '#888888') for level in risk_dist.index]
        
        fig = go.Figure(data=[
            go.Pie(
                labels=risk_dist.index,
                values=risk_dist.values,
                hole=0.4,
                marker=dict(colors=colors),
                textinfo='label+percent+value',
                textfont_size=12
            )
        ])
        
        fig.update_layout(
            title='Risk Level Distribution',
            template='plotly_dark',
            height=400
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating risk chart: {e}")
        fig = go.Figure()
        fig.update_layout(title='Risk Distribution - Error', template='plotly_dark')
        return fig

def create_volume_chart(market_data):
    """Tạo biểu đồ volume"""
    if market_data.empty:
        fig = go.Figure()
        fig.update_layout(title='Trading Volume - No Data', template='plotly_dark')
        return fig
    
    try:
        # Sort by volume
        df_sorted = market_data.sort_values('volume', ascending=False).head(10)
        
        fig = go.Figure(data=[
            go.Bar(
                x=df_sorted['symbol'],
                y=df_sorted['volume'],
                marker_color='#764ba2',
                text=[f"{x:,.0f}" for x in df_sorted['volume']],
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Khối Lượng Giao Dịch Hàng Đầu',
            xaxis_title='Mã Cổ Phiếu',
            yaxis_title='Khối Lượng',
            template='plotly_dark',
            height=400
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating volume chart: {e}")
        fig = go.Figure()
        fig.update_layout(title='Trading Volume - Error', template='plotly_dark')
        return fig

# Main dashboard
def main():
    st.title("🚀 Bảng Điều Khiển Giám Sát Rủi Ro Thị Trường & Gian Lận")
    st.markdown("Giám sát trực tiếp thị trường chứng khoán và gian lận giao dịch")
    
    # Sidebar
    st.sidebar.title("📊 Điều Khiển Bảng Tin")
    
    # Manual refresh controls
    if st.sidebar.button("🔄 Làm Mới Dữ Liệu"):
        st.cache_data.clear()
        st.rerun()
    
    # Redis status
    redis_connected = test_redis_connection()
    status_color = "🟢" if redis_connected else "🔴"
    st.sidebar.markdown(f"**Trạng Thái Redis:** {status_color} {'Đã Kết Nối' if redis_connected else 'Mất Kết Nối'}")
    
    # Last update time
    st.sidebar.markdown(f"**Cập Nhật Lần Cuối:** {datetime.now().strftime('%H:%M:%S')}")
    
    # Main tabs
    tab1, tab2 = st.tabs(["🛡️ Phát Hiện Gian Lận", "📈 Giám Sát Rủi Ro Thị Trường"])
    
    # Fraud Detection Tab
    with tab1:
        st.header("🛡️ Giám Sát Phát Hiện Gian Lận")
        
        # Refresh button for this tab
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button('🔄 Làm Mới', key='fraud_refresh'):
                st.rerun()
        
        # Get data
        transactions = get_transactions_from_redis()
        alerts = get_fraud_alerts_from_redis()
        fraud_stats = get_fraud_statistics_from_redis()
        
        # Summary metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            # Get total processed transactions
            total_processed = get_total_transaction_count()
            recent_tx = len(transactions)
            fraud_tx = len([tx for tx in transactions if tx.get('predicted_fraud') == 1])
            
            st.metric(
                "Tổng Số Đã Xử Lý", 
                f"{total_processed:,}", 
                delta=f"{recent_tx} gần đây"
            )
        
        with col2:
            fraud_count = len(alerts)
            # Get total fraud count from counter
            total_fraud_count = 0
            try:
                total_fraud_str = redis_client.get('fraud:counter:total_fraud')
                if total_fraud_str:
                    total_fraud_count = int(total_fraud_str)
                else:
                    # Calculate from recent fraud transactions
                    total_fraud_count = len([tx for tx in transactions if tx.get('predicted_fraud') == 1])
            except:
                total_fraud_count = fraud_tx
                
            st.metric("Phát Hiện Gian Lận", f"{total_fraud_count:,}", delta=f"{fraud_count} cảnh báo")
        
        with col3:
            # Calculate fraud rate from total processed vs total fraud
            fraud_rate = (total_fraud_count / total_processed * 100) if total_processed > 0 else 0
            recent_fraud_rate = (fraud_tx / recent_tx * 100) if recent_tx > 0 else 0
            
            st.metric(
                "Tỷ Lệ Gian Lận Tổng", 
                f"{fraud_rate:.2f}%", 
                delta=f"Gần đây: {recent_fraud_rate:.1f}%"
            )
        
        with col4:
            # Calculate amounts from recent transactions
            total_tx_amount = sum(tx.get('amount', 0) for tx in transactions)
            alert_amount = sum(alert.get('amount', 0) for alert in alerts)
            
            # Format amount display
            if total_tx_amount > 1e9:
                amount_display = f"${total_tx_amount/1e9:.1f}B"
                delta_display = f"${alert_amount/1e6:.1f}M at risk"
            elif total_tx_amount > 1e6:
                amount_display = f"${total_tx_amount/1e6:.1f}M"
                delta_display = f"${alert_amount/1e3:.1f}K at risk"
            elif total_tx_amount > 1e3:
                amount_display = f"${total_tx_amount/1e3:.1f}K"
                delta_display = f"${alert_amount:.0f} at risk"
            else:
                amount_display = f"${total_tx_amount:.0f}"
                delta_display = f"${alert_amount:.0f} at risk"
                
            st.metric("Số Tiền Gần Đây", amount_display, delta=delta_display)
        
        with col5:
            accuracy = fraud_stats.get('accuracy_pct', 0)
            total_stats = fraud_stats.get('total_transactions', 0)
            delta_text = f"Thống kê: {total_stats}" if total_stats > 0 else "Hiệu Suất ML"
            st.metric("Độ Chính Xác Mô Hình", f"{accuracy:.1f}%", delta=delta_text)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(create_fraud_timeline_chart(alerts), width='stretch')
        
        with col2:
            st.plotly_chart(create_fraud_distribution_chart(alerts), width='stretch')
        
        # Additional chart
        st.plotly_chart(create_amount_distribution_chart(alerts), width='stretch')
        
        # Recent transactions table
        st.subheader("📋 Giao Dịch Gần Đây")
        
        if transactions:
            # Convert to DataFrame for better display
            tx_df = pd.DataFrame(transactions[:20])
            
            # Select and rename columns for display
            display_columns = {}
            if 'tx_id' in tx_df.columns:
                display_columns['Mã GD'] = tx_df['tx_id']
            if 'tx_type' in tx_df.columns:
                display_columns['Loại'] = tx_df['tx_type']
            if 'amount' in tx_df.columns:
                display_columns['Số Tiền'] = tx_df['amount'].apply(lambda x: f"${x:,.2f}")
            if 'from_account' in tx_df.columns:
                display_columns['Từ Tài Khoản'] = tx_df['from_account'].apply(lambda x: str(x)[:15] + "...")
            if 'fraud_prob_pct' in tx_df.columns:
                display_columns['Xác Suất Gian Lận'] = tx_df['fraud_prob_pct'].apply(lambda x: f"{x:.1f}%")
            if 'predicted_fraud' in tx_df.columns:
                display_columns['Trạng Thái'] = tx_df['predicted_fraud'].apply(lambda x: "🚨 GIAN LẬN" if x == 1 else "✅ OK")
            
            if display_columns:
                display_df = pd.DataFrame(display_columns)
                st.dataframe(display_df, width='stretch')
            else:
                st.write("Transaction data format not recognized")
        else:
            st.info("Không tìm thấy giao dịch gần đây")
        
        # Fraud alerts
        st.subheader("🚨 Cảnh Báo Gian Lận Gần Đây")
        
        if alerts:
            for i, alert in enumerate(alerts[:10]):
                alert_type = alert.get('alert_type', 'UNKNOWN')
                fraud_prob = alert.get('fraud_prob_pct', 0)
                predicted_fraud = alert.get('predicted_fraud', 0)
                
                # Color coding based on risk level
                if predicted_fraud == 1:
                    alert_color = "🔴"
                    risk_level = "FRAUD DETECTED"
                elif fraud_prob > 70:
                    alert_color = "🟠"
                    risk_level = "VERY HIGH RISK"
                elif fraud_prob > 45:
                    alert_color = "🟡"
                    risk_level = "HIGH RISK"
                else:
                    alert_color = "🔵"
                    risk_level = "MODERATE RISK"
                
                with st.expander(f"{alert_color} Alert #{alert.get('tx_id', i+1)} - ${alert.get('amount', 0):,.2f} ({risk_level})"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.write(f"**Transaction ID:** {alert.get('tx_id', 'N/A')}")
                        st.write(f"**Type:** {alert.get('tx_type', 'N/A')}")
                        st.write(f"**Amount:** ${alert.get('amount', 0):,.2f}")
                    
                    with col2:
                        st.write(f"**From:** {alert.get('from_account', 'N/A')}")
                        st.write(f"**To:** {alert.get('to_account', 'N/A')}")
                        st.write(f"**Risk Level:** {risk_level}")
                    
                    with col3:
                        st.write(f"**Transaction Time:** {alert.get('transaction_time', 'N/A')}")
                        st.write(f"**Fraud Probability:** {fraud_prob:.1f}%")
                        
                        # Progress bar for fraud probability
                        progress_val = min(fraud_prob / 100, 1.0)
                        st.progress(progress_val, f"Risk Score: {fraud_prob:.1f}%")
                    
        else:
            # Show enhanced message when no alerts found
            st.info("📊 No specific fraud alerts generated yet.")
            
            # Check if there are transactions with fraud probability
            try:
                all_transactions = get_transactions_from_redis()
                if all_transactions:
                    high_risk_tx = [tx for tx in all_transactions if tx.get('fraud_prob_pct', 0) > 30]
                    if high_risk_tx:
                        st.warning(f"⚠️ Found {len(high_risk_tx)} transactions with fraud probability > 30%. Review Recent Transactions below.")
                    else:
                        st.success("✅ All recent transactions appear to be legitimate (low fraud probability).")
                else:
                    st.info("ℹ️ No transaction data available. Make sure the fraud detection consumer is running.")
            except Exception as e:
                st.error(f"Error checking transactions: {e}")
    
    # Market Risk Tab
    with tab2:
        st.title("📊 Phân Tích Rủi Ro Thị Trường Nâng Cao")
        
        # Refresh button for this tab
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button('🔄 Làm Mới', key='market_refresh'):
                st.cache_data.clear()
                st.rerun()
        
        # Get comprehensive market data
        market_data = get_market_data_from_redis(redis_client)
        high_risk_stocks = get_high_risk_stocks(redis_client)
        market_stats = get_market_statistics(redis_client)
        
        if market_data is not None and not market_data.empty:
            # Enhanced Market Statistics
            st.subheader("📊 Tổng Quan Thị Trường")
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "Tổng Số Mã", 
                    market_stats['symbol_count'],
                    delta=f"{len(market_data)} đang hoạt động"
                )
            
            with col2:
                avg_change = market_data['change_pct'].mean()
                st.metric(
                    "Xu Hướng Thị Trường", 
                    f"{avg_change:+.2f}%",
                    delta="Thay đổi trung bình"
                )
            
            with col3:
                high_risk_count = len([s for s in market_data['composite_risk_score'] if s > 2.0])
                st.metric(
                    "Rủi Ro Cao", 
                    high_risk_count,
                    delta=f"{high_risk_count/len(market_data)*100:.1f}%" if len(market_data) > 0 else "0%"
                )
            
            with col4:
                avg_liquidity = market_data['liquidity_value'].mean()
                # Display in millions VND instead of billions
                liquidity_millions = avg_liquidity * 1000  # Convert from billions to millions
                liquidity_display = f"{liquidity_millions:.0f}M VND"
                st.metric(
                    "Thanh Khoản TB", 
                    liquidity_display,
                    delta="Độ sâu thị trường"
                )
            
            with col5:
                avg_risk_reward = market_data['risk_reward_ratio'].mean()
                st.metric(
                    "Tỷ Lệ Rủi Ro/Lợi Nhuận TB", 
                    f"{avg_risk_reward:.2f}",
                    delta="Cơ hội thị trường"
                )
            
            # High Risk Alert Section - Check from market_data directly
            high_risk_data = market_data[market_data['composite_risk_score'] > 2.0] if not market_data.empty else pd.DataFrame()
            
            if not high_risk_data.empty:
                st.subheader("🚨 Cảnh Báo Rủi Ro Cao")
                st.warning(f"Tìm thấy {len(high_risk_data)} cổ phiếu có điểm rủi ro tổng hợp > 2.0")
                
                # Display top 5 high risk stocks
                for i, (idx, stock) in enumerate(high_risk_data.head(5).iterrows()):
                    with st.expander(f"🔴 {stock['symbol']} - Risk Score: {stock['composite_risk_score']:.2f}"):
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.write(f"**Biến Động:** {stock.get('volatility_risk_level', 'N/A')}")
                            st.write(f"**Vị Thế Giá:** {stock.get('price_risk_position', 'N/A')}")
                        with col_b:
                            st.write(f"**Dòng Vốn Nước Ngoài:** {stock.get('foreign_flow_risk', 'N/A')}")
                            st.write(f"**Thanh Khoản:** {stock.get('liquidity_value', 0)*1000:.0f}M VND")
                        with col_c:
                            st.write(f"**Rủi Ro/Lợi Nhuận:** {stock.get('risk_reward_ratio', 0):.2f}")
                            risk_progress = min(stock['composite_risk_score'] / 3.0, 1.0)
                            st.progress(risk_progress, f"Mức Rủi Ro: {stock['composite_risk_score']:.2f}/3.0")
            
            # Advanced Risk Analysis Charts
            st.subheader("📈 Phân Tích Rủi Ro Nâng Cao")
            
            # First row of charts
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                # Risk Heatmap
                heatmap_fig = create_risk_heatmap(market_data)
                st.plotly_chart(heatmap_fig, width='stretch')
                
                # Liquidity Analysis
                liquidity_fig = create_liquidity_analysis(market_data)
                st.plotly_chart(liquidity_fig, width='stretch')
            
            with chart_col2:
                # Risk vs Reward Scatter
                scatter_fig = create_risk_reward_scatter(market_data)
                st.plotly_chart(scatter_fig, width='stretch')
                
                # Price change chart
                change_fig = create_market_change_chart(market_data)
                st.plotly_chart(change_fig, width='stretch')
                
                # Volume chart
                volume_fig = create_volume_chart(market_data)
                st.plotly_chart(volume_fig, width='stretch')
            
            # Enhanced Market Data Display
            st.subheader("📋 Phân Tích Thị Trường Chi Tiết")
            
            # Filter options
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            
            with filter_col1:
                risk_filter = st.selectbox(
                    "Lọc Theo Mức Rủi Ro",
                    ["Tất Cả", "Rủi Ro Thấp (<1.5)", "Rủi Ro Trung Bình (1.5-2.5)", "Rủi Ro Cao (>2.5)"],
                    key="risk_filter"
                )
            
            with filter_col2:
                volatility_filter = st.selectbox(
                    "Lọc Theo Độ Biến Động",
                    ["Tất Cả", "THẤP", "TRUNG BÌNH", "CAO"],
                    key="volatility_filter"
                )
            
            with filter_col3:
                sort_by = st.selectbox(
                    "Sắp Xếp Theo",
                    ["composite_risk_score", "risk_reward_ratio", "liquidity_value", "change_pct", "volatility_pct"],
                    key="sort_by"
                )
            
            # Apply filters
            filtered_data = market_data.copy()
            
            if risk_filter != "Tất Cả":
                if risk_filter == "Rủi Ro Thấp (<1.5)":
                    filtered_data = filtered_data[filtered_data['composite_risk_score'] < 1.5]
                elif risk_filter == "Rủi Ro Trung Bình (1.5-2.5)":
                    filtered_data = filtered_data[(filtered_data['composite_risk_score'] >= 1.5) & (filtered_data['composite_risk_score'] <= 2.5)]
                elif risk_filter == "Rủi Ro Cao (>2.5)":
                    filtered_data = filtered_data[filtered_data['composite_risk_score'] > 2.5]
            
            if volatility_filter != "Tất Cả":
                volatility_map = {"THẤP": "LOW", "TRUNG BÌNH": "MEDIUM", "CAO": "HIGH"}
                english_filter = volatility_map.get(volatility_filter, volatility_filter)
                filtered_data = filtered_data[filtered_data['volatility_risk_level'] == english_filter]
            
            # Sort data
            filtered_data = filtered_data.sort_values(sort_by, ascending=False)
            
            # Display comprehensive table
            if not filtered_data.empty:
                # Select key columns for display
                display_columns = [
                    'symbol', 'company_name', 'price', 'change_pct', 'volume',
                    'volatility_pct', 'volatility_risk_level', 'composite_risk_score',
                    'risk_reward_ratio', 'liquidity_value', 'price_risk_position',
                    'foreign_flow_risk', 'foreign_net'
                ]
                
                # Filter columns that exist in the data
                available_columns = [col for col in display_columns if col in filtered_data.columns]
                display_data = filtered_data[available_columns]
                
                # Convert liquidity to millions for display
                display_data_copy = display_data.copy()
                if 'liquidity_value' in display_data_copy.columns:
                    display_data_copy['liquidity_value'] = display_data_copy['liquidity_value'] * 1000  # Convert billions to millions
                
                # Format the dataframe for better display
                formatted_df = display_data_copy.style.format({
                    'price': '{:,.0f}',
                    'change_pct': '{:+.2f}%',
                    'volume': '{:,.0f}',
                    'volatility_pct': '{:.2f}%',
                    'composite_risk_score': '{:.2f}',
                    'risk_reward_ratio': '{:.2f}',
                    'liquidity_value': '{:.0f}M',  # Changed from B to M (millions)
                    'foreign_net': '{:,.0f}'
                }).background_gradient(
                    subset=['composite_risk_score'], cmap='Reds', vmin=1, vmax=3
                ).background_gradient(
                    subset=['risk_reward_ratio'], cmap='RdYlGn', vmin=0, vmax=2
                )
                
                st.dataframe(formatted_df, width='stretch')
                
                # Summary statistics for filtered data
                st.subheader("📊 Tổng Hợp Dữ Liệu Đã Lọc")
                summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
                
                with summary_col1:
                    avg_risk = filtered_data['composite_risk_score'].mean()
                    st.metric("Điểm Rủi Ro TB", f"{avg_risk:.2f}")
                
                with summary_col2:
                    avg_rr = filtered_data['risk_reward_ratio'].mean()
                    st.metric("Tỷ Lệ Rủi Ro/Lợi Nhuận TB", f"{avg_rr:.2f}")
                
                with summary_col3:
                    total_liquidity = filtered_data['liquidity_value'].sum() * 1000  # Convert to millions
                    st.metric("Tổng Thanh Khoản", f"{total_liquidity:,.0f}M VND")
                
                with summary_col4:
                    high_vol_count = len(filtered_data[filtered_data['volatility_risk_level'] == 'HIGH'])
                    st.metric("Biến Động Cao", f"{high_vol_count}/{len(filtered_data)}")
            else:
                st.info("Không có dữ liệu nào phù hợp với bộ lọc đã chọn.")
        else:
            st.warning("Không có dữ liệu thị trường. Đảm bảo consumer thị trường đang chạy.")
    


if __name__ == "__main__":
    main()