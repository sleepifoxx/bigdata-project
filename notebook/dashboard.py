"""
Real-time Dashboard - Market Risk & Fraud Detection
Sử dụng Plotly Dash và Redis để hiển thị thông tin real-time
"""

import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd
import redis
import json
from collections import deque
import threading
import time
import sys

# Add redis module to path
sys.path.insert(0, '/home/jovyan/work/redis')

# Kết nối Redis
redis_client = redis.Redis(
    host='redis',
    port=6379,
    db=0,
    decode_responses=True
)

# Initialize Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Real-time Market & Fraud Dashboard"

# CSS styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #0e1117;
                color: #fafafa;
                margin: 0;
                padding: 0;
            }
            .header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
                text-align: center;
                box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            }
            .header h1 {
                margin: 0;
                font-size: 2.5em;
                font-weight: 700;
                color: white;
            }
            .stats-card {
                background: #1e2130;
                border-radius: 10px;
                padding: 20px;
                margin: 10px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.3);
                border-left: 4px solid #667eea;
            }
            .alert-high {
                border-left-color: #ff4444 !important;
                animation: pulse 2s infinite;
            }
            .alert-medium {
                border-left-color: #ffaa00 !important;
            }
            .alert-low {
                border-left-color: #00cc66 !important;
            }
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.7; }
            }
            .metric-value {
                font-size: 2em;
                font-weight: bold;
                margin: 10px 0;
            }
            .metric-label {
                font-size: 0.9em;
                color: #888;
                text-transform: uppercase;
            }
            .tab-custom {
                background-color: #1e2130 !important;
                color: #fafafa !important;
            }
            .fraud-alert {
                background: #2d1f1f;
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
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("🚀 Real-time Market Risk & Fraud Detection Dashboard"),
        html.P("Live monitoring of stock market and transaction fraud", 
               style={'fontSize': '1.2em', 'margin': '10px 0 0 0'})
    ], className='header'),
    
    # Main content
    html.Div([
        # Tabs
        dcc.Tabs(id='main-tabs', value='market-tab', children=[
            dcc.Tab(label='📈 Market Risk Monitor', value='market-tab', className='tab-custom'),
            dcc.Tab(label='🛡️ Fraud Detection', value='fraud-tab', className='tab-custom'),
            dcc.Tab(label='📊 Statistics', value='stats-tab', className='tab-custom'),
        ], style={'marginBottom': '20px'}),
        
        # Tab content
        html.Div(id='tab-content'),
        
        # Auto-refresh interval
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # 5 seconds
            n_intervals=0
        ),
        
    ], style={'padding': '20px'}),
    
    # Hidden div for storing data
    html.Div(id='hidden-div', style={'display': 'none'})
])


# ============================================================================
# MARKET RISK TAB LAYOUT
# ============================================================================
def create_market_tab():
    return html.Div([
        # Summary Cards
        html.Div([
            html.Div(id='market-summary-cards', children=[]),
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'marginBottom': '20px'}),
        
        # Charts Row 1
        html.Div([
            html.Div([
                dcc.Graph(id='market-price-chart', style={'height': '400px'})
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            
            html.Div([
                dcc.Graph(id='market-risk-chart', style={'height': '400px'})
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        ]),
        
        # Charts Row 2
        html.Div([
            html.Div([
                dcc.Graph(id='market-volatility-chart', style={'height': '400px'})
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            
            html.Div([
                dcc.Graph(id='foreign-flow-chart', style={'height': '400px'})
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        ]),
        
        # Risk Alerts
        html.Div([
            html.H3("⚠️ Risk Alerts", style={'color': '#ff4444'}),
            html.Div(id='market-risk-alerts')
        ], className='stats-card', style={'marginTop': '20px'}),
    ])


# ============================================================================
# FRAUD DETECTION TAB LAYOUT
# ============================================================================
def create_fraud_tab():
    return html.Div([
        # Summary Cards
        html.Div([
            html.Div(id='fraud-summary-cards', children=[]),
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'marginBottom': '20px'}),
        
        # Charts
        html.Div([
            html.Div([
                dcc.Graph(id='fraud-timeline-chart', style={'height': '400px'})
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            
            html.Div([
                dcc.Graph(id='fraud-distribution-chart', style={'height': '400px'})
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        ]),
        
        # Recent Transactions
        html.Div([
            html.H3("📋 Recent Transactions"),
            html.Div(id='recent-transactions-table')
        ], className='stats-card', style={'marginTop': '20px'}),
        
        # Fraud Alerts
        html.Div([
            html.H3("🚨 Fraud Alerts", style={'color': '#ff4444'}),
            html.Div(id='fraud-alerts-list')
        ], className='stats-card', style={'marginTop': '20px'}),
    ])


# ============================================================================
# STATISTICS TAB LAYOUT
# ============================================================================
def create_stats_tab():
    return html.Div([
        html.Div([
            html.Div([
                html.H3("Market Statistics"),
                html.Div(id='market-stats-content')
            ], className='stats-card', style={'width': '48%', 'display': 'inline-block', 'margin': '10px'}),
            
            html.Div([
                html.H3("Fraud Detection Statistics"),
                html.Div(id='fraud-stats-content')
            ], className='stats-card', style={'width': '48%', 'display': 'inline-block', 'margin': '10px'}),
        ]),
        
        # Historical Data
        html.Div([
            html.H3("📊 Historical Performance"),
            dcc.Graph(id='historical-performance-chart', style={'height': '500px'})
        ], className='stats-card', style={'marginTop': '20px'}),
    ])


# ============================================================================
# CALLBACKS
# ============================================================================

@app.callback(
    Output('tab-content', 'children'),
    Input('main-tabs', 'value')
)
def render_tab_content(tab):
    if tab == 'market-tab':
        return create_market_tab()
    elif tab == 'fraud-tab':
        return create_fraud_tab()
    elif tab == 'stats-tab':
        return create_stats_tab()


# ============================================================================
# MARKET RISK CALLBACKS
# ============================================================================

@app.callback(
    [Output('market-summary-cards', 'children'),
     Output('market-price-chart', 'figure'),
     Output('market-risk-chart', 'figure'),
     Output('market-volatility-chart', 'figure'),
     Output('foreign-flow-chart', 'figure'),
     Output('market-risk-alerts', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_market_dashboard(n):
    # Lấy dữ liệu từ Redis
    market_data = get_market_data_from_redis()
    risk_alerts = get_market_risk_alerts_from_redis()
    
    # Summary Cards
    summary_cards = create_market_summary_cards(market_data)
    
    # Charts
    price_chart = create_price_chart(market_data)
    risk_chart = create_risk_score_chart(market_data)
    volatility_chart = create_volatility_chart(market_data)
    foreign_chart = create_foreign_flow_chart(market_data)
    
    # Alerts
    alerts_html = create_market_alerts_html(risk_alerts)
    
    return summary_cards, price_chart, risk_chart, volatility_chart, foreign_chart, alerts_html


# ============================================================================
# FRAUD DETECTION CALLBACKS
# ============================================================================

@app.callback(
    [Output('fraud-summary-cards', 'children'),
     Output('fraud-timeline-chart', 'figure'),
     Output('fraud-distribution-chart', 'figure'),
     Output('recent-transactions-table', 'children'),
     Output('fraud-alerts-list', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_fraud_dashboard(n):
    # Lấy dữ liệu từ Redis
    transactions = get_transactions_from_redis()
    fraud_alerts = get_fraud_alerts_from_redis()
    
    # Summary Cards
    summary_cards = create_fraud_summary_cards(transactions, fraud_alerts)
    
    # Charts
    timeline_chart = create_fraud_timeline_chart(fraud_alerts)
    distribution_chart = create_fraud_distribution_chart(fraud_alerts)
    
    # Tables
    transactions_table = create_transactions_table(transactions)
    alerts_html = create_fraud_alerts_html(fraud_alerts)
    
    return summary_cards, timeline_chart, distribution_chart, transactions_table, alerts_html


# ============================================================================
# STATISTICS CALLBACKS
# ============================================================================

@app.callback(
    [Output('market-stats-content', 'children'),
     Output('fraud-stats-content', 'children'),
     Output('historical-performance-chart', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_statistics(n):
    market_stats = get_market_statistics_from_redis()
    fraud_stats = get_fraud_statistics_from_redis()
    historical_chart = create_historical_chart()
    
    market_html = create_stats_html(market_stats)
    fraud_html = create_stats_html(fraud_stats)
    
    return market_html, fraud_html, historical_chart


# ============================================================================
# REDIS DATA FUNCTIONS
# ============================================================================

def get_market_data_from_redis():
    """Lấy dữ liệu market từ Redis"""
    try:
        # Lấy 100 records gần nhất
        keys = redis_client.keys('market:stock:*')
        data = []
        
        for key in sorted(keys, reverse=True)[:100]:
            value = redis_client.get(key)
            if value:
                data.append(json.loads(value))
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        print(f"Error getting market data: {e}")
        return pd.DataFrame()


def get_market_risk_alerts_from_redis():
    """Lấy cảnh báo rủi ro từ Redis"""
    try:
        keys = redis_client.keys('market:alert:*')
        alerts = []
        
        for key in sorted(keys, reverse=True)[:20]:
            value = redis_client.get(key)
            if value:
                alerts.append(json.loads(value))
        
        return alerts
    except Exception as e:
        print(f"Error getting risk alerts: {e}")
        return []


def get_transactions_from_redis():
    """Lấy giao dịch gần nhất từ Redis"""
    try:
        keys = redis_client.keys('fraud:transaction:*')
        transactions = []
        
        for key in sorted(keys, reverse=True)[:50]:
            value = redis_client.get(key)
            if value:
                transactions.append(json.loads(value))
        
        return transactions
    except Exception as e:
        print(f"Error getting transactions: {e}")
        return []


def get_fraud_alerts_from_redis():
    """Lấy cảnh báo gian lận từ Redis"""
    try:
        keys = redis_client.keys('fraud:alert:*')
        alerts = []
        
        for key in sorted(keys, reverse=True)[:30]:
            value = redis_client.get(key)
            if value:
                alerts.append(json.loads(value))
        
        return alerts
    except Exception as e:
        print(f"Error getting fraud alerts: {e}")
        return []


def get_market_statistics_from_redis():
    """Lấy thống kê market từ Redis"""
    try:
        stats = redis_client.get('market:statistics')
        return json.loads(stats) if stats else {}
    except:
        return {}


def get_fraud_statistics_from_redis():
    """Lấy thống kê fraud từ Redis"""
    try:
        stats = redis_client.get('fraud:statistics')
        return json.loads(stats) if stats else {}
    except:
        return {}


# ============================================================================
# CHART CREATION FUNCTIONS - MARKET
# ============================================================================

def create_market_summary_cards(df):
    """Tạo summary cards cho market"""
    if df.empty:
        return [html.Div("No data available", className='stats-card')]
    
    cards = []
    
    # Số mã đang theo dõi
    total_stocks = df['symbol'].nunique() if 'symbol' in df.columns else 0
    cards.append(
        html.Div([
            html.Div("Total Stocks", className='metric-label'),
            html.Div(str(total_stocks), className='metric-value', style={'color': '#667eea'})
        ], className='stats-card', style={'width': '200px'})
    )
    
    # Mã rủi ro cao
    high_risk = len(df[df.get('risk_score', 0) > 2]) if 'risk_score' in df.columns else 0
    cards.append(
        html.Div([
            html.Div("High Risk", className='metric-label'),
            html.Div(str(high_risk), className='metric-value', style={'color': '#ff4444'})
        ], className='stats-card alert-high' if high_risk > 0 else 'stats-card', 
        style={'width': '200px'})
    )
    
    # Volatility trung bình
    avg_vol = df['volatility_pct'].mean() if 'volatility_pct' in df.columns else 0
    cards.append(
        html.Div([
            html.Div("Avg Volatility", className='metric-label'),
            html.Div(f"{avg_vol:.2f}%", className='metric-value', style={'color': '#ffaa00'})
        ], className='stats-card', style={'width': '200px'})
    )
    
    # Foreign Flow
    total_foreign = df['foreign_net'].sum() if 'foreign_net' in df.columns else 0
    color = '#00cc66' if total_foreign > 0 else '#ff4444'
    cards.append(
        html.Div([
            html.Div("Foreign Flow", className='metric-label'),
            html.Div(f"{total_foreign/1e6:.1f}M", className='metric-value', style={'color': color})
        ], className='stats-card', style={'width': '200px'})
    )
    
    return cards


def create_price_chart(df):
    """Tạo biểu đồ giá"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    for symbol in df['symbol'].unique()[:5]:  # Top 5 stocks
        symbol_data = df[df['symbol'] == symbol]
        fig.add_trace(go.Scatter(
            x=symbol_data.get('timestamp', []),
            y=symbol_data.get('price', []),
            name=symbol,
            mode='lines+markers'
        ))
    
    fig.update_layout(
        title='Stock Prices (Top 5)',
        xaxis_title='Time',
        yaxis_title='Price (VND)',
        template='plotly_dark',
        hovermode='x unified'
    )
    
    return fig


def create_risk_score_chart(df):
    """Tạo biểu đồ risk score"""
    if df.empty or 'risk_score' not in df.columns:
        return go.Figure()
    
    # Aggregate by symbol
    risk_by_symbol = df.groupby('symbol')['risk_score'].mean().sort_values(ascending=False).head(10)
    
    colors = ['#ff4444' if x > 2 else '#ffaa00' if x > 1.5 else '#00cc66' 
              for x in risk_by_symbol.values]
    
    fig = go.Figure(data=[
        go.Bar(
            x=risk_by_symbol.index,
            y=risk_by_symbol.values,
            marker_color=colors,
            text=[f'{x:.2f}' for x in risk_by_symbol.values],
            textposition='auto'
        )
    ])
    
    fig.update_layout(
        title='Risk Score by Symbol',
        xaxis_title='Symbol',
        yaxis_title='Risk Score',
        template='plotly_dark',
        showlegend=False
    )
    
    fig.add_hline(y=2.0, line_dash="dash", line_color="red", 
                  annotation_text="High Risk")
    fig.add_hline(y=1.5, line_dash="dash", line_color="orange", 
                  annotation_text="Medium Risk")
    
    return fig


def create_volatility_chart(df):
    """Tạo biểu đồ volatility"""
    if df.empty or 'volatility_pct' not in df.columns:
        return go.Figure()
    
    vol_by_symbol = df.groupby('symbol')['volatility_pct'].mean().sort_values(ascending=False).head(10)
    
    fig = go.Figure(data=[
        go.Bar(
            x=vol_by_symbol.index,
            y=vol_by_symbol.values,
            marker_color='#764ba2',
            text=[f'{x:.2f}%' for x in vol_by_symbol.values],
            textposition='auto'
        )
    ])
    
    fig.update_layout(
        title='Volatility by Symbol',
        xaxis_title='Symbol',
        yaxis_title='Volatility (%)',
        template='plotly_dark'
    )
    
    return fig


def create_foreign_flow_chart(df):
    """Tạo biểu đồ dòng tiền nước ngoài"""
    if df.empty or 'foreign_net' not in df.columns:
        return go.Figure()
    
    foreign_by_symbol = df.groupby('symbol')['foreign_net'].sum().sort_values(ascending=False).head(10)
    
    colors = ['#00cc66' if x > 0 else '#ff4444' for x in foreign_by_symbol.values]
    
    fig = go.Figure(data=[
        go.Bar(
            x=foreign_by_symbol.index,
            y=foreign_by_symbol.values / 1000,  # Convert to thousands
            marker_color=colors,
            text=[f'{x/1000:.0f}K' for x in foreign_by_symbol.values],
            textposition='auto'
        )
    ])
    
    fig.update_layout(
        title='Foreign Net Flow by Symbol',
        xaxis_title='Symbol',
        yaxis_title='Net Flow (K shares)',
        template='plotly_dark'
    )
    
    fig.add_hline(y=0, line_dash="dash", line_color="white")
    
    return fig


def create_market_alerts_html(alerts):
    """Tạo HTML cho market alerts"""
    if not alerts:
        return html.Div("No risk alerts at the moment ✅", style={'color': '#00cc66'})
    
    alert_divs = []
    for alert in alerts[:10]:
        alert_class = 'alert-high' if alert.get('risk_level') == 'HIGH' else 'alert-medium'
        
        alert_divs.append(
            html.Div([
                html.Div([
                    html.Strong(f"{alert.get('symbol', 'N/A')}", style={'fontSize': '1.2em'}),
                    html.Span(f" - {alert.get('timestamp', '')}", 
                             style={'color': '#888', 'marginLeft': '10px'})
                ]),
                html.Div(f"Risk Score: {alert.get('risk_score', 0):.2f}", 
                        style={'margin': '5px 0'}),
                html.Div(f"⚠️ {alert.get('message', '')}", 
                        style={'color': '#ffaa00'})
            ], className=f'stats-card {alert_class}', style={'marginBottom': '10px'})
        )
    
    return html.Div(alert_divs)


# ============================================================================
# CHART CREATION FUNCTIONS - FRAUD
# ============================================================================

def create_fraud_summary_cards(transactions, alerts):
    """Tạo summary cards cho fraud"""
    cards = []
    
    # Total transactions
    total_tx = len(transactions)
    cards.append(
        html.Div([
            html.Div("Total Transactions", className='metric-label'),
            html.Div(f"{total_tx:,}", className='metric-value', style={'color': '#667eea'})
        ], className='stats-card', style={'width': '200px'})
    )
    
    # Fraud alerts
    fraud_count = len(alerts)
    cards.append(
        html.Div([
            html.Div("Fraud Alerts", className='metric-label'),
            html.Div(str(fraud_count), className='metric-value', style={'color': '#ff4444'})
        ], className='stats-card alert-high' if fraud_count > 0 else 'stats-card',
        style={'width': '200px'})
    )
    
    # Fraud rate
    fraud_rate = (fraud_count / total_tx * 100) if total_tx > 0 else 0
    cards.append(
        html.Div([
            html.Div("Fraud Rate", className='metric-label'),
            html.Div(f"{fraud_rate:.2f}%", className='metric-value', style={'color': '#ffaa00'})
        ], className='stats-card', style={'width': '200px'})
    )
    
    # Total amount at risk
    total_amount = sum(alert.get('amount', 0) for alert in alerts)
    cards.append(
        html.Div([
            html.Div("Amount at Risk", className='metric-label'),
            html.Div(f"${total_amount/1e6:.1f}M", className='metric-value', style={'color': '#ff4444'})
        ], className='stats-card', style={'width': '200px'})
    )
    
    return cards


def create_fraud_timeline_chart(alerts):
    """Tạo biểu đồ timeline fraud"""
    if not alerts:
        return go.Figure()
    
    df = pd.DataFrame(alerts)
    df['timestamp'] = pd.to_datetime(df.get('timestamp', []))
    df = df.sort_values('timestamp')
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df.get('fraud_prob_pct', []),
        mode='lines+markers',
        name='Fraud Probability',
        marker=dict(size=10, color=df.get('fraud_prob_pct', []), 
                   colorscale='Reds', showscale=True),
        line=dict(color='#ff4444', width=2)
    ))
    
    fig.update_layout(
        title='Fraud Detection Timeline',
        xaxis_title='Time',
        yaxis_title='Fraud Probability (%)',
        template='plotly_dark',
        hovermode='x unified'
    )
    
    fig.add_hline(y=50, line_dash="dash", line_color="orange", 
                  annotation_text="Threshold")
    
    return fig


def create_fraud_distribution_chart(alerts):
    """Tạo biểu đồ phân bố fraud"""
    if not alerts:
        return go.Figure()
    
    df = pd.DataFrame(alerts)
    
    # Distribution by transaction type
    type_dist = df.get('tx_type', pd.Series()).value_counts()
    
    fig = go.Figure(data=[
        go.Pie(
            labels=type_dist.index,
            values=type_dist.values,
            hole=0.4,
            marker=dict(colors=['#ff4444', '#ff8844'])
        )
    ])
    
    fig.update_layout(
        title='Fraud Distribution by Transaction Type',
        template='plotly_dark'
    )
    
    return fig


def create_transactions_table(transactions):
    """Tạo bảng transactions"""
    if not transactions:
        return html.Div("No transactions", style={'color': '#888'})
    
    rows = []
    for tx in transactions[:20]:
        predicted = tx.get('predicted_fraud', 0)
        bg_color = '#2d1f1f' if predicted == 1 else 'transparent'
        
        rows.append(
            html.Tr([
                html.Td(tx.get('tx_id', '')),
                html.Td(tx.get('tx_type', '')),
                html.Td(f"${tx.get('amount', 0):,.2f}"),
                html.Td(tx.get('from_account', '')[:10] + '...'),
                html.Td(f"{tx.get('fraud_prob_pct', 0):.1f}%"),
                html.Td('🚨 FRAUD' if predicted == 1 else '✅ OK',
                       style={'color': '#ff4444' if predicted == 1 else '#00cc66',
                              'fontWeight': 'bold'})
            ], style={'backgroundColor': bg_color})
        )
    
    return html.Table([
        html.Thead(html.Tr([
            html.Th('TX ID'),
            html.Th('Type'),
            html.Th('Amount'),
            html.Th('From'),
            html.Th('Fraud Prob'),
            html.Th('Status')
        ])),
        html.Tbody(rows)
    ], style={'width': '100%', 'borderCollapse': 'collapse'})


def create_fraud_alerts_html(alerts):
    """Tạo HTML cho fraud alerts"""
    if not alerts:
        return html.Div("No fraud detected ✅", style={'color': '#00cc66'})
    
    alert_divs = []
    for alert in alerts[:10]:
        alert_divs.append(
            html.Div([
                html.Div([
                    html.Strong(f"TX #{alert.get('tx_id', 'N/A')}", 
                               style={'fontSize': '1.2em', 'color': '#ff4444'}),
                    html.Span(f" - {alert.get('transaction_time', '')}", 
                             style={'color': '#888', 'marginLeft': '10px'})
                ]),
                html.Div([
                    html.Span(f"Type: {alert.get('tx_type', '')}", 
                             style={'marginRight': '20px'}),
                    html.Span(f"Amount: ${alert.get('amount', 0):,.2f}", 
                             style={'marginRight': '20px'}),
                    html.Span(f"Probability: {alert.get('fraud_prob_pct', 0):.1f}%",
                             style={'color': '#ff4444', 'fontWeight': 'bold'})
                ], style={'margin': '10px 0'}),
                html.Div([
                    html.Span(f"From: {alert.get('from_account', '')}", 
                             style={'marginRight': '20px'}),
                    html.Span(f"To: {alert.get('to_account', '')}")
                ], style={'fontSize': '0.9em', 'color': '#aaa'})
            ], className='fraud-alert')
        )
    
    return html.Div(alert_divs)


def create_stats_html(stats):
    """Tạo HTML cho statistics"""
    if not stats:
        return html.Div("No statistics available", style={'color': '#888'})
    
    items = []
    for key, value in stats.items():
        items.append(
            html.Div([
                html.Strong(f"{key}: "),
                html.Span(str(value))
            ], style={'margin': '10px 0'})
        )
    
    return html.Div(items)


def create_historical_chart():
    """Tạo biểu đồ historical performance"""
    # Placeholder - can be extended with actual historical data
    fig = go.Figure()
    fig.update_layout(
        title='Historical Performance (Coming Soon)',
        template='plotly_dark'
    )
    return fig


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    print("="*80)
    print("🚀 Starting Real-time Dashboard")
    print("="*80)
    print("Dashboard URL: http://localhost:8050")
    print("Press Ctrl+C to stop")
    print("="*80)
    
    app.run_server(debug=True, host='0.0.0.0', port=8050)
