"""
Real-time Fraud Detection Dashboard với Plotly Dash
"""

import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import plotly.express as px
from kafka import KafkaConsumer
import json
from datetime import datetime
from collections import deque
import threading
import pandas as pd
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "predictions"
MAX_POINTS = 100  # Số điểm tối đa trên biểu đồ

# Dữ liệu lưu trữ
data_store = {
    'timestamps': deque(maxlen=MAX_POINTS),
    'amounts': deque(maxlen=MAX_POINTS),
    'predictions': deque(maxlen=MAX_POINTS),
    'probabilities': deque(maxlen=MAX_POINTS),
    'types': deque(maxlen=MAX_POINTS),
    'actual_fraud': deque(maxlen=MAX_POINTS),
    'recent_transactions': deque(maxlen=20),  # 20 giao dịch gần nhất
    'fraud_count': 0,
    'total_count': 0,
    'total_amount': 0.0,
    'fraud_amount': 0.0,
    'true_positive': 0,
    'false_positive': 0,
    'true_negative': 0,
    'false_negative': 0
}

# Lock để đồng bộ
data_lock = threading.Lock()


def consume_kafka():
    """Background thread để consume Kafka messages"""
    logger.info(f"🔌 Kết nối tới Kafka: {KAFKA_BROKER}")
    logger.info(f"📥 Subscribe topic: {KAFKA_TOPIC}")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='dashboard-consumer'
    )
    
    logger.info("✅ Kafka consumer đã sẵn sàng")
    logger.info("⏳ Đang chờ messages...")
    
    for message in consumer:
        try:
            data = message.value
            
            with data_lock:
                # Thêm vào deque
                timestamp = datetime.now()
                data_store['timestamps'].append(timestamp)
                data_store['amounts'].append(data['amount'])
                data_store['predictions'].append(data['fraud_prediction'])
                data_store['probabilities'].append(data['fraud_probability'])
                data_store['types'].append(data['type'])
                data_store['actual_fraud'].append(data.get('isFraud', 0))
                
                # Thống kê
                data_store['total_count'] += 1
                data_store['total_amount'] += data['amount']
                
                if data['fraud_prediction'] == 1:
                    data_store['fraud_count'] += 1
                    data_store['fraud_amount'] += data['amount']
                
                # Confusion matrix
                pred = data['fraud_prediction']
                actual = data.get('isFraud', 0)
                
                if pred == 1 and actual == 1:
                    data_store['true_positive'] += 1
                elif pred == 1 and actual == 0:
                    data_store['false_positive'] += 1
                elif pred == 0 and actual == 0:
                    data_store['true_negative'] += 1
                elif pred == 0 and actual == 1:
                    data_store['false_negative'] += 1
                
                # Recent transactions
                transaction_info = {
                    'time': timestamp.strftime("%H:%M:%S"),
                    'type': data['type'],
                    'amount': data['amount'],
                    'from': data['nameOrig'],
                    'to': data['nameDest'],
                    'prediction': '🚨 FRAUD' if data['fraud_prediction'] == 1 else '✅ Normal',
                    'probability': data['fraud_probability'],
                    'actual': data.get('isFraud', 0)
                }
                data_store['recent_transactions'].append(transaction_info)
                
                # Log
                if data_store['total_count'] % 10 == 0:
                    logger.info(f"📊 Đã nhận {data_store['total_count']} transactions, "
                              f"Fraud: {data_store['fraud_count']}")
        
        except Exception as e:
            logger.error(f"❌ Lỗi khi xử lý message: {e}")


# Khởi động Kafka consumer thread
consumer_thread = threading.Thread(target=consume_kafka, daemon=True)
consumer_thread.start()

# Tạo Dash app
app = dash.Dash(__name__)
app.title = "Fraud Detection Dashboard"

# Layout
app.layout = html.Div([
    html.Div([
        html.H1("🔍 Real-time Fraud Detection Dashboard", 
                style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 20}),
    ]),
    
    # Metrics Row
    html.Div([
        html.Div([
            html.Div([
                html.H4("Total Transactions", style={'color': '#7f8c8d'}),
                html.H2(id='total-transactions', children="0", style={'color': '#3498db'})
            ], className='metric-card'),
        ], style={'width': '25%', 'display': 'inline-block', 'padding': '10px'}),
        
        html.Div([
            html.Div([
                html.H4("Fraud Detected", style={'color': '#7f8c8d'}),
                html.H2(id='fraud-count', children="0", style={'color': '#e74c3c'})
            ], className='metric-card'),
        ], style={'width': '25%', 'display': 'inline-block', 'padding': '10px'}),
        
        html.Div([
            html.Div([
                html.H4("Fraud Rate", style={'color': '#7f8c8d'}),
                html.H2(id='fraud-rate', children="0%", style={'color': '#e67e22'})
            ], className='metric-card'),
        ], style={'width': '25%', 'display': 'inline-block', 'padding': '10px'}),
        
        html.Div([
            html.Div([
                html.H4("Total Amount", style={'color': '#7f8c8d'}),
                html.H2(id='total-amount', children="$0", style={'color': '#27ae60'})
            ], className='metric-card'),
        ], style={'width': '25%', 'display': 'inline-block', 'padding': '10px'}),
    ]),
    
    # Charts Row 1
    html.Div([
        html.Div([
            dcc.Graph(id='transaction-timeline')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        
        html.Div([
            dcc.Graph(id='fraud-distribution')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
    ]),
    
    # Charts Row 2
    html.Div([
        html.Div([
            dcc.Graph(id='amount-distribution')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        
        html.Div([
            dcc.Graph(id='confusion-matrix')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
    ]),
    
    # Recent Transactions Table
    html.Div([
        html.H3("📋 Recent Transactions", style={'color': '#2c3e50', 'marginTop': 30}),
        html.Div(id='recent-transactions-table', style={'overflowX': 'auto'})
    ], style={'padding': '20px'}),
    
    # Auto-refresh interval
    dcc.Interval(
        id='interval-component',
        interval=1000,  # Update every 1 second
        n_intervals=0
    )
], style={'backgroundColor': '#ecf0f1', 'padding': '20px'})


# Callbacks
@app.callback(
    [Output('total-transactions', 'children'),
     Output('fraud-count', 'children'),
     Output('fraud-rate', 'children'),
     Output('total-amount', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_metrics(n):
    with data_lock:
        total = data_store['total_count']
        fraud = data_store['fraud_count']
        fraud_rate = (fraud / total * 100) if total > 0 else 0
        amount = data_store['total_amount']
        
        return (
            f"{total:,}",
            f"{fraud:,}",
            f"{fraud_rate:.2f}%",
            f"${amount:,.2f}"
        )


@app.callback(
    Output('transaction-timeline', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_timeline(n):
    with data_lock:
        timestamps = list(data_store['timestamps'])
        amounts = list(data_store['amounts'])
        predictions = list(data_store['predictions'])
    
    if not timestamps:
        return go.Figure()
    
    # Tạo 2 traces: Normal và Fraud
    normal_mask = [p == 0 for p in predictions]
    fraud_mask = [p == 1 for p in predictions]
    
    normal_times = [t for t, m in zip(timestamps, normal_mask) if m]
    normal_amounts = [a for a, m in zip(amounts, normal_mask) if m]
    
    fraud_times = [t for t, m in zip(timestamps, fraud_mask) if m]
    fraud_amounts = [a for a, m in zip(amounts, fraud_mask) if m]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=normal_times, y=normal_amounts,
        mode='markers',
        name='Normal',
        marker=dict(color='#3498db', size=8)
    ))
    
    fig.add_trace(go.Scatter(
        x=fraud_times, y=fraud_amounts,
        mode='markers',
        name='Fraud',
        marker=dict(color='#e74c3c', size=12, symbol='diamond')
    ))
    
    fig.update_layout(
        title='Transaction Timeline',
        xaxis_title='Time',
        yaxis_title='Amount ($)',
        hovermode='closest',
        showlegend=True,
        height=400
    )
    
    return fig


@app.callback(
    Output('fraud-distribution', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_fraud_distribution(n):
    with data_lock:
        types = list(data_store['types'])
        predictions = list(data_store['predictions'])
    
    if not types:
        return go.Figure()
    
    df = pd.DataFrame({'type': types, 'prediction': predictions})
    df_grouped = df.groupby(['type', 'prediction']).size().reset_index(name='count')
    
    fig = px.bar(
        df_grouped,
        x='type',
        y='count',
        color='prediction',
        barmode='group',
        color_discrete_map={0: '#3498db', 1: '#e74c3c'},
        labels={'prediction': 'Fraud', 'count': 'Count', 'type': 'Transaction Type'},
        title='Fraud Distribution by Transaction Type'
    )
    
    fig.update_layout(height=400)
    
    return fig


@app.callback(
    Output('amount-distribution', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_amount_distribution(n):
    with data_lock:
        amounts = list(data_store['amounts'])
        predictions = list(data_store['predictions'])
    
    if not amounts:
        return go.Figure()
    
    df = pd.DataFrame({'amount': amounts, 'prediction': predictions})
    df['category'] = df['prediction'].map({0: 'Normal', 1: 'Fraud'})
    
    fig = px.box(
        df,
        x='category',
        y='amount',
        color='category',
        color_discrete_map={'Normal': '#3498db', 'Fraud': '#e74c3c'},
        title='Amount Distribution: Normal vs Fraud',
        labels={'amount': 'Amount ($)', 'category': 'Category'}
    )
    
    fig.update_layout(height=400, showlegend=False)
    
    return fig


@app.callback(
    Output('confusion-matrix', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_confusion_matrix(n):
    with data_lock:
        tp = data_store['true_positive']
        fp = data_store['false_positive']
        tn = data_store['true_negative']
        fn = data_store['false_negative']
    
    # Confusion matrix
    cm = [[tn, fp], [fn, tp]]
    
    fig = go.Figure(data=go.Heatmap(
        z=cm,
        x=['Predicted Normal', 'Predicted Fraud'],
        y=['Actual Normal', 'Actual Fraud'],
        colorscale='RdYlGn_r',
        text=cm,
        texttemplate='%{text}',
        textfont={"size": 20},
        showscale=True
    ))
    
    fig.update_layout(
        title='Confusion Matrix',
        height=400,
        xaxis={'side': 'bottom'}
    )
    
    return fig


@app.callback(
    Output('recent-transactions-table', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_recent_transactions(n):
    with data_lock:
        transactions = list(data_store['recent_transactions'])
    
    if not transactions:
        return html.P("No transactions yet...", style={'color': '#7f8c8d'})
    
    # Tạo DataFrame
    df = pd.DataFrame(transactions)
    
    # Tạo HTML table
    table_header = [
        html.Thead(html.Tr([html.Th(col) for col in ['Time', 'Type', 'Amount', 'From', 'To', 'Prediction', 'Probability']]))
    ]
    
    rows = []
    for _, row in df.iterrows():
        color = '#ffe6e6' if '🚨' in row['prediction'] else '#e6f7ff'
        rows.append(html.Tr([
            html.Td(row['time']),
            html.Td(row['type']),
            html.Td(f"${row['amount']:,.2f}"),
            html.Td(row['from'][:10] + '...'),
            html.Td(row['to'][:10] + '...'),
            html.Td(row['prediction'], style={'fontWeight': 'bold'}),
            html.Td(f"{row['probability']:.3f}")
        ], style={'backgroundColor': color}))
    
    table_body = [html.Tbody(rows)]
    
    return html.Table(
        table_header + table_body,
        style={
            'width': '100%',
            'borderCollapse': 'collapse',
            'border': '1px solid #ddd'
        },
        className='table'
    )


# CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .metric-card {
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                text-align: center;
            }
            table {
                font-family: Arial, sans-serif;
            }
            table th {
                background-color: #3498db;
                color: white;
                padding: 12px;
                text-align: left;
            }
            table td {
                padding: 10px;
                border-bottom: 1px solid #ddd;
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


if __name__ == '__main__':
    logger.info("="*80)
    logger.info("🚀 STARTING FRAUD DETECTION DASHBOARD")
    logger.info("="*80)
    logger.info("📊 Dashboard URL: http://localhost:8050")
    logger.info("💡 Mở trình duyệt và truy cập: http://localhost:8050")
    logger.info("")
    
    app.run_server(debug=False, host='0.0.0.0', port=8050)
