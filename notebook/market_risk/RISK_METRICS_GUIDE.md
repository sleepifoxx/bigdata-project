# Hướng dẫn Phân tích Rủi ro Thị trường Chứng khoán

## Tổng quan

Spark Consumer đã được nâng cấp để tính toán các chỉ số phân tích thị trường và rủi ro nâng cao, giúp đánh giá toàn diện tình hình giao dịch và mức độ rủi ro của từng mã chứng khoán.

## Các Chỉ Số Được Tính Toán

### 1. Chỉ Số Cơ Bản (Basic Metrics)

| Chỉ số | Mô tả | Cách tính |
|--------|-------|-----------|
| **price_change** | Thay đổi giá tuyệt đối | `price - ref_price` |
| **price_change_percent** | Thay đổi giá % | `(price - ref_price) / ref_price * 100` |
| **foreign_net** | Dòng tiền nước ngoài ròng | `foreign_buy - foreign_sell` |
| **spread** | Chênh lệch bid-ask | `ask_price - bid_price` |
| **spread_percent** | Chênh lệch bid-ask % | `spread / bid_price * 100` |
| **avg_price** | Giá giao dịch bình quân | `total_value / total_volume` |
| **price_position** | Vị trí giá (0-100%) | `(price - floor) / (ceiling - floor) * 100` |
| **foreign_ownership_pct** | % sở hữu nước ngoài | `(total_room - current_room) / total_room * 100` |
| **liquidity_value** | Giá trị thanh khoản (triệu VND) | `total_volume * avg_price` |

### 2. Chỉ Số Thị Trường (Market Metrics)

#### 2.1 Volatility Intraday
```python
volatility_intraday = (high - low) / ref_price * 100
```
**Ý nghĩa:** Đo lường độ biến động giá trong ngày
- **< 2%**: Biến động thấp (LOW)
- **2-5%**: Biến động trung bình (MEDIUM)  
- **> 5%**: Biến động cao (HIGH)

#### 2.2 Price Momentum
```python
price_momentum = (price - prior_close) / prior_close * 100
```
**Ý nghĩa:** Xu hướng giá so với phiên trước
- **Dương**: Xu hướng tăng
- **Âm**: Xu hướng giảm

#### 2.3 Order Imbalance
```python
order_imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
```
**Ý nghĩa:** Mất cân bằng cung cầu
- **> 0**: Áp lực mua mạnh hơn
- **< 0**: Áp lực bán mạnh hơn
- **Gần 0**: Cân bằng

#### 2.4 Liquidity Score
```python
liquidity_score = total_volume / 1,000,000
```
**Ý nghĩa:** Điểm thanh khoản (tính bằng triệu cổ phiếu)

#### 2.5 Foreign Pressure
```python
foreign_pressure = foreign_net / total_volume * 100
```
**Ý nghĩa:** Áp lực mua/bán của khối ngoại (% khối lượng)

#### 2.6 Price Efficiency
```python
price_efficiency = spread / price * 100
```
**Ý nghĩa:** Hiệu quả định giá (spread càng nhỏ càng hiệu quả)

### 3. Chỉ Số Rủi Ro (Risk Metrics)

#### 3.1 Volatility Risk Level
**Phân loại rủi ro biến động:**
- **HIGH**: Volatility > 5%
- **MEDIUM**: Volatility 2-5%
- **LOW**: Volatility < 2%

#### 3.2 Liquidity Risk Score (1-3)
**Đánh giá rủi ro thanh khoản:**
- **3 (High Risk)**: Giá trị thanh khoản < 10 tỷ VND
- **2 (Medium Risk)**: Giá trị thanh khoản 10-50 tỷ VND
- **1 (Low Risk)**: Giá trị thanh khoản > 50 tỷ VND

#### 3.3 Price Risk Position
**Vị trí giá và rủi ro:**
- **OVERBOUGHT**: Giá > 95% khoảng floor-ceiling
- **OVERSOLD**: Giá < 5% khoảng floor-ceiling
- **NORMAL**: Giá trong khoảng 5-95%

#### 3.4 Foreign Flow Risk
**Dòng tiền nước ngoài:**
- **HEAVY_SELL**: Bán ròng > 1 triệu CP
- **SELLING**: Bán ròng nhẹ
- **NEUTRAL**: Cân bằng
- **BUYING**: Mua ròng nhẹ
- **HEAVY_BUY**: Mua ròng > 1 triệu CP

#### 3.5 Spread Risk Score (1-3)
**Rủi ro từ chênh lệch giá:**
- **3 (High)**: Spread > 1% giá
- **2 (Medium)**: Spread 0.5-1% giá
- **1 (Low)**: Spread < 0.5% giá

#### 3.6 Composite Risk Score (1-3)
```python
composite_risk_score = (liquidity_risk_score + spread_risk_score) / 2
```
**Điểm rủi ro tổng hợp:**
- **< 1.5**: Rủi ro thấp
- **1.5-2.5**: Rủi ro trung bình
- **> 2.5**: Rủi ro cao

### 4. Value at Risk (VaR) Metrics

#### 4.1 Max Loss Potential
```python
max_loss_potential = (price - floor) * position_size
```
**Ý nghĩa:** Tổn thất tối đa có thể xảy ra nếu giá chạm sàn (với 1000 CP)

#### 4.2 Max Gain Potential
```python
max_gain_potential = (ceiling - price) * position_size
```
**Ý nghĩa:** Lợi nhuận tối đa có thể đạt được nếu giá chạm trần (với 1000 CP)

#### 4.3 Downside Risk %
```python
downside_risk_pct = (price - floor) / price * 100
```
**Ý nghĩa:** % rủi ro giảm giá tối đa

#### 4.4 Upside Potential %
```python
upside_potential_pct = (ceiling - price) / price * 100
```
**Ý nghĩa:** % tiềm năng tăng giá tối đa

#### 4.5 Risk/Reward Ratio
```python
risk_reward_ratio = upside_potential_pct / downside_risk_pct
```
**Ý nghĩa:** Tỷ lệ lợi nhuận/rủi ro
- **< 1**: Rủi ro > tiềm năng (không hấp dẫn)
- **= 1**: Cân bằng
- **> 1**: Tiềm năng > rủi ro (hấp dẫn)
- **> 2**: Rất hấp dẫn

## Output Streams

### Stream 1: Detailed Metrics (15s interval)
Hiển thị tất cả metrics cho mỗi mã chứng khoán:
- Symbol, Company Name
- Price, Change %, Volume
- Liquidity Value
- Volatility & Risk Level
- Foreign Net Flow & Risk
- Spread %, Risk Score
- Price Position
- Risk/Reward Ratio

### Stream 2: Windowed Aggregations (30s interval)
Tổng hợp theo window 1 phút:
- Giá trung bình, max, min
- Tổng khối lượng
- Volatility trung bình
- Risk score trung bình
- Độ lệch chuẩn giá (price volatility stddev)

### Stream 3: High Risk Summary (60s interval)
Cảnh báo các mã có rủi ro cao (risk score > 1.5):
- Sắp xếp theo risk score giảm dần
- Hiển thị volatility level
- Foreign flow status
- Price position

## Cách Sử dụng để Ra Quyết định

### Kịch bản 1: Tìm cơ hội mua an toàn
**Điều kiện:**
```
volatility_risk_level = LOW
liquidity_risk_score = 1
composite_risk_score < 1.5
risk_reward_ratio > 1.5
foreign_flow_risk = BUYING hoặc HEAVY_BUY
price_risk_position = NORMAL hoặc OVERSOLD
```

### Kịch bản 2: Cảnh báo bán
**Điều kiện:**
```
volatility_risk_level = HIGH
price_risk_position = OVERBOUGHT
foreign_flow_risk = SELLING hoặc HEAVY_SELL
composite_risk_score > 2
```

### Kịch bản 3: Đánh giá thanh khoản
**Điều kiện tốt:**
```
liquidity_value > 50,000 (> 50 tỷ)
spread_percent < 0.5
liquidity_risk_score = 1
```

### Kịch bản 4: Phân tích dòng tiền nước ngoài
```
foreign_pressure > 5%: Khối ngoại mua mạnh
foreign_net > 1,000,000: Dòng tiền lớn vào
foreign_ownership_pct tăng: Tích lũy dài hạn
```

## Thống kê Aggregation

### Volatility Stddev
```python
price_volatility_stddev = stddev(price_change_percent)
```
**Ý nghĩa:** 
- Độ lệch chuẩn của % thay đổi giá
- Càng cao = biến động càng không ổn định
- Dùng để tính VaR và đánh giá rủi ro

### Average Risk Score
```python
avg_risk_score = avg(composite_risk_score)
```
**Ý nghĩa:**
- Điểm rủi ro trung bình trong window
- Theo dõi xu hướng rủi ro tăng/giảm

## Ví dụ Phân tích Thực tế

### Mã VCB - Tốt, rủi ro thấp
```
Symbol: VCB
Price: 90,200
Change %: +1.32%
Volatility: 1.8% (LOW)
Risk Score: 1.2 (Low)
Liquidity Value: 202,588 triệu (>50 tỷ)
Foreign Net: +124,300 (BUYING)
Price Position: 65% (NORMAL)
Risk/Reward: 1.8 (Hấp dẫn)
```
**Đánh giá:** Cổ phiếu Blue chip, thanh khoản cao, rủi ro thấp, xu hướng tích cực

### Mã ABC - Cẩn trọng, rủi ro cao
```
Symbol: ABC
Price: 28,500
Change %: -3.5%
Volatility: 6.2% (HIGH)
Risk Score: 2.8 (High)
Liquidity Value: 8,500 triệu (<10 tỷ)
Foreign Net: -850,000 (HEAVY_SELL)
Price Position: 12% (OVERSOLD)
Risk/Reward: 0.6 (Không hấp dẫn)
```
**Đánh giá:** Thanh khoản thấp, biến động lớn, khối ngoại bán mạnh, cần thận trọng

## Monitoring & Alerts

### Critical Thresholds
```python
# Cảnh báo rủi ro cao
if composite_risk_score > 2.5:
    ALERT: "HIGH RISK"

# Cảnh báo thanh khoản thấp
if liquidity_value < 10000:
    ALERT: "LOW LIQUIDITY"

# Cảnh báo biến động lớn
if volatility_intraday > 7:
    ALERT: "EXTREME VOLATILITY"

# Cảnh báo khối ngoại bán mạnh
if foreign_net < -2000000:
    ALERT: "HEAVY FOREIGN SELLING"
```

## Best Practices

1. **Không dựa vào một chỉ số duy nhất** - Kết hợp nhiều metrics
2. **Theo dõi xu hướng** - Xem aggregation theo thời gian
3. **Đối chiếu với VN-Index** - So sánh với thị trường chung
4. **Phân tích dòng tiền** - Foreign flow là tín hiệu quan trọng
5. **Quản lý rủi ro** - Ưu tiên composite_risk_score < 2
6. **Thanh khoản là chìa khóa** - Tránh mã có liquidity_value < 10 tỷ

## Tích hợp với Dashboard

Các metrics này có thể được visualize bằng Dashboard (dashboard.py):
- Real-time price charts với risk bands
- Heatmap theo risk score
- Foreign flow tracking
- Volatility trends
- Risk/Reward scatter plot

---

**Lưu ý:** Các chỉ số này chỉ mang tính tham khảo. Quyết định đầu tư cần kết hợp phân tích cơ bản, tin tức thị trường và quản lý vốn hợp lý.
