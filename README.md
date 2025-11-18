# Dự án Big Data: Phát hiện gian lận & Phân tích rủi ro tài chính

## 1. Giới thiệu tổng quan

Dự án này xây dựng một hệ sinh thái dữ liệu lớn (Big Data) hoàn chỉnh, được container hóa bằng Docker, nhằm mục đích phân tích dữ liệu tài chính. Các chức năng chính bao gồm:

*   **Hệ thống phát hiện gian lận giao dịch theo thời gian thực**: Sử dụng Kafka để tiếp nhận dữ liệu và Spark Streaming để xử lý, phân tích và đưa ra cảnh báo ngay lập tức.
*   **Module phân tích rủi ro thị trường**: Cung cấp các công cụ và môi trường để phân tích dữ liệu tài chính lịch sử, tính toán các chỉ số rủi ro.

Toàn bộ nền tảng được xây dựng dựa trên cụm Hadoop (HDFS) cho lưu trữ phân tán, Spark cho xử lý dữ liệu, và môi trường Jupyter Notebook cho việc phân tích tương tác và phát triển mô hình học máy. Việc sử dụng `docker-compose.yml` giúp toàn bộ hệ thống trở nên linh hoạt, dễ dàng cài đặt và triển khai trên mọi môi trường.

## 2. Kiến trúc hệ thống

### 2.1. Sơ đồ kiến trúc và luồng dữ liệu

Hệ thống bao gồm các luồng dữ liệu chính sau:

*   **Luồng Real-time (Phát hiện gian lận):**
    1.  `Realtime Producer` (một script Python) giả lập và gửi các giao dịch tài chính liên tục đến một topic trên `Kafka`.
    2.  `Spark Streaming` (`Realtime Consumer`) lắng nghe topic này, xử lý các giao dịch đến trong các micro-batch.
    3.  Mỗi giao dịch được đưa qua một mô hình Machine Learning đã được huấn luyện để phân loại là "gian lận" hay "hợp lệ".
    4.  Kết quả được đưa ra (ví dụ: in ra log, lưu vào database, hoặc gửi cảnh báo).

*   **Luồng Batch (Huấn luyện mô hình):**
    1.  Dữ liệu lịch sử (ví dụ: `paysim_train.csv`) được lưu trữ trên `Hadoop HDFS`.
    2.  Từ `Jupyter Notebook`, Data Scientist sử dụng `PySpark` để đọc dữ liệu từ HDFS.
    3.  Thực hiện các bước tiền xử lý, khám phá dữ liệu và huấn luyện mô hình Machine Learning trên cụm Spark.
    4.  Mô hình sau khi huấn luyện sẽ được lưu lại để sử dụng cho luồng real-time.

### 2.2. Chi tiết các thành phần

*   **Tầng Ingestion & Streaming:**
    *   **Kafka & Zookeeper:** Đóng vai trò là hệ thống message broker, chịu trách nhiệm tiếp nhận và phân phối các luồng dữ liệu giao dịch theo thời gian thực một cách bền bỉ và có khả năng chịu lỗi.
*   **Tầng Lưu trữ Phân tán:**
    *   **Hadoop HDFS (`namenode`, `datanode1`, `datanode2`):** Hệ thống tệp tin phân tán, là nơi lưu trữ các bộ dữ liệu lớn, dữ liệu huấn luyện và dữ liệu lịch sử.
*   **Tầng Xử lý Dữ liệu:**
    *   **Apache Spark (`spark-master`, `spark-worker`):** Framework tính toán phân tán mạnh mẽ, được sử dụng cho cả xử lý batch (huấn luyện mô hình) và xử lý thời gian thực (Spark Streaming).
    *   **YARN (`resourcemanager`, `nodemanager1`, `historyserver`):** Trình quản lý tài nguyên của cụm Hadoop, chịu trách nhiệm cấp phát và giám sát tài nguyên (CPU, RAM) cho các ứng dụng Spark.
*   **Tầng Phân tích & Phát triển:**
    *   **Jupyter/PySpark Notebook (`pyspark-notebook`):** Môi trường làm việc chính của Data Scientist. Đây là một giao diện web tương tác, cho phép viết code Python, sử dụng PySpark để làm việc trực tiếp với dữ liệu trên cụm Spark và HDFS.
*   **Tầng Caching & Giao diện:**
    *   **Redis:** Cơ sở dữ liệu key-value trong bộ nhớ, có tốc độ truy xuất cực nhanh. Có thể được sử dụng để cache các tham số mô hình, ngưỡng phát hiện gian lận, hoặc dữ liệu nóng.
    *   **Nginx:** Reverse proxy, có thể được cấu hình để cung cấp một cổng truy cập duy nhất và thân thiện hơn cho các giao diện web của hệ thống.

## 3. Yêu cầu hệ thống

*   **Docker Engine** (phiên bản 20.10.0 trở lên)
*   **Docker Compose** (phiên bản 1.29.0 trở lên)
*   **Cấu hình khuyến nghị:**
    *   RAM: Tối thiểu 8GB (16GB để có hiệu năng tốt nhất)
    *   CPU: Tối thiểu 4 cores

## 4. Hướng dẫn cài đặt và khởi chạy

1.  **Clone repository** về máy tính của bạn.

2.  **Mở terminal** và di chuyển đến thư mục gốc của dự án (nơi chứa file `docker-compose.yml`).

3.  **Khởi chạy toàn bộ hệ thống** bằng lệnh sau:
    ```bash
    docker-compose up -d
    ```
    Lệnh này sẽ tự động build các image cần thiết và khởi chạy tất cả các service ở chế độ nền (detached mode).

4.  **Kiểm tra trạng thái và logs** để đảm bảo các service đã khởi động thành công:
    ```bash
    # Xem logs của tất cả các service
    docker-compose logs -f

    # Xem logs của một service cụ thể (ví dụ: pyspark-notebook)
    docker-compose logs -f pyspark-notebook
    ```

5.  **Truy cập các giao diện Web:**
    *   **Jupyter Notebook:** `http://localhost:8888`
    *   **Hadoop Namenode UI:** `http://localhost:9870`
    *   **YARN ResourceManager UI:** `http://localhost:8089`
    *   **Spark Master UI:** `http://localhost:8080`
    *   **Spark Worker UI:** `http://localhost:8081`
    *   **Hadoop History Server:** `http://localhost:8188`

## 5. Hướng dẫn sử dụng

### Tự động hóa trong Pyspark Notebook

Khi service `pyspark-notebook` khởi động, nó được cấu hình để tự động thực hiện một chuỗi các tác vụ chuẩn bị môi trường:
1.  Tải về các bộ dữ liệu cần thiết (`get_data.sh`).
2.  Cài đặt các thư viện Python được định nghĩa trong `requirements.txt`.
3.  Tải dữ liệu huấn luyện lên HDFS (`upload_to_hdfs.py`).
4.  Thực thi script `run_all.sh` để chạy các tác vụ xử lý khác.
5.  Khởi động Jupyter Notebook server, sẵn sàng cho việc phân tích.

Truy cập `http://localhost:8888` để bắt đầu làm việc với các notebook:
*   **Tiền xử lý dữ liệu:** `fraud_detection/preprocessing.ipynb`
*   **Huấn luyện mô hình:** `fraud_detection/train.ipynb`
*   **Phân tích rủi ro:** Các file trong thư mục `market_risk/`

### Dừng hệ thống

Để dừng toàn bộ các container, chạy lệnh:
```bash
docker-compose down
```
**Lưu ý:** Lệnh trên sẽ không xóa các volume chứa dữ liệu của bạn (như dữ liệu trên HDFS). Nếu bạn muốn xóa cả container và volume để làm sạch hoàn toàn, hãy dùng lệnh:
```bash
docker-compose down -v
```

## 6. Cấu trúc thư mục dự án

```
.
├── base/               # Dockerfile cơ sở cho các thành phần Hadoop
├── datanode/           # Cấu hình Docker cho Hadoop Datanode
├── hadoop.env          # Các biến môi trường cho cụm Hadoop
├── historyserver/      # Cấu hình Docker cho YARN History Server
├── namenode/           # Cấu hình Docker cho Hadoop Namenode
├── nginx/              # Cấu hình Nginx
├── nodemanager/        # Cấu hình Docker cho YARN Nodemanager
├── notebook/           # Chứa toàn bộ notebook, script Python và dữ liệu
│   ├── data/
│   ├── fraud_detection/
│   ├── market_risk/
│   └── ...
├── resourcemanager/    # Cấu hình Docker cho YARN ResourceManager
├── docker-compose.yml  # File chính để điều phối toàn bộ hệ thống
└── README.md           # File hướng dẫn này
```
