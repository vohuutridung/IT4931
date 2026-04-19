# IT4931 - Hệ thống xử lý dữ liệu mạng xã hội thời thực

> Pipeline Kafka + Spark Streaming để xử lý dữ liệu mạng xã hội (Reddit, Instagram, Facebook) theo thời thực.

## 📊 Tổng quan dự án

Một **pipeline streaming từ đầu đến cuối** hoàn chỉnh:
1. **Thu thập**: Đọc dữ liệu từ nhiều nguồn mạng xã hội
2. **Chuẩn hóa**: Chuyển đổi sang định dạng Avro thống nhất
3. **Lưu trữ tạm**: Lưu trong Kafka để tách rời
4. **Xử lý**: Biến đổi dữ liệu thời thực với Spark Streaming
5. **Đầu ra**: Hasil lưu trong Parquet (sẵn sàng phân tích)

```
📁 Tệp dữ liệu
   ↓ (Thu thập)
📨 Topic Kafka (social-raw-batch, social-raw-realtime)
   ↓ (Consumer Spark)
⚡ Spark Streaming (XỬ LÝ ← MỚI)
   ├─ Giải mã Avro
   ├─ Biến đổi (engagement_score, metrics)
   ├─ Tổng hợp (cửa sổ 10 phút)
   └─ Loại bỏ trùng lặp
   ↓ (Nhiều Sinks)
💾 Tệp Parquet (sẵn sàng phân tích)
📺 Đầu ra Console (giám sát)
📤 Topic Kafka (xử lý tiếp theo)
```

---

## 🔧 Yêu cầu trước

### **Yêu cầu hệ thống:**
- Linux/macOS (hoặc WSL2 trên Windows)
- Docker & Docker Compose
- Git

### **Phần mềm:**
- Python 3.11+
- Java 8+
- Conda (khuyên dùng)

---

## 📦 Hướng dẫn cài đặt

### **1. Clone & Điều hướng**

```bash
cd /home/dungtt/Code/BigData/IT4931
git checkout spark    # Chuyển sang branch spark (nếu đang ở main)
```

### **2. Tạo môi trường Python**

#### **Tùy chọn A: Sử dụng Conda (Khuyên dùng)**

```bash
# Tạo từ tệp
conda env create -f environment.yml

# Kích hoạt
conda activate it4931-streaming

# Xác minh (tất cả 6 kiểm tra phải PASS ✅)
python verify_environment.py
```

#### **Tùy chọn B: Cài đặt tự động**

```bash
chmod +x setup_environment.sh
./setup_environment.sh
python verify_environment.py
```

### **3. Khởi động dịch vụ Kafka**

```bash
# Khởi động các container Docker
docker-compose -f docker-compose.simplified.yml up -d

# Đợi 30-45 giây, sau đó xác minh (tất cả phải là "healthy")
docker-compose -f docker-compose.simplified.yml ps
```

---

## 🚀 Chạy toàn bộ Pipeline

### **Bước 1: Thu thập dữ liệu → Kafka**

```bash
conda activate it4931-streaming

# Chạy thử (không gửi dữ liệu)
python -m ingestion.main --dry-run

# Chạy thực tế (gửi dữ liệu tới Kafka)
python -m ingestion.main
```

**Điều gì xảy ra:**
- Đọc tệp từ `data/reddit_data/`, `data/instagram_data/`, `data/facebook_data/`
- Chuẩn hóa sang schema Avro thống nhất
- Gửi tới các topic Kafka

### **Bước 2: Xử lý với Spark Streaming**

```bash
# Xóa đầu ra trước đó
rm -rf /tmp/streaming-output/raw/*
rm -rf /tmp/spark-checkpoints/parquet/*

# Chạy job Spark (xử lý tin nhắn Kafka)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
  --driver-memory 4g \
  --conf spark.pyspark.python=$(which python) \
  --conf spark.pyspark.driver.python=$(which python) \
  streaming/main.py
```

**Điều gì xảy ra:**
- Đọc 10.000+ tin nhắn từ Kafka ✅
- Giải mã định dạng Avro
- Áp dụng các phép biến đổi (engagement_score, metrics, deduplication)
- Nhóm vào các cửa sổ 10 phút
- Đưa ra tệp Parquet
- Tạo các checkpoint để khôi phục

**Đầu ra kỳ vọng:**
```
26/04/20 02:36:33 INFO MicroBatchExecution: Streaming query made progress: {
  "numInputRows" : 10034,
  "processedRowsPerSecond" : 317.69,
  ...
}
```

### **Bước 3: Giám sát tiến độ**

Trong terminal khác khi Spark đang chạy:

```bash
# Theo dõi các tệp được tạo
watch -n 2 'ls -lh /tmp/streaming-output/raw/ && echo && python -c "import pandas as pd; df = pd.read_parquet(\"/tmp/streaming-output/raw/\"); print(f\"Records: {len(df):,}\")"'

# Dừng Spark: Nhấn Ctrl+C trong terminal Spark
```

---

## 📊 Xác minh kết quả

### **Kiểm tra dữ liệu đã xử lý**

```bash
python << 'EOF'
import pandas as pd

df = pd.read_parquet('/tmp/streaming-output/raw/')

print("=== KẾT QUẢ XỬ LÝ SPARK ===")
print(f"✅ Tổng số bản ghi: {len(df):,}")
print(f"✅ Các cột: {df.columns.tolist()}")
print(f"✅ Dung lượng: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

print("\n📊 Theo Topic:")
for topic in df['topic'].unique():
    count = len(df[df['topic'] == topic])
    print(f"  {topic}: {count:,}")

print(f"\n⏰ Phạm vi thời gian:")
print(f"  Từ: {df['timestamp'].min()}")
print(f"  Tới: {df['timestamp'].max()}")
EOF
```

### **Chạy các bài kiểm tra**

```bash
# Kiểm tra tích hợp (6 bài kiểm tra)
python test_integration.py

# Kỳ vọng: Tất cả PASS ✅
```

---

## 🏗️ Chi tiết kiến trúc

### **Lớp Thu thập dữ liệu** (thư mục ingestion/)
- Đọc từ nhiều nguồn (Reddit, Instagram, Facebook)
- Chuẩn hóa sang định dạng thống nhất
- Gửi tới các topic Kafka thông qua serialization Avro

### **Lớp Streaming** (thư mục streaming/) – MỚI ⭐
```
config/          - Cấu hình Spark & Kafka
processors/      - Công cụ biến đổi dữ liệu
  ├─ social_processor.py: engagement_score, deduplication
  └─ engagement_agg.py: tổng hợp cửa sổ
sinks/           - Xử lý đầu ra
  ├─ parquet_sink.py: Lưu trữ phân tích
  ├─ kafka_sink.py: Xử lý tiếp theo
  └─ console_sink.py: Giám sát thời thực
utils/           - Tiện ích trợ giúp
  ├─ avro_helper.py: Giải mã Avro
  └─ metrics.py: Số liệu hiệu suất
main.py          - Chỉnh phối job streaming chính
```

---

## 📈 Hiệu suất

Được kiểm tra với 10.034 bản ghi Reddit:

| Chỉ số | Kết quả |
|--------|--------|
| Tốc độ xử lý | 317,7 bản ghi/giây |
| Tổng thời gian | 31,5 giây |
| Đầu ra dữ liệu | 6,26 MB (Parquet) |
| Trạng thái kiểm tra | ✅ Tất cả PASS |

---

## 🛠️ Khắc phục sự cố

### **Docker từ chối quyền truy cập**
```bash
sudo usermod -aG docker $USER
newgrp docker
```

### **Spark không thể tìm thấy Kafka**
```bash
# Đảm bảo sử dụng spark-submit với cờ --packages (đã có trong các lệnh ở trên)
# Hoặc cài đặt theo cách thủ công:
pip install pyspark[sql] confluent-kafka[avro]
```

### **Không có dữ liệu trong đầu ra Parquet**
```bash
# Kiểm tra Kafka có tin nhắn không
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic social-raw-batch \
  --from-beginning \
  --max-messages 1

# Nếu trống, chạy thu thập trước:
python -m ingestion.main
```

---

## 📚 Tài liệu bổ sung

- [PIPELINE_GUIDE_VI.md](PIPELINE_GUIDE_VI.md) - Hướng dẫn tiếng Việt chi tiết
- [DATA_LOCATIONS.md](DATA_LOCATIONS.md) - Tham chiếu vị trí lưu trữ dữ liệu
- [WORK_SUMMARY.md](WORK_SUMMARY.md) - Tóm tắt dự án hoàn chỉnh
- [streaming/README.md](streaming/README.md) - Tài liệu mô-đun Spark

---

## ✅ Danh sách kiểm tra bắt đầu nhanh

- [ ] `conda env create -f environment.yml`
- [ ] `conda activate it4931-streaming`
- [ ] `docker-compose -f docker-compose.simplified.yml up -d`
- [ ] Chờ các dịch vụ lành mạnh
- [ ] `python -m ingestion.main`
- [ ] `spark-submit ... streaming/main.py`
- [ ] Chờ 30+ giây để xử lý
- [ ] Nhấn Ctrl+C để dừng Spark
- [ ] `python verify_environment.py` ✅
- [ ] Kiểm tra `/tmp/streaming-output/raw/` để xem kết quả

**Tất cả xong? Pipeline của bạn đang hoạt động! 🎉**