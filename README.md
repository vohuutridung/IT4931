```bash
# 1. Vào ingestion tạo .evn
cd ingestion

# 2. Chỉnh DATA_DIR trong .env cho đúng path máy

# 3. Khởi động Kafka stack (từ thư mục chứa docker-compose.yml)
docker-compose up -d

# 4. Chờ tất cả healthy (~45 giây), kiểm tra
docker-compose ps

# 5. Chạy ingestion
python -m ingestion.main --dry-run      # test trước
python -m ingestion.main                # gửi thật
```