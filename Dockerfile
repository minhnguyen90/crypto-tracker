# Sử dụng image Python chính thức
FROM python:3.11-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Sao chép requirements.txt và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép code và file .env
COPY . .

# Chạy bot
CMD ["python", "crypto_tracker_binance.py"]