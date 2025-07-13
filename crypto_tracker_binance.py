import requests
import pandas as pd
from datetime import datetime
import asyncio
import telegram
from telegram.request import HTTPXRequest
import os
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Tải biến môi trường từ file .env
load_dotenv()

# Cấu hình logging
logging.basicConfig(
    filename='crypto_tracker.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Cấu hình Telegram từ file .env
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Kiểm tra Telegram config
if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.error("TELEGRAM_TOKEN or CHAT_ID is missing in .env file")
    raise ValueError("TELEGRAM_TOKEN or CHAT_ID is missing in .env file")

bot = telegram.Bot(
    token=TELEGRAM_TOKEN,
    request=HTTPXRequest(
        connection_pool_size=16,
        pool_timeout=20,
        read_timeout=20,
        connect_timeout=20
    )
)

# Hàm lấy dữ liệu từ Binance
async def get_crypto_data():
    try:
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        result = {}
        
        coin = 'bitcoin'
        url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=21"
        response = session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        if not data or isinstance(data, dict):
            logging.error(f"Invalid response for {coin}: {data}")
            return None, f"Invalid response for {coin}"
        
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignored'
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['volume'] = df['volume'].astype(float)  # Khối lượng giao dịch
        df['close'] = df['close'].astype(float)   # Giá đóng cửa
        result[coin] = df[['timestamp', 'volume', 'close']]
        
        logging.info(f"API response for {coin}: {data[:2]}")
        return result, None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logging.error("Rate limit exceeded for Binance API.")
            return None, "Rate limit exceeded. Please try again later."
        logging.error(f"HTTP error fetching data: {str(e)}")
        return None, str(e)
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        return None, str(e)

# Hàm lưu dữ liệu vào CSV
async def save_to_csv(df, coin, volume_15m, ma20_volume, volume_ratio_percent, price, price_change_percent):
    try:
        file_path = f'{coin}_data.csv'
        file_exists = os.path.isfile(file_path)
        # Lưu khối lượng 15 phút, MA20 khối lượng, tỷ lệ, giá, và biến động giá
        latest_data = pd.DataFrame([{
            'timestamp': df['timestamp'].iloc[-1],
            'volume_15m': volume_15m,
            'ma20_volume': ma20_volume,
            'volume_ratio_percent': volume_ratio_percent,
            'price': price,
            'price_change_percent': price_change_percent
        }])
        latest_data.to_csv(file_path, mode='a', header=not file_exists, index=False, encoding='utf-8')
        logging.info(f"Data saved to {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error saving {coin} to CSV: {str(e)}")
        return str(e)

# Hàm phân tích khối lượng và giá
async def analyze_data(df, coin):
    try:
        if len(df) < 21:
            logging.warning(f"Not enough data points for {coin} to calculate MA20.")
            return None, None, None, None, f"Not enough data points for {coin}"
        
        # Lấy khối lượng và giá khung 15 phút gần nhất
        volume_15m = df['volume'].iloc[-1]
        price = df['close'].iloc[-1]
        
        # Xử lý khối lượng bất thường
        if volume_15m == 0:
            valid_volumes = df['volume'][df['volume'] > 0]
            if not valid_volumes.empty:
                volume_15m = valid_volumes.iloc[-1]
                logging.warning(f"Used fallback volume for {coin}: {volume_15m}")
            else:
                logging.warning(f"No valid volume for {coin}")
                return None, None, None, None, f"No valid volume for {coin}"
        
        # Tính MA20 khối lượng (20 khung 15 phút trước)
        ma20_volume = df['volume'].iloc[:-1].tail(20).mean()
        
        if pd.isna(ma20_volume) or ma20_volume == 0:
            logging.warning(f"Invalid MA20 volume for {coin}")
            return volume_15m, None, price, None, f"Invalid MA20 volume for {coin}"
        
        # Tính tỷ lệ phần trăm khối lượng
        volume_ratio_percent = (volume_15m / ma20_volume) * 100
        
        # Tính biến động giá so với 15 phút trước
        previous_price = df['close'].iloc[-2]
        if pd.isna(previous_price) or previous_price == 0:
            logging.warning(f"Invalid previous price for {coin}")
            return volume_15m, volume_ratio_percent, price, None, f"Invalid previous price for {coin}"
        
        price_change_percent = ((price - previous_price) / previous_price) * 100
        
        return volume_15m, volume_ratio_percent, price, price_change_percent, None
    except Exception as e:
        logging.error(f"Error analyzing {coin} data: {str(e)}")
        return None, None, None, None, str(e)

# Hàm gửi thông báo qua Telegram
async def send_notification(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
        logging.info(f"Notification sent: {message}")
    except Exception as e:
        logging.error(f"Error sending notification: {str(e)}")

# Hàm chính của bot
async def run_bot():
    logging.info("Running bot cycle...")
    data, fetch_error = await get_crypto_data()
    
    if fetch_error:
        await send_notification(f"Error: Failed to fetch data: {fetch_error}")
        if "Rate limit exceeded" in fetch_error:
            logging.info("Pausing for 300 seconds due to rate limit...")
            await asyncio.sleep(300)  # Tạm dừng 5 phút nếu vượt rate limit
        return
    
    coin = 'bitcoin'
    df = data.get(coin)
    if df is not None:
        # Phân tích dữ liệu
        volume_15m, volume_ratio_percent, price, price_change_percent, analyze_error = await analyze_data(df, coin)
        
        if analyze_error:
            logging.warning(f"Analysis skipped for {coin}: {analyze_error}")
            return
        
        if volume_15m is not None and volume_ratio_percent is not None and price is not None:
            # Lưu dữ liệu
            ma20_volume = df['volume'].iloc[:-1].tail(20).mean()
            save_error = await save_to_csv(df, coin, volume_15m, ma20_volume, volume_ratio_percent, price, price_change_percent)
            if save_error:
                await send_notification(f"Error: Failed to save {coin} data: {save_error}")
                return
            
            # Gửi thông báo nếu volume_ratio_percent > 200% hoặc price_change_percent > 1%
            if volume_ratio_percent > 200 or (price_change_percent is not None and price_change_percent > 1):
                message = (
                    f"📊 Bitcoin Volume Update:\n"
                    f"Volume (15m, {df['timestamp'].iloc[-1].strftime('%H:%M')}): {volume_15m:.2f}\n"
                    f"MA20 Volume (20 periods): {ma20_volume:.2f}\n"
                    f"Ratio: {volume_ratio_percent:.2f}%\n"
                    f"Price: ${price:.2f}\n"
                    f"Price Change: {price_change_percent:.2f}%\n"
                    f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                await send_notification(message)
                print(message)
            else:
                logging.info(f"No notification sent for {coin}: Ratio {volume_ratio_percent:.2f}% <= 200% and Price Change {price_change_percent:.2f}% <= 1%")
    
# Hàm chạy bot theo lịch
async def schedule_bot():
    while True:
        await run_bot()
        await asyncio.sleep(900)  # Chạy mỗi 15 phút

# Hàm chính
async def main():
    print("Starting Crypto Tracking Bot...")
    logging.info("Crypto Tracking Bot started.")
    await send_notification("Bot started. Waiting for Bitcoin volume and price data to analyze.")
    await schedule_bot()

if __name__ == "__main__":
    asyncio.run(main())