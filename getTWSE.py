import requests
import sqlite3
import time
from datetime import datetime

# 初始化資料庫
def init_db():
    conn = sqlite3.connect("stock_data.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS YearlyData (
            stock_no TEXT,
            year INTEGER,
            highest_price REAL,
            highest_date TEXT,
            lowest_price REAL,
            lowest_date TEXT,
            average_close_price REAL,
            PRIMARY KEY (stock_no, year)
        )
    ''')
    conn.commit()
    conn.close()

# 讀取股票清單
def read_stock_list(filename):
    stock_list = []
    with open(filename, "r", encoding="utf-8") as file:
        for line in file:
            if line.strip().startswith("#"):
                continue  # 跳過註解行
            parts = line.strip().split()
            if len(parts) >= 2:
                stock_list.append((parts[0], parts[1]))  # (股票代號, 股票名稱)
    return stock_list

# 獲取資料庫中上一年度最大股票代號
def get_last_processed_stock(current_year):
    conn = sqlite3.connect("stock_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(stock_no) FROM YearlyData WHERE year = ?", (current_year - 1,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result and result[0] else None

# 抓取資料的函數
def fetch_stock_data(stock_no):
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMNPTK?stockNo={stock_no}&response=json"

    # 設置自訂 HTTP 標頭
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.twse.com.tw/",
    }

    # 發送請求並處理回應
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

    data = response.json()
    if not data or "tables" not in data or len(data["tables"]) == 0:
        raise Exception(f"No valid data for stock {stock_no}")

    # 提取年度數據部分
    tables = data["tables"]
    for table in tables:
        if "fields" in table and "年度" in table["fields"]:
            return table["data"]

    raise Exception(f"No annual trading data found for stock {stock_no}")

# 檢查資料是否已存在
def data_exists(stock_no, year):
    conn = sqlite3.connect("stock_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM YearlyData WHERE stock_no = ? AND year = ?", (stock_no, year))
    result = cursor.fetchone()
    conn.close()
    return result is not None

# 分析資料並儲存
def process_and_save_data(stock_no, data):
    current_year = datetime.now().year
    five_years_ago = current_year - 5

    # 過濾出最近 5 年的資料
    filtered_data = [
        row for row in data if five_years_ago <= (int(row[0]) + 1911) <= current_year
    ]

    if not filtered_data:
        raise Exception(f"No data available for stock {stock_no} in the last 5 years.")

    result = {}
    for row in filtered_data:
        year = int(row[0]) + 1911
        if data_exists(stock_no, year):
            print(f"Data for stock {stock_no} in year {year} already exists. Skipping.")
            continue

        high_price = float(row[4].replace(',', ''))  # 最高價
        high_date = row[5]          # 最高價日期
        low_price = float(row[6].replace(',', ''))   # 最低價
        low_date = row[7]           # 最低價日期
        avg_close_price = float(row[8].replace(',', ''))  # 平均收盤價

        result[year] = {
            "highest_price": high_price,
            "highest_date": high_date,
            "lowest_price": low_price,
            "lowest_date": low_date,
            "average_close_price": avg_close_price
        }

    # 儲存到資料庫
    conn = sqlite3.connect("stock_data.db")
    cursor = conn.cursor()

    for year, stats in result.items():
        cursor.execute('''
            INSERT OR REPLACE INTO YearlyData (
                stock_no, year, highest_price, highest_date, lowest_price, lowest_date, average_close_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            stock_no, year, stats["highest_price"], stats["highest_date"],
            stats["lowest_price"], stats["lowest_date"], stats["average_close_price"]
        ))

    conn.commit()
    conn.close()

# 主程式
if __name__ == "__main__":
    #init_db()
    stock_list = read_stock_list("twse.cfg")
    current_year = datetime.now().year
    last_processed_stock = get_last_processed_stock(current_year)

    for stock_no, stock_name in stock_list:
        if last_processed_stock and stock_no <= last_processed_stock:
            print(f"Skipping {stock_no} {stock_name}, already processed.")
            continue

        print(f"Fetching data for {stock_no} {stock_name}...")
        try:
            raw_data = fetch_stock_data(stock_no)
            process_and_save_data(stock_no, raw_data)
            print(f"Data for {stock_no} {stock_name} has been successfully saved.")
        except Exception as e:
            print(f"Error fetching data for {stock_no} {stock_name}: {e}")
            break
        # Pause for 1 seconds
        time.sleep(3)
