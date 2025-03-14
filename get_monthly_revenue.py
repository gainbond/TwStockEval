import pandas as pd
import requests
from io import StringIO
import sqlite3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# 定義下載 CSV 的函數
def fetch_csv_data(url, data):
    try:
        response = requests.post(url, data=data)
        response.encoding = 'utf-8'  # 確保編碼正確
        if response.status_code == 200:
            print("CSV 文件下載成功！")
            return response.text
        else:
            print(f"下載失敗，狀態碼: {response.status_code}")
            return None
    except Exception as e:
        print(f"下載過程中發生錯誤: {e}")
        return None

# 定義解析 CSV 的函數
def parse_csv(csv_text):
    try:
        csv_text = csv_text.lstrip("\ufeff")
        df = pd.read_csv(
            StringIO(csv_text),
            engine="python",
            sep=None,
            error_bad_lines=False,
            warn_bad_lines=True
        )
        print(f"CSV 文件成功解析！欄位名稱: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"解析過程中發生錯誤: {e}")
        return None

def process_data(df, stock_codes, report_month):
    try:
        # 指定所需的欄位名稱
        required_columns = ['公司代號', '營業收入-當月營收', '營業收入-去年同月增減(%)']
        column_mapping = {
            '公司代號': 'stock_no',
            '營業收入-當月營收': 'monthly_revenue',
            '營業收入-去年同月增減(%)': 'yoy_growth'
        }

        # 檢查是否包含所有必要欄位
        if not all(col in df.columns for col in required_columns):
            missing_columns = [col for col in required_columns if col not in df.columns]
            print(f"缺少以下必要欄位: {missing_columns}")
            return None

        # 重新命名欄位
        df = df.rename(columns=column_mapping)

        # 確保股票代碼與 CSV 的公司代號格式一致
        stock_codes = [code.strip() for code in stock_codes]
        df['stock_no'] = df['stock_no'].astype(str).str.strip()
        filtered_df = df[df['stock_no'].isin(stock_codes)].copy()
        filtered_df['revenue_month'] = report_month

        print(f"成功提取與指定股票代碼相關的資料，數量: {len(filtered_df)}")
        return filtered_df
    except Exception as e:
        print(f"處理過程中發生錯誤: {e}")
        return None

# 初始化 SQLite 資料表
def init_db(db_name, table_name):
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                stock_no TEXT,
                monthly_revenue REAL,
                yoy_growth REAL,
                revenue_month TEXT,
                PRIMARY KEY (stock_no, revenue_month)
            )
        """)
        conn.commit()
        conn.close()
        print(f"資料表 {table_name} 已初始化（若不存在則建立）。")
    except Exception as e:
        print(f"初始化資料表過程中發生錯誤: {e}")

# 儲存資料至 SQLite 的函數
def save_to_sqlite(db_name, table_name, df):
    try:
        if df.empty:
            print("警告: 嘗試保存的資料為空，略過保存步驟。")
            return

        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT OR REPLACE INTO {table_name} 
                (stock_no, monthly_revenue, yoy_growth, revenue_month)
                VALUES (?, ?, ?, ?)
            """, (
                row['stock_no'], 
                row['monthly_revenue'], 
                row['yoy_growth'], 
                row['revenue_month']
            ))

        conn.commit()
        conn.close()
        print(f"資料成功儲存至資料庫 {db_name} 的資料表 {table_name}，儲存筆數: {len(df)}")
    except Exception as e:
        print(f"儲存過程中發生錯誤: {e}")

# 讀取股票代碼清單
def read_stock_codes(config_file):
    try:
        with open(config_file, 'r') as file:
            stock_codes = [line.strip().split()[0] for line in file if line.strip()]
        print(f"成功讀取股票代碼: {stock_codes}")
        return stock_codes
    except Exception as e:
        print(f"讀取股票代碼過程中發生錯誤: {e}")
        return []

# 處理市場資料的函數
def handle_market_data(url, filepath, stock_codes, db_name, table_name, start_date):
    for i in range(6):
        report_date = start_date - relativedelta(months=i)
        year = report_date.year - 1911
        month = report_date.month
        file_name = f"t21sc03_{year}_{month}.csv"
        data = {
            'step': '9',
            'functionName': 'show_file2',
            'filePath': filepath,
            'fileName': file_name
        }

        print(f"正在處理檔案: {file_name}")
        csv_text = fetch_csv_data(url, data)
        if csv_text:
            df = parse_csv(csv_text)
            if df is not None:
                filtered_data = process_data(df, stock_codes, f"{report_date.year}-{report_date.month:02d}")
                if filtered_data is not None:
                    save_to_sqlite(db_name, table_name, filtered_data)

# 主程式
def main():
    url = 'https://mopsov.twse.com.tw/server-java/FileDownLoad'
    db_name = 'stock_data.db'
    table_name = 'monthly_revenue'

    # 初始化資料表
    init_db(db_name, table_name)

    # 設定起始年月為當前月份的前一個月
    current_date = datetime.now().replace(day=1) - timedelta(days=1)

    # 處理上市資料
    twse_config = 'twse.cfg'
    twse_filepath = '/t21/sii/'
    twse_stock_codes = read_stock_codes(twse_config)
    if twse_stock_codes:
        handle_market_data(url, twse_filepath, twse_stock_codes, db_name, table_name, current_date)

    # 處理上櫃資料
    otc_config = 'otc.cfg'
    otc_filepath = '/t21/otc/'
    otc_stock_codes = read_stock_codes(otc_config)
    if otc_stock_codes:
        handle_market_data(url, otc_filepath, otc_stock_codes, db_name, table_name, current_date)

if __name__ == '__main__':
    main()

