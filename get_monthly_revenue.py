# -*- coding: utf-8 -*-
import pandas as pd
import requests
from io import StringIO
import sqlite3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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

def process_data(df, stock_codes, report_month):
    try:
        required_columns = ['公司代號', '營業收入-當月營收', '營業收入-去年同月增減(%)']
        column_mapping = {
            '公司代號': 'stock_no',
            '營業收入-當月營收': 'monthly_revenue',
            '營業收入-去年同月增減(%)': 'yoy_growth'
        }

        if not all(col in df.columns for col in required_columns):
            missing_columns = [col for col in required_columns if col not in df.columns]
            print(f"缺少以下必要欄位: {missing_columns}")
            return None

        df = df.rename(columns=column_mapping)
        stock_codes = [code.strip() for code in stock_codes]
        df['stock_no'] = df['stock_no'].astype(str).str.strip()
        filtered_df = df[df['stock_no'].isin(stock_codes)].copy()
        filtered_df['revenue_month'] = report_month

        return filtered_df
    except Exception as e:
        print(f"處理過程中發生錯誤: {e}")
        return None

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

