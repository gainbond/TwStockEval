#!/usr/bin/env python3
"""
功能：從 Yahoo 奇摩股市台股法說會行事曆中抓取今日活動，
      並比對與使用者 portfolio (股票代號) 是否有匹配，
      若有則發送 Telegram 訊息通知「今天有開法說會」
      
使用方法：
    python script.py portfolio.cfg

portfolio.cfg 範例如下：
    5871 中租-KY
    3152 璟德
    6177 達麗
"""

import os
import sys
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

# 讀取環境變數（優先 .env，找不到才用 export）
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        r = requests.post(url, data=payload)
        r.raise_for_status()
        print("Telegram 訊息發送成功")
    except Exception as e:
        print(f"Telegram 訊息發送失敗: {e}")

def load_portfolio(file_path):
    portfolio_codes = set()
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if parts:
                    portfolio_codes.add(parts[0])
    except Exception as e:
        print(f"讀取 portfolio 檔案錯誤: {e}")
    return portfolio_codes

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py portfolio.cfg")
        sys.exit(1)

    portfolio_file = sys.argv[1]
    portfolio_codes = load_portfolio(portfolio_file)
    print("Portfolio 股票代號：", portfolio_codes)

    url = "https://tw.stock.yahoo.com/calendar/earnings-call"

    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"無法取得網頁資料: {e}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    # 取得第一個 class 包含 "calendarDetail" 的 <section>
    section = soup.find("section", class_=lambda x: x and "calendarDetail" in x)
    if not section:
        print("找不到指定的 section (class 包含 calendarDetail)")
        return

    # 取得今日日期，格式需與網頁日期格式一致
    today_str = datetime.now().strftime("%Y/%m/%d")
    print(f"今日日期：{today_str}\n")

    ul = section.find("ul")
    if not ul:
        print("找不到活動清單 (<ul> 元素)")
        return

    events = ul.find_all("li")
    if not events:
        print("活動清單中未發現任何資料")
        return

    matching_codes = []
    for li in events:
        container = li.find("div")
        if not container:
            continue
        cols = container.find_all("div", recursive=False)
        if len(cols) < 2:
            continue

        # 取出第二欄（index 1）的日期資訊，格式如 "2025/03/18 14:00"
        date_text = cols[1].get_text(strip=True)
        event_date = date_text.split()[0]
        if event_date != today_str:
            continue

        company_col = cols[0]
        # 股票代號通常在內部含 "Fz(14px)" 的 <span> 中
        code_span = company_col.find("span", class_=lambda x: x and "Fz(14px)" in x)
        if not code_span:
            continue

        full_stock_code = code_span.get_text(strip=True)
        stock_code = full_stock_code.split('.')[0]

        if stock_code in portfolio_codes:
            company_div = company_col.find("div", class_=lambda x: x and "Fw(600)" in x)
            code_span = company_col.find("span", class_=lambda x: x and "Fz(14px)" in x)
            if company_div and code_span:
                company_name = company_div.get_text(strip=True)
                matching_codes.append(stock_code + " " +  company_name)
            else:
                matching_codes.append(stock_code)

    if matching_codes:
        matching_codes = sorted(set(matching_codes))
        message = "今天有開法說會的股票：\n" + "\n".join(matching_codes)
        print("Telegram 訊息內容：\n", message)
        send_telegram_message(message)
    else:
        print("今日無您 portfolio 中的法說會活動。")

if __name__ == '__main__':
    main()

