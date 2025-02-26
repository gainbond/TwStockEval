import os
import requests
import sqlite3
import pandas as pd
import argparse
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

LAST_COLOR_JSON = "last_color.json"

# 讀取環境變數（優先 .env，找不到才用 export）
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def load_stock_codes_and_names(cfg_path):
    result = {}
    if not os.path.exists(cfg_path):
        return result
    with open(cfg_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                stock_no = parts[0]
                stock_name = "".join(parts[1:])
                result[stock_no] = stock_name
    return result

def fetch_twse_latest_price():
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
    result = {}
    try:
        resp = requests.get(url, headers={
            "If-Modified-Since": "Mon, 26 Jul 1997 05:00:00 GMT",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        })
        resp.raise_for_status()
        data = resp.json()
    except:
        print(f"抓取 TWSE 最新股價時發生錯誤: {e}")
        return result
    for item in data:
        code = item.get("Code")
        cp_str = item.get("ClosingPrice")
        if code and cp_str:
            try:
                cp = float(cp_str)
                result[code] = cp
            except:
                pass
    return result

def fetch_otc_latest_price():
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    result = {}
    try:
        resp = requests.get(url, headers={
            "If-Modified-Since": "Mon, 26 Jul 1997 05:00:00 GMT",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        })
        resp.raise_for_status()
        data = resp.json()
    except:
        print(f"抓取 OTC 最新股價時發生錯誤: {e}")
        return result
    for item in data:
        code = item.get("SecuritiesCompanyCode")
        cp_str = item.get("Close")
        if code and cp_str:
            try:
                cp = float(cp_str)
                result[code] = cp
            except:
                pass
    return result

def has_4_years_data(conn, stock_no):
    query = """
    SELECT COUNT(DISTINCT SUBSTR(quarter, 1, 4)) AS year_count
    FROM stock_quarterly
    WHERE stock_no = ?
    """
    df = pd.read_sql(query, conn, params=(stock_no,))
    return (df["year_count"].iloc[0] or 0) >= 4

def is_profitable_in_5_years(conn, stock_no, report_year):
    start_year = report_year - 4
    q = f"""
    SELECT SUBSTR(quarter,1,4) AS y,
           SUM(eps) AS yearly_eps
    FROM stock_quarterly
    WHERE stock_no = '{stock_no}'
      AND CAST(SUBSTR(quarter,1,4) AS INT) BETWEEN {start_year} AND {report_year}
    GROUP BY y
    """
    df = pd.read_sql(q, conn)
    if df.empty:
        return False
    for row in df.itertuples():
        if (row.yearly_eps is None) or (row.yearly_eps <= 0):
            return False
    return True

def calculate_estimated_eps(conn, stock_no, report_year):
    pm_q = f"""
    SELECT SUM(net_income_after_tax) AS total_net_income,
           SUM(quarter_revenue) AS total_revenue
    FROM (
        SELECT net_income_after_tax, quarter_revenue
        FROM stock_quarterly
        WHERE stock_no = '{stock_no}'
        ORDER BY quarter DESC
        LIMIT 4
    ) AS limited_data;
    """
    df_pm = pd.read_sql(pm_q, conn)
    total_net_income = df_pm["total_net_income"].iloc[0] or 0
    total_revenue = df_pm["total_revenue"].iloc[0] or 0
    profit_margin = (total_net_income / total_revenue) if total_revenue else 0

    growth_q = f"""
    WITH cte AS (
        SELECT stock_no, yoy_growth,
               ROW_NUMBER() OVER (PARTITION BY stock_no ORDER BY revenue_month DESC) AS rn
        FROM monthly_revenue
        WHERE stock_no = '{stock_no}'
    )
    SELECT
       AVG(CASE WHEN rn <= 6 THEN yoy_growth END) AS avg_growth_6_months,
       MAX(CASE WHEN rn = 1 THEN yoy_growth END) AS last_month_growth
    FROM cte;
    """


    df_g = pd.read_sql(growth_q, conn)
    avg_growth = df_g["avg_growth_6_months"].iloc[0] or 0
    last_month_growth = df_g["last_month_growth"].iloc[0] or 0
    revenue_growth_rate = min(avg_growth, last_month_growth)

    ly_q = f"""
    SELECT SUM(quarter_revenue) AS last_year_revenue
    FROM stock_quarterly
    WHERE stock_no = '{stock_no}'
      AND quarter LIKE '{report_year - 1}%';
    """
    df_ly = pd.read_sql(ly_q, conn)
    last_year_revenue = df_ly["last_year_revenue"].iloc[0] or 0

    cap_q = f"""
    SELECT capital
    FROM stock_quarterly
    WHERE stock_no = '{stock_no}'
    ORDER BY quarter DESC
    LIMIT 1;
    """
    df_cap = pd.read_sql(cap_q, conn)
    latest_equity = df_cap["capital"].iloc[0] if not df_cap.empty else 0

    if latest_equity > 0:
        estimated_eps = last_year_revenue * (1 + revenue_growth_rate / 100) * profit_margin / (latest_equity / 10)
    else:
        estimated_eps = 0
    return estimated_eps

def remove_iqr_outliers(df, col_list):
    import pandas as pd
    outlier_condition = pd.Series([False]*len(df), index=df.index)
    for col in col_list:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5*iqr
        upper_bound = q3 + 1.5*iqr
        is_outlier = (df[col]<lower_bound)|(df[col]>upper_bound)
        outlier_condition = outlier_condition|is_outlier
    return df[~outlier_condition].copy()

def calculate_price_ranges(conn, stock_no, estimated_eps, report_year, lookback_years=5):
    import pandas as pd
    start_year = report_year - lookback_years + 1
    q = f"""
        SELECT year, highest_per, average_per, lowest_per
        FROM YearlyPER
        WHERE stock_no = '{stock_no}'
          AND year BETWEEN {start_year} AND {report_year}
        ORDER BY year
    """
    df = pd.read_sql(q, conn)
    if df.empty:
        return (None, None, None)

    df.dropna(subset=["highest_per","average_per","lowest_per"], inplace=True)
    if df.empty:
        return (None, None, None)

    df_clean = remove_iqr_outliers(df, ["highest_per","average_per","lowest_per"])
    if df_clean.empty:
        return (None, None, None)

    avg_high = df_clean["highest_per"].mean()
    avg_avg = df_clean["average_per"].mean()
    avg_low = df_clean["lowest_per"].mean()

    cheap = estimated_eps * avg_low
    fair  = estimated_eps * avg_avg
    exp   = estimated_eps * avg_high
    return (cheap, fair, exp)

def get_two_months_growths(conn, stock_no):
    q = f"""
    SELECT yoy_growth
    FROM monthly_revenue
    WHERE stock_no = '{stock_no}'
    ORDER BY revenue_month DESC
    LIMIT 2
    """
    df = pd.read_sql(q, conn)
    yoy = [0,0]
    for i in range(len(df)):
        yoy[i] = df["yoy_growth"].iloc[i] or 0
    return yoy

def get_last_month_growth(conn, stock_no):
    q = f"""
    SELECT yoy_growth
    FROM monthly_revenue
    WHERE stock_no = '{stock_no}'
    ORDER BY revenue_month DESC
    LIMIT 1
    """
    df = pd.read_sql(q, conn)
    if df.empty:
        return 0
    return df["yoy_growth"].iloc[0] or 0

def generate_pdf_report(df_result, pdf_filename="eps_report.pdf",
                        font_name="NotoSansTC", font_path="NotoSansTC-Regular.otf"):
    if os.path.exists(font_path):
        pdfmetrics.registerFont(TTFont(font_name, font_path))
    doc = SimpleDocTemplate(pdf_filename, pagesize=A4)
    from reportlab.platypus import Table, TableStyle
    table_data = [
        ["股票代號", "名稱", "最新收盤價", "預估EPS", "近月營收年增率", "便宜價", "合理價", "昂貴價"]
    ]
    for _, row in df_result.iterrows():
        table_data.append([
            row["股票代號"],
            row["名稱"],
            f"{row['最新收盤價']:.2f}",
            f"{row['估測EPS']:.2f}",
            f"{row['近月營收年增率']:.2f}%",
            f"{row['便宜價']:.2f}",
            f"{row['合理價']:.2f}",
            f"{row['昂貴價']:.2f}"
        ])
    table = Table(table_data, colWidths=[70, 70, 50, 50, 70, 50, 50, 50])
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (2, 1), (-1, -1), 'CENTER'),
    ])
    table.setStyle(style)
    from reportlab.platypus import Spacer
    elements = [table]
    doc.build(elements)

def send_telegram_text(bot_token, chat_id, text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    r = requests.post(url, data=data)
    if r.status_code == 200:
        print("已透過 Telegram 傳送文字訊息")
    else:
        print(f"Telegram 傳送失敗 code={r.status_code}, resp={r.text}")

def send_telegram_document(bot_token, chat_id, file_path, caption="EPS 報表檔案"):
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(file_path, 'rb') as f:
        files = {'document': f}
        data = {"chat_id": chat_id, "caption": caption}
        resp = requests.post(url, data=data, files=files)
    if resp.status_code == 200:
        print("已透過 Telegram 傳送檔案")
    else:
        print(f"寄送檔案失敗 code={resp.status_code}, resp={resp.text}")

def load_last_colors(json_path):
    if not os.path.exists(json_path):
        return {}
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {}
    except:
        return {}

def save_new_colors(json_path, color_dict):
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(color_dict, f, ensure_ascii=False, indent=2)
    except:
        pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-year", type=int)
    parser.add_argument("--portfolio-cfg", type=str)
    args = parser.parse_args()

    report_year = args.report_year if args.report_year else datetime.now().year

    db_name = "stock_data.db"
    if not os.path.exists(db_name):
        return

    conn = sqlite3.connect(db_name)
    if args.portfolio_cfg and os.path.exists(args.portfolio_cfg):
        portfolio_dict = load_stock_codes_and_names(args.portfolio_cfg)
        all_stocks = portfolio_dict
    else:
        twse_dict = load_stock_codes_and_names("twse.cfg")
        otc_dict  = load_stock_codes_and_names("otc.cfg")
        all_stocks = {**twse_dict, **otc_dict}

    twse_prices = fetch_twse_latest_price()
    otc_prices  = fetch_otc_latest_price()

    rows = []
    for stock_no, stock_name in all_stocks.items():
        if not has_4_years_data(conn, stock_no):
            continue
        if not is_profitable_in_5_years(conn, stock_no, report_year):
            continue
        est_eps = calculate_estimated_eps(conn, stock_no, report_year)
        if est_eps <= 0:
            continue
        cheap, fair, expensive = calculate_price_ranges(conn, stock_no, est_eps, report_year, 5)
        if cheap is None:
            continue

        if stock_no in twse_prices:
            latest_close = twse_prices[stock_no]
        elif stock_no in otc_prices:
            latest_close = otc_prices[stock_no]
        else:
            continue

        last_month_yoy = get_last_month_growth(conn, stock_no)
        rows.append({
            "股票代號": stock_no,
            "名稱": stock_name,
            "最新收盤價": round(latest_close, 2),
            "估測EPS": round(est_eps, 2),
            "近月營收年增率": round(last_month_yoy, 2),
            "便宜價": round(cheap, 2),
            "合理價": round(fair, 2),
            "昂貴價": round(expensive, 2),
        })

    if not rows:
        conn.close()
        return

    df_result = pd.DataFrame(rows)
    yoy_2m_list = []
    for idx, row in df_result.iterrows():
        yoy2 = get_two_months_growths(conn, row["股票代號"])
        yoy_2m_list.append(yoy2)
    df_result["last_2m_list"] = yoy_2m_list
    conn.close()

    def classify_color(r):
        yoy2 = r["last_2m_list"]
        if r["最新收盤價"] < r["便宜價"]:
            if yoy2[0]>5 and yoy2[1]>5:
                return "red"
            else:
                return "orange"
        if r["最新收盤價"] > r["昂貴價"]:
            return "green"
        return "none"

    df_result["color_class"] = df_result.apply(classify_color, axis=1)
    color_priority = {"red":0, "orange":1, "green":2, "none":3}
    df_result["sort_key"] = df_result["color_class"].map(color_priority)
    df_result.sort_values("sort_key", inplace=True, ignore_index=True)
    df_result.drop(columns=["sort_key","last_2m_list"], inplace=True)

    old_colors = load_last_colors(LAST_COLOR_JSON)

    today_str = datetime.now().strftime("%Y%m%d")
    if args.portfolio_cfg:
        base_filename = os.path.basename(args.portfolio_cfg)
        pf_name, _ = os.path.splitext(base_filename)
        pdf_filename = f"eps_report_{today_str}_{pf_name}.pdf"
    else:
        pdf_filename = f"eps_report_{today_str}.pdf"

    generate_pdf_report(df_result, pdf_filename)

    new_colors = {}
    color_emoji_map = {
        "red": "🔴",
        "orange": "🟠",
        "green": "🟢",
        "none": "⚪"
    }

    summary_lines = []
    for _, row in df_result.iterrows():
        s_no   = row["股票代號"]
        s_name = row["名稱"]
        cclass = row["color_class"]
        old_c  = old_colors.get(s_no, None)
        changed_flag = ""
        if old_c and old_c != cclass:
            changed_flag = "🔺"
        color_emoji = color_emoji_map[cclass]
        summary_lines.append(f"{color_emoji}{changed_flag} `{s_no}` {s_name}")
        new_colors[s_no] = cclass

    save_new_colors(LAST_COLOR_JSON, new_colors)

    if BOT_TOKEN and CHAT_ID:
        if summary_lines:
            text_msg = "*EPS 報表摘要*\n\n" + "\n".join(summary_lines)
            send_telegram_text(BOT_TOKEN, CHAT_ID, text_msg)
            time.sleep(1)
        send_telegram_document(BOT_TOKEN, CHAT_ID, pdf_filename, caption="EPS 報表檔案")

if __name__ == "__main__":
    main()

