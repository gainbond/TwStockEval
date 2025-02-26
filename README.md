# TwStockEval
Stock Value Evaluator in Taiwan(TWSE/TPEX) market with telegram notification
## 指令範例：
```sh
/usr/bin/python3.6 eps_report.py --report-year 2024 --portfolio-cfg portfolio.cfg
```

## 參數說明：
### `--portfolio-cfg`
- 若有指定，例如：
  ```sh
  python eps_report.py --portfolio-cfg=my_portfolio.cfg
  ```
  則程式**只**會讀取 `my_portfolio.cfg`。
- 若沒指定，則程式預設會合併 `twse.cfg + otc.cfg` 作為股票清單。

### `--report-year`
- 可指定財報基準年，例如：
  ```sh
  python eps_report.py --report-year 2024
  ```
- 若未指定，則預設使用 `datetime.now().year`。

---

## **篩選邏輯：**
1. **近 5 年無 EPS ≤ 0** → 保留
2. **IQR 去除離群值後** 計算 **便宜/合理/昂貴價**
3. **紅色**：`收盤價 < 便宜價` 且 **最近 2 個月年增率均 > 5%**
4. **橘色**：`收盤價 < 便宜價` 但 **不符合紅色條件**
5. **綠色**：`收盤價 > 昂貴價`
6. 其餘 → **無標記**

---

## **如何使用？**
1. **確保已安裝 Python 3.6 以上版本**
2. **取得至少最近五年證交所、櫃買年成交資料**(`getTWSE.py`, `getOTC.py`)
3. **取得月營收資料**(`get_monthly_revenue.py`)
4. **執行指令**
   ```sh
   python3 eps_report.py --report-year 2024 --portfolio-cfg portfolio.cfg
   ```
5. **查看輸出結果**
   - 根據上述篩選邏輯，計算不同股票的所屬級距。

---
