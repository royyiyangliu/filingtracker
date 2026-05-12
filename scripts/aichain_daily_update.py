"""
daily_update.py
每日定时脚本（由 GitHub Actions 调用）：
  1. 为所有股票全量重新拉取5年前复权历史收盘价，覆盖写入 data/history/<TICKER>.json
     （全量重下载可确保前复权历史数据在除权除息后始终准确）
  2. 从 history 文件计算各阶段涨幅（1d/1w/1m/6m/1y/3y）
  3. 通过 Yahoo Finance 逐只获取实时市值、PE/PS/PB
  4. 生成 data/summary.json（供前端直接使用）
  5. 更新 data/last_updated.json
"""

import json
import os
import time
import sys
from pathlib import Path
from datetime import datetime, date, timedelta

import yfinance as yf
import pandas as pd
import numpy as np
import requests

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data" / "aichain"
HISTORY_DIR = DATA_DIR / "history"
STOCKS_FILE = DATA_DIR / "stocks.json"

# 各阶段涨幅对应的日历天数（与前端图表保持一致）
PERIOD_DAYS = {
    "1d":  1,
    "1w":  7,
    "1m":  31,
    "6m":  183,
    "1y":  365,
    "3y":  1095,
}

# 需要转换为USD的货币列表（yfinance汇率ticker格式：XXXUSD=X）
FX_TICKERS = {
    "AUD": "AUDUSD=X",
    "CAD": "CADUSD=X",
    "CHF": "CHFUSD=X",    # 瑞士法郎（ABB等）
    "CNY": "CNYUSD=X",
    "DKK": "DKKUSD=X",    # 丹麦克朗（Asetek等）
    "EUR": "EURUSD=X",
    "GBP": "GBPUSD=X",
    "GBX": None,          # 英国便士，1 GBX = 0.01 GBP，特殊处理
    "HKD": "HKDUSD=X",
    "INR": "INRUSD=X",
    "JPY": "JPYUSD=X",
    "KRW": "KRWUSD=X",
    "NOK": "NOKUSD=X",    # 挪威克朗
    "SEK": "SEKUSD=X",    # 瑞典克朗（Alfa Laval、Munters等）
    "SGD": "SGDUSD=X",
    "TWD": "TWDUSD=X",
    "USD": None,          # 基准货币
}


def fetch_fx_rates() -> dict:
    """
    从 Yahoo Finance 获取最新汇率，返回 {货币: 对USD汇率}
    例如 HKD=0.1282 表示 1 HKD = 0.1282 USD
    """
    rates = {"USD": 1.0}
    yf_tickers = [v for v in FX_TICKERS.values() if v is not None]
    try:
        data = yf.download(
            yf_tickers,
            period="2d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            timeout=20,
        )
        for currency, yf_tk in FX_TICKERS.items():
            if yf_tk is None:
                continue
            try:
                col = data["Close"]
                if isinstance(col, pd.DataFrame):
                    closes = col[yf_tk].dropna() if yf_tk in col.columns else col.iloc[:, 0].dropna()
                else:
                    closes = col.dropna()
                if not closes.empty:
                    rates[currency] = float(closes.iloc[-1])
            except Exception:
                pass
    except Exception as e:
        print(f"  汇率获取失败: {e}，使用默认值1.0")

    # GBX（英国便士）= GBP / 100
    if "GBP" in rates:
        rates["GBX"] = rates["GBP"] / 100

    print(f"  汇率获取完成：{len(rates)} 种货币")
    for cur, rate in sorted(rates.items()):
        print(f"    1 {cur} = {rate:.6f} USD")
    return rates


def to_yf_ticker(ticker: str) -> str:
    t = ticker.strip()
    if t.endswith(".SH"):
        return t[:-3] + ".SS"
    return t


def load_history(ticker: str) -> tuple[list, list]:
    """加载历史数据，返回 (dates, prices)"""
    path = HISTORY_DIR / f"{ticker}.json"
    if not path.exists():
        return [], []
    with open(path) as f:
        d = json.load(f)
    return d.get("dates", []), d.get("prices", [])


def save_history(ticker: str, dates: list, prices: list):
    path = HISTORY_DIR / f"{ticker}.json"
    with open(path, "w") as f:
        json.dump({"ticker": ticker, "dates": dates, "prices": prices}, f)


def calc_returns(dates: list, prices: list) -> dict:
    """
    根据历史价格计算各阶段涨幅（百分比，保留2位小数）。
    1d：直接用倒数第二个交易日收盘价（避免周末/节假日导致基准落在非交易日）。
    其余周期：从最后交易日往前推日历天数，找到 >= cutoff 的第一条价格。
    """
    n = len(dates)
    if n == 0:
        return {p: None for p in PERIOD_DAYS}
    current = prices[-1]
    latest_date = date.fromisoformat(dates[-1])
    result = {}
    for period, cal_days in PERIOD_DAYS.items():
        if period == "1d":
            # 直接取前一个交易日，不依赖日历天数
            base = prices[-2] if n >= 2 else None
        else:
            cutoff = (latest_date - timedelta(days=cal_days)).isoformat()
            base = None
            for i, d in enumerate(dates):
                if d >= cutoff:
                    base = prices[i]
                    break
        if base and base != 0:
            result[period] = round((current / base - 1) * 100, 2)
        else:
            result[period] = None
    return result


def fetch_all_history(stocks: list) -> tuple[int, int]:
    """
    全量下载所有股票5年前复权历史收盘价，覆盖写入 data/history/<TICKER>.json
    每次全量重下载确保前复权数据在除权除息后始终准确
    返回 (成功数, 失败数)
    """
    ok, fail = 0, 0
    total = len(stocks)
    for i, s in enumerate(stocks):
        ticker = s["ticker"]
        yf_tk  = to_yf_ticker(ticker)
        try:
            hist = yf.download(
                yf_tk,
                period="5y",
                interval="1d",
                auto_adjust=True,   # 前复权（当前价=实际市价，历史价格向下调整）
                progress=False,
                timeout=30,
            )
            if hist.empty:
                print(f"  [{i+1}/{total}] {ticker} — 无数据")
                fail += 1
                time.sleep(0.3)
                continue

            closes = hist["Close"].dropna()
            # 新版 yfinance 单只下载可能返回 DataFrame（MultiIndex列），需压缩为 Series
            if isinstance(closes, pd.DataFrame):
                closes = closes.iloc[:, 0].dropna()

            if closes.empty:
                fail += 1
                time.sleep(0.3)
                continue

            dates  = [d.strftime("%Y-%m-%d") for d in closes.index]
            prices = [round(float(p), 4) for p in closes.to_numpy().ravel()]

            save_history(ticker, dates, prices)
            ok += 1
            if (i + 1) % 30 == 0 or (i + 1) == total:
                print(f"  进度：{i+1}/{total}，已成功 {ok} 只")
        except Exception as e:
            print(f"  [{i+1}/{total}] {ticker} 失败: {e}")
            fail += 1
        time.sleep(0.3)

    return ok, fail


def fetch_quote_data(stocks: list, fx_rates: dict) -> dict:
    """
    通过 Yahoo Finance 批量获取市值、PE/PS/PB，并将市值换算为亿美元
    返回 {ticker: {market_cap_usd, currency, pe, ps, pb}}
    """
    result = {}
    for s in stocks:
        ticker = s["ticker"]
        yf_tk = to_yf_ticker(ticker)
        try:
            tk = yf.Ticker(yf_tk)
            info = tk.fast_info
            mc = getattr(info, "market_cap", None)
            currency = getattr(info, "currency", "USD") or "USD"

            # 汇率换算：本地货币市值 → USD市值 → 亿美元
            rate = fx_rates.get(currency, None)
            if mc and rate is not None:
                mc_usd = round(mc * rate / 1e8, 2)
            elif mc and currency == "USD":
                mc_usd = round(mc / 1e8, 2)
            else:
                mc_usd = None  # 未知货币，不瞎猜

            # 需要完整info才能拿到PS/PB
            full = tk.info
            pe = full.get("trailingPE") or full.get("forwardPE")
            ps = full.get("priceToSalesTrailing12Months")
            pb = full.get("priceToBook")

            result[ticker] = {
                "market_cap_usd": mc_usd,
                "currency": currency,
                "pe":  round(pe, 2) if pe else None,
                "ps":  round(ps, 2) if ps else None,
                "pb":  round(pb, 2) if pb else None,
            }
        except Exception:
            result[ticker] = {
                "market_cap_usd": None, "currency": "USD",
                "pe": None, "ps": None, "pb": None,
            }
        time.sleep(0.2)

    return result


def build_summary(stocks: list, quote_data: dict) -> dict:
    """
    整合历史涨幅 + 实时报价，生成 summary.json 的 stocks 部分
    """
    summary_stocks = []
    for s in stocks:
        ticker = s["ticker"]
        dates, prices = load_history(ticker)
        returns = calc_returns(dates, prices)
        q = quote_data.get(ticker, {})

        last_price = prices[-1] if prices else None
        last_date  = dates[-1]  if dates  else None

        summary_stocks.append({
            "ticker":          ticker,
            "name_cn":         s["name_cn"],
            "name_en":         s["name_en"],
            "market":          s["market"],
            "l1":              s["l1"],
            "l2":              s["l2"],
            "l1_name":         s["l1_name"],
            "l2_name":         s["l2_name"],
            "last_price":      last_price,
            "last_date":       last_date,
            "market_cap_usd":  q.get("market_cap_usd"),   # 亿美元
            "currency":        q.get("currency", "USD"),
            "pe":              q.get("pe"),
            "ps":              q.get("ps"),
            "pb":              q.get("pb"),
            "returns":         returns,   # {1d, 1w, 1m, 6m, 1y, 3y}
        })
    return summary_stocks


def build_layer_aggregates(summary_stocks: list, layers: dict) -> dict:
    """
    按 L1/L2 汇总：总市值、市值加权平均各阶段涨幅、股票数
    """
    from collections import defaultdict
    l1_groups = defaultdict(list)
    l2_groups = defaultdict(list)

    for s in summary_stocks:
        l1_groups[s["l1"]].append(s)
        l2_groups[(s["l1"], s["l2"])].append(s)

    def weighted_avg_return(stocks_list, period):
        """
        区间起点市值加权涨幅：
        起点市值 = 当前市值 / (1 + 涨幅)
        组合涨幅 = Σ当前市值 / Σ起点市值 - 1
        """
        total_current = 0.0
        total_begin   = 0.0
        for s in stocks_list:
            cap = s.get("market_cap_usd") or 0
            ret = (s.get("returns") or {}).get(period)
            if ret is None or cap <= 0:
                continue
            total_current += cap
            total_begin   += cap / (1 + ret / 100)
        if total_begin == 0:
            return None
        return round((total_current / total_begin - 1) * 100, 2)

    def aggregate(stocks_list):
        total_cap = sum(s["market_cap_usd"] or 0 for s in stocks_list)
        return {
            "count": len(stocks_list),
            "total_cap_usd": round(total_cap, 2),
            "returns": {
                p: weighted_avg_return(stocks_list, p)
                for p in PERIOD_DAYS
            }
        }

    l1_agg = {}
    for l1, slist in l1_groups.items():
        l1_agg[l1] = aggregate(slist)
        l1_agg[l1]["name"] = layers.get(l1, {}).get("name", l1)

    l2_agg = {}
    for (l1, l2), slist in l2_groups.items():
        l2_agg[l2] = aggregate(slist)
        l2_agg[l2]["l1"] = l1
        l2_agg[l2]["name"] = slist[0]["l2_name"] if slist else l2

    return {"l1": l1_agg, "l2": l2_agg}


def build_chart_data(stocks: list, summary_stocks: list) -> None:
    """
    构建各 L1/L2 分类的历史累计回报指数，用于前端对比图表。
    方法：一致篮子法（consistent basket）
      - 每日涨幅仅由当天与前一天均有报价的成分股计算
      - 避免新上市/退市个股导致的指数突变
    输出：data/chart_categories.json
    """
    from collections import defaultdict

    print(f"\n[Step 6] 构建分类历史回报指数（一致篮子法）...")

    # ── 当前市值映射（亿美元）──────────────────────────────
    mc_map = {s["ticker"]: (s.get("market_cap_usd") or 0) for s in summary_stocks}

    # ── 加载所有历史文件到内存 ────────────────────────────
    hist_map: dict[str, dict] = {}
    for s in stocks:
        ticker = s["ticker"]
        path = HISTORY_DIR / f"{ticker}.json"
        if path.exists():
            with open(path) as f:
                hist_map[ticker] = json.load(f)

    print(f"  加载历史数据：{len(hist_map)}/{len(stocks)} 只")

    # ── 每只股票：日期→价格 映射 + 最新价格 ──────────────
    price_map:  dict[str, dict[str, float]] = {}   # ticker -> {date: price}
    last_price: dict[str, float] = {}              # ticker -> latest price

    for ticker, h in hist_map.items():
        if h.get("dates") and h.get("prices"):
            price_map[ticker]  = dict(zip(h["dates"], h["prices"]))
            last_price[ticker] = h["prices"][-1]

    # ── 按 L1/L2 分组（用 set 去重，同一只股票可能横跨两个 L2）──
    l1_groups: dict[str, set] = defaultdict(set)
    l2_groups: dict[str, set] = defaultdict(set)
    l2_meta:   dict[str, dict] = {}

    for s in stocks:
        t = s["ticker"]
        if t not in price_map:
            continue
        l1_groups[s["l1"]].add(t)
        l2_groups[s["l2"]].add(t)
        l2_meta[s["l2"]] = {"name": s["l2_name"], "l1": s["l1"]}

    # ── 核心计算函数：一致篮子指数 ────────────────────────
    def compute_index(tickers: list) -> tuple[list[str], list[float]]:
        """
        输入成分股列表，返回 (dates, values)
        values[0] = 1.0（从第一个有效交易日开始）
        """
        if not tickers:
            return [], []

        def hist_mc(ticker: str, date: str):
            """推算指定日期的历史市值（亿美元）"""
            p = price_map.get(ticker, {}).get(date)
            p_curr = last_price.get(ticker)
            mc_curr = mc_map.get(ticker, 0)
            if not p or not p_curr or not mc_curr or p_curr == 0:
                return None
            return mc_curr * p / p_curr

        # 所有成分股报价日期的并集，排序
        all_dates = sorted(
            set(d for t in tickers for d in price_map.get(t, {}).keys())
        )
        if not all_dates:
            return [], []

        idx_dates:  list[str]   = [all_dates[0]]
        idx_values: list[float] = [1.0]
        current = 1.0

        for i in range(1, len(all_dates)):
            d_prev, d_curr = all_dates[i - 1], all_dates[i]
            num = den = 0.0
            for t in tickers:
                mc_p = hist_mc(t, d_prev)
                mc_c = hist_mc(t, d_curr)
                if mc_p is not None and mc_c is not None:
                    num += mc_c
                    den += mc_p
            if den > 0:
                current *= num / den
            idx_dates.append(d_curr)
            idx_values.append(round(current, 8))

        return idx_dates, idx_values

    # ── L1 名称映射 ───────────────────────────────────────
    l1_names = {
        "L1": "原材料与半导体设备", "L2": "芯片制造",
        "L3": "核心芯片设计",       "L4": "系统集成",
        "L5": "数据中心基础设施",   "L6": "电力与能源供给",
        "L7": "算力平台",           "L8": "AI应用层",
    }

    categories: dict[str, dict] = {}

    # L1 分类
    for l1, tset in l1_groups.items():
        dates, values = compute_index(list(tset))
        categories[l1] = {
            "name": l1_names.get(l1, l1),
            "l1": l1,
            "dates": dates,
            "values": values,
        }
        print(f"  {l1}: {len(tset)}只 → {len(dates)}个交易日")

    # L2 分类
    for l2, tset in l2_groups.items():
        dates, values = compute_index(list(tset))
        meta = l2_meta.get(l2, {})
        categories[l2] = {
            "name": meta.get("name", l2),
            "l1":   meta.get("l1", ""),
            "dates":  dates,
            "values": values,
        }

    # 全产业链合计（跨 L1 去重）
    all_tickers = list({s["ticker"] for s in stocks if s["ticker"] in price_map})
    dates_all, vals_all = compute_index(all_tickers)
    categories["ALL"] = {
        "name": "全产业链合计",
        "l1": "",
        "dates":  dates_all,
        "values": vals_all,
    }
    print(f"  ALL: {len(all_tickers)}只 → {len(dates_all)}个交易日")

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories": categories,
    }
    out_path = DATA_DIR / "chart_categories.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))
    sz = out_path.stat().st_size
    print(f"  chart_categories.json 写入完成（{sz // 1024} KB）")


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始每日更新")

    with open(STOCKS_FILE, encoding="utf-8") as f:
        meta = json.load(f)
    stocks = meta["stocks"]
    layers = meta["layers"]
    links  = meta["links"]

    # ── Step 1: 全量重新下载5年历史（覆盖写入） ──────────────
    print(f"\n[Step 1] 全量下载5年前复权历史数据（{len(stocks)} 只股票）...")
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    ok, fail = fetch_all_history(stocks)
    print(f"  完成：成功 {ok} 只，失败 {fail} 只")

    # ── Step 2: 获取汇率 ───────────────────────────────────
    print(f"\n[Step 2] 获取最新汇率...")
    fx_rates = fetch_fx_rates()

    # ── Step 3: 批量获取市值/估值 ──────────────────────────
    print(f"\n[Step 3] 获取市值和估值数据...")
    quote_data = fetch_quote_data(stocks, fx_rates)
    print(f"  获取完成，{sum(1 for v in quote_data.values() if v.get('market_cap_usd'))} 只有市值数据")

    # ── Step 4: 生成 summary.json ──────────────────────────
    print(f"\n[Step 4] 生成 summary.json...")
    summary_stocks = build_summary(stocks, quote_data)
    aggregates = build_layer_aggregates(summary_stocks, layers)

    summary = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_stocks": len(summary_stocks),
        "fx_rates": fx_rates,   # 汇率快照，供前端实时数据换算使用
        "layers": layers,
        "links":  links,
        "aggregates": aggregates,
        "stocks": summary_stocks,
    }

    summary_path = DATA_DIR / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  summary.json 写入完成（{summary_path.stat().st_size // 1024} KB）")

    # ── Step 5: 更新时间戳 ────────────────────────────────
    with open(DATA_DIR / "last_updated.json", "w") as f:
        json.dump({
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "stocks_ok":   ok,
            "stocks_fail": fail,
        }, f)

    # ── Step 6: 构建分类历史回报指数（用于前端对比图表）────
    build_chart_data(stocks, summary_stocks)

    print(f"\n[完成] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
