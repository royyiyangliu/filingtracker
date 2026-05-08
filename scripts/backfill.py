"""
历史数据回填脚本（一次性运行）
- 拉取沪港通/深港通南向资金流向全量历史
- 按月拉取个股南向持股数据，存为年度 CSV
"""
import sys
import time
import logging
from datetime import date, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

ROOT = Path(__file__).parent.parent
FLOW_DIR = ROOT / "data" / "hkdata" / "flow"
HOLDINGS_DIR = ROOT / "data" / "hkdata" / "holdings"

for d in [FLOW_DIR, HOLDINGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

FLOW_RENAME = {
    "日期": "date", "当日成交净买额": "net",
    "买入成交额": "buy", "卖出成交额": "sell"
}
HOLDINGS_RENAME = {
    "持股日期": "date", "股票代码": "code", "股票简称": "name",
    "当日收盘价": "close", "持股数量": "shares",
    "持股市值": "mktval", "持股数量占发行股百分比": "pct"
}


def retry(fn, n=3, wait=20):
    for i in range(n):
        try:
            return fn()
        except Exception as e:
            if i < n - 1:
                log.warning(f"第{i+1}次失败: {e}，{wait}秒后重试")
                time.sleep(wait)
            else:
                raise


# ── 资金流向 ──────────────────────────────────────────────────

def backfill_flow():
    log.info("=== 拉取南向资金流向 ===")
    for symbol, fname in [("港股通沪", "hu"), ("港股通深", "shen")]:
        df = retry(lambda s=symbol: ak.stock_hsgt_hist_em(symbol=s))
        df = df.rename(columns=FLOW_RENAME)[list(FLOW_RENAME.values())]
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("date").reset_index(drop=True)
        path = FLOW_DIR / f"{fname}.csv"
        df.to_csv(path, index=False)
        log.info(f"  {symbol}: {len(df)}行，{df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")


# ── 个股持股数据 ───────────────────────────────────────────────

def _month_iter(start_year=2016, start_month=12):
    """从深港通开通月起，逐月产生 (year, month)"""
    y, m = start_year, start_month
    today = date.today()
    while date(y, m, 1) <= today:
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1


def _month_end(y, m):
    if m == 12:
        return date(y + 1, 1, 1) - timedelta(days=1)
    return date(y, m + 1, 1) - timedelta(days=1)


def _fetch_holdings(start_str: str, end_str: str) -> pd.DataFrame:
    df = retry(
        lambda: ak.stock_hsgt_stock_statistics_em(
            symbol="南向持股", start_date=start_str, end_date=end_str
        ),
        n=3, wait=20
    )
    df = df.rename(columns=HOLDINGS_RENAME)[list(HOLDINGS_RENAME.values())]
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["code"] = df["code"].astype(str).str.zfill(5)
    for col in ["close", "shares", "mktval", "pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _save_holdings(df: pd.DataFrame):
    """按年份追加写入，自动去重"""
    df = df.copy()
    df["_year"] = pd.to_datetime(df["date"]).dt.year
    for year, grp in df.groupby("_year"):
        path = HOLDINGS_DIR / f"{year}.csv"
        g = grp.drop(columns="_year")
        if path.exists():
            existing = pd.read_csv(path, dtype={"code": str})
            combined = pd.concat([existing, g]).drop_duplicates(["date", "code"])
            combined.sort_values(["date", "code"]).to_csv(path, index=False)
        else:
            g.sort_values(["date", "code"]).to_csv(path, index=False)


def _already_fetched_months() -> set:
    """返回已存入的 YYYY-MM 集合（跳过已有月份）"""
    fetched = set()
    for csv in HOLDINGS_DIR.glob("*.csv"):
        try:
            df = pd.read_csv(csv, usecols=["date"])
            fetched.update(df["date"].str[:7].dropna().unique())
        except Exception:
            pass
    return fetched


def backfill_holdings():
    log.info("=== 拉取个股南向持股（按月回填）===")
    fetched = _already_fetched_months()
    total_rows = 0

    for y, m in _month_iter():
        month_str = f"{y}-{m:02d}"
        if month_str in fetched:
            log.info(f"  {month_str}: 已存在，跳过")
            continue

        start = date(y, m, 1).strftime("%Y%m%d")
        end = min(_month_end(y, m), date.today()).strftime("%Y%m%d")
        log.info(f"  {month_str}: 拉取 {start}~{end} ...")

        try:
            df = _fetch_holdings(start, end)
            if df.empty:
                log.info(f"  {month_str}: 无数据（假期？）")
            else:
                _save_holdings(df)
                days = df["date"].nunique()
                log.info(f"  {month_str}: {len(df)}行，{days}个交易日，{df['code'].nunique()}只股票")
                total_rows += len(df)
        except Exception as e:
            log.error(f"  {month_str}: 失败 - {e}")

        time.sleep(1)  # 礼貌间隔

    log.info(f"个股持股回填完成，共新增 {total_rows} 行")


# ── 入口 ──────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode in ("all", "flow"):
        backfill_flow()

    if mode in ("all", "holdings"):
        backfill_holdings()

    log.info("=== backfill 完成 ===")
