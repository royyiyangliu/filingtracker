"""
从存储的 CSV 构建网站 JSON 文件
输出到 data/hkdata/（由 GitHub Pages 从根目录托管）

产出：
  data/hkdata/flow.json        南向资金流向时间序列
  data/hkdata/ranking.json     各周期净买入/持股比例变化排行
  data/hkdata/stocks/{code}.json  个股历史持仓比例
"""
import json
import logging
import math
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import DateOffset

ROOT = Path(__file__).parent.parent
FLOW_DIR = ROOT / "data" / "hkdata" / "flow"
HOLDINGS_DIR = ROOT / "data" / "hkdata" / "holdings"
SITE_DIR = ROOT / "data" / "hkdata"
STOCKS_DIR = SITE_DIR / "stocks"

for d in [SITE_DIR, STOCKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# 各周期对应的自然日偏移（1d 特殊处理：上一交易日）
PERIOD_OFFSETS = {
    "1d": None,
    "1w": DateOffset(days=7),
    "1m": DateOffset(months=1),
    "3m": DateOffset(months=3),
    "6m": DateOffset(months=6),
    "1y": DateOffset(years=1),
}


def _find_ref_date(dates_sorted: list, cutoff_str: str) -> str | None:
    """找到 ≤ cutoff_str 的最近一个实际交易日。"""
    candidates = [d for d in dates_sorted if d <= cutoff_str]
    return candidates[-1] if candidates else None


def _safe(v, digits=None):
    """NaN/Inf → None；有效浮点数按 digits 四舍五入。"""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return round(f, digits) if digits is not None else f


def _dump(obj, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


# ── 加载原始数据 ─────────────────────────────────────────────

def load_industry() -> dict[str, dict]:
    """读取 data/hkdata/industry.csv，返回 {code: {gics_l1, gics_l2, gics_l3, gics_l4}}"""
    path = ROOT / "data" / "hkdata" / "industry.csv"
    if not path.exists():
        log.warning("industry.csv 不存在，行业字段将为空（请先运行 fetch_industry.py）")
        return {}
    df = pd.read_csv(path, dtype=str).fillna("")
    if "gics_l1" not in df.columns:
        log.warning("industry.csv 为旧格式（HKEX行业），行业字段将为空，请重新运行 fetch_industry.py")
        return {}
    for col in ["gics_l1", "gics_l2", "gics_l3", "gics_l4"]:
        if col not in df.columns:
            df[col] = ""
    mapping = {
        row["code"]: {
            "gics_l1": row["gics_l1"],
            "gics_l2": row["gics_l2"],
            "gics_l3": row["gics_l3"],
            "gics_l4": row["gics_l4"],
        }
        for _, row in df.iterrows()
    }
    filled = sum(1 for v in mapping.values() if v["gics_l1"])
    log.info(f"GICS行业映射：{len(mapping)} 条，有效 {filled} 条")
    return mapping


def load_flow() -> dict:
    result = {}
    for channel, fname in [("hu", "hu"), ("shen", "shen")]:
        path = FLOW_DIR / f"{fname}.csv"
        if not path.exists():
            result[channel] = []
            continue
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("date")
        result[channel] = [
            {
                "date": r["date"],
                "net":  _safe(r["net"],  4),
                "buy":  _safe(r["buy"],  4),
                "sell": _safe(r["sell"], 4),
            }
            for _, r in df.iterrows()
            if _safe(r["net"]) is not None  # 跳过完全无效行
        ]
    return result


def load_holdings() -> pd.DataFrame:
    files = sorted(HOLDINGS_DIR.glob("*.csv"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_csv(f, dtype={"code": str}) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values(["date", "code"]).drop_duplicates(["date", "code"])
    for col in ["close", "shares", "mktval", "pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── 构建 flow.json ───────────────────────────────────────────

def build_flow_json(flow: dict):
    out = {"updated": str(date.today()), **flow}
    _dump(out, SITE_DIR / "flow.json")
    total = sum(len(v) for v in flow.values())
    log.info(f"flow.json: 沪港通{len(flow.get('hu',[]))}行，深港通{len(flow.get('shen',[]))}行")


# ── 构建 ranking.json ────────────────────────────────────────

def build_ranking_json(df: pd.DataFrame, industry_map: dict[str, dict] | None = None):
    if df.empty:
        log.warning("holdings 数据为空，跳过 ranking.json")
        return

    if industry_map is None:
        industry_map = {}

    dates = sorted(df["date"].unique())
    latest_date = dates[-1]
    latest = df[df["date"] == latest_date].set_index("code")

    # 预计算逐日净变动：Δshares × (close_prev + close) / 2
    # date 列已是字符串，按 [code, date] 排序后 shift(1) 即为上一交易日
    df_daily = df.sort_values(["code", "date"]).copy()
    df_daily["_shares_prev"] = df_daily.groupby("code")["shares"].shift(1)
    df_daily["_close_prev"]  = df_daily.groupby("code")["close"].shift(1)
    df_daily["_delta"]       = df_daily["shares"] - df_daily["_shares_prev"]
    df_daily["_avg_close"]   = (df_daily["close"] + df_daily["_close_prev"]) / 2
    df_daily["_daily_val"]   = df_daily["_delta"] * df_daily["_avg_close"]

    rankings = {"updated": str(date.today()), "latest_date": latest_date}

    for period, offset in PERIOD_OFFSETS.items():
        if offset is None:
            # 1d：直接取上一个实际交易日
            if len(dates) < 2:
                rankings[period] = []
                continue
            earlier_date = dates[-2]
        else:
            cutoff = (pd.Timestamp(latest_date) - offset).strftime("%Y-%m-%d")
            earlier_date = _find_ref_date(dates, cutoff)
            if earlier_date is None or earlier_date == latest_date:
                rankings[period] = []
                continue

        earlier = df[df["date"] == earlier_date].set_index("code")

        merged = latest.join(
            earlier[["shares", "pct"]].rename(
                columns={"shares": "shares_prev", "pct": "pct_prev"}
            ),
            how="left",
        )
        merged["shares_prev"] = merged["shares_prev"].fillna(0)
        merged["pct_prev"]    = merged["pct_prev"].fillna(0)

        merged["net_shares"] = (merged["shares"] - merged["shares_prev"]).round(0).astype("int64")
        merged["pct_chg"]    = (merged["pct"] - merged["pct_prev"]).round(4)

        # 净买入金额 = 区间 (earlier_date, latest_date] 内每日净变动之和
        # 每日净变动 = Δshares × (close_prev + close) / 2
        window = df_daily[
            (df_daily["date"] > earlier_date) &
            (df_daily["date"] <= latest_date)
        ]
        net_val = window.groupby("code")["_daily_val"].sum().rename("net_value")
        merged = merged.join(net_val, how="left")
        merged["net_value"] = merged["net_value"].fillna(0).round(0).astype("int64")

        records = [
            {
                "code": code,
                "name": str(row["name"]),
                "gics_l1":    industry_map.get(str(code), {}).get("gics_l1", ""),
                "gics_l2":    industry_map.get(str(code), {}).get("gics_l2", ""),
                "close":      _safe(row["close"],      3),
                "pct":        _safe(row["pct"],        4),
                "pct_chg":    _safe(row["pct_chg"],    4),
                "net_shares": int(row["net_shares"]),
                "net_value":  int(row["net_value"]),
            }
            for code, row in merged.iterrows()
            if _safe(row["close"]) is not None
        ]
        rankings[period] = records
        log.info(f"  {period}: {len(records)} 只股票，参考日期 {earlier_date} → {latest_date}")

    _dump(rankings, SITE_DIR / "ranking.json")
    log.info("ranking.json 写入完成")


# ── 构建个股历史 stocks/{code}.json ─────────────────────────

def build_stock_histories(df: pd.DataFrame):
    if df.empty:
        return

    codes = df["code"].unique()
    log.info(f"生成 {len(codes)} 只股票历史文件...")

    for code in codes:
        stock = df[df["code"] == code].sort_values("date")
        name = stock["name"].iloc[-1]
        out = {
            "code": code,
            "name": name,
            "dates": stock["date"].tolist(),
            "pct": [round(float(x), 4) if pd.notna(x) else None for x in stock["pct"]],
            "shares": [int(x) if pd.notna(x) else None for x in stock["shares"]],
        }
        _dump(out, STOCKS_DIR / f"{code}.json")

    log.info(f"stocks/ 目录：{len(codes)} 个文件")


# ── 主入口 ───────────────────────────────────────────────────

def main():
    log.info("=== 构建网站数据 ===")

    log.info("加载资金流向...")
    flow = load_flow()
    build_flow_json(flow)

    log.info("加载行业分类...")
    industry_map = load_industry()

    log.info("加载持股数据...")
    df = load_holdings()
    log.info(f"  共 {len(df)} 行，{df['date'].nunique() if not df.empty else 0} 个交易日，"
             f"{df['code'].nunique() if not df.empty else 0} 只股票")

    build_ranking_json(df, industry_map)
    build_stock_histories(df)

    log.info("=== build_site 完成 ===")


if __name__ == "__main__":
    main()
