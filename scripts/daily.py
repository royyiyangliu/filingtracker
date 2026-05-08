"""
每日增量更新脚本（GitHub Actions 定时调用）
- 更新资金流向（全量重拉，速度快）
- 更新近14天个股持股（upsert，防漏）
- 调用 build_site.py 重建网站 JSON
"""
import sys
import time
import logging
from datetime import date, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from backfill import (
    FLOW_DIR, HOLDINGS_DIR,
    FLOW_RENAME, HOLDINGS_RENAME,
    retry, _fetch_holdings, _save_holdings
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


def update_flow():
    log.info("更新资金流向...")
    for symbol, fname in [("港股通沪", "hu"), ("港股通深", "shen")]:
        df = retry(lambda s=symbol: ak.stock_hsgt_hist_em(symbol=s))
        df = df.rename(columns=FLOW_RENAME)[list(FLOW_RENAME.values())]
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("date").reset_index(drop=True)
        path = FLOW_DIR / f"{fname}.csv"
        df.to_csv(path, index=False)
        log.info(f"  {symbol}: 最新日期 {df['date'].iloc[-1]}")


def update_holdings():
    log.info("更新近14天持股数据...")
    today = date.today()
    start = (today - timedelta(days=14)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    try:
        df = _fetch_holdings(start, end)
        if df.empty:
            log.warning("  未获取到数据（今日可能为非交易日）")
        else:
            _save_holdings(df)
            log.info(f"  更新 {df['date'].nunique()} 个交易日，{df['code'].nunique()} 只股票")
    except Exception as e:
        log.error(f"  持股数据更新失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    update_flow()
    update_holdings()

    # 重建网站 JSON
    log.info("重建网站数据...")
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "build_site", ROOT / "scripts" / "build_site.py"
    )
    mod = importlib.util.load_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()

    log.info("每日更新完成")
