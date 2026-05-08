"""
抓取港股通持股股票的行业分类（东方财富 stock_hk_company_profile_em）
输出：data/industry.csv  (code, industry)

特性：
- 增量更新：已有记录不重复抓取
- 多线程并发：默认 6 个 worker，约 30 秒完成全量 700+ 只
- 抓取失败的股票记为空字符串，下次重新尝试
"""
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import akshare as ak
import pandas as pd

ROOT = Path(__file__).parent.parent
HOLDINGS_DIR = ROOT / "data" / "hkdata" / "holdings"
INDUSTRY_FILE = ROOT / "data" / "hkdata" / "industry.csv"

WORKERS = 6   # 并发线程数（过高可能触发限流）

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


def fetch_one(code: str) -> tuple[str, str]:
    try:
        df = ak.stock_hk_company_profile_em(symbol=code)
        val = df["所属行业"].iloc[0] if "所属行业" in df.columns else ""
        industry = str(val).strip() if val and str(val) not in ("nan", "None", "") else ""
        return code, industry
    except Exception as e:
        log.debug(f"{code} 失败: {e}")
        return code, ""


def main():
    # 收集历史持股中出现过的全部 code
    files = sorted(HOLDINGS_DIR.glob("*.csv"))
    if not files:
        log.warning("data/holdings/ 目录为空，无股票可抓")
        return
    codes_all = set()
    for f in files:
        df = pd.read_csv(f, dtype={"code": str}, usecols=["code"])
        codes_all.update(df["code"].tolist())
    log.info(f"持股股票总数: {len(codes_all)}")

    # 读取已有映射（只跳过已有且非空的记录）
    existing: dict[str, str] = {}
    if INDUSTRY_FILE.exists():
        ex = pd.read_csv(INDUSTRY_FILE, dtype={"code": str})
        ex["industry"] = ex["industry"].fillna("").astype(str)
        # 只保留非空的已有结果
        existing = {r["code"]: r["industry"] for _, r in ex.iterrows() if r["industry"]}
        log.info(f"已有有效映射: {len(existing)} 条")

    missing = sorted(c for c in codes_all if c not in existing)
    log.info(f"需要抓取: {len(missing)} 只")

    results: dict[str, str] = dict(existing)

    if missing:
        done = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(fetch_one, c): c for c in missing}
            for fut in as_completed(futures):
                code, ind = fut.result()
                results[code] = ind
                done += 1
                if done % 50 == 0 or done == len(missing):
                    log.info(f"  进度: {done}/{len(missing)}")

    # 保存（所有曾出现的 code，无论是否成功）
    rows = sorted(
        [{"code": c, "industry": results.get(c, "")} for c in codes_all],
        key=lambda x: x["code"]
    )
    pd.DataFrame(rows).to_csv(INDUSTRY_FILE, index=False)

    filled = sum(1 for r in rows if r["industry"])
    log.info(f"已保存 {INDUSTRY_FILE.name}：{len(rows)} 条，有效 {filled} 条")

    # 输出行业分布统计
    from collections import Counter
    c = Counter(r["industry"] for r in rows if r["industry"])
    log.info("行业分布（前20）：")
    for ind, cnt in c.most_common(20):
        log.info(f"  {ind}: {cnt}")


if __name__ == "__main__":
    main()
