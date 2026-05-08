"""
抓取港股通持股股票的 GICS 行业分类（东方财富 F10 INDUSTRY_TYPE 字段）
输出：data/hkdata/industry.csv  (code, gics_l1, gics_l2, gics_l3, gics_l4)

GICS 层级说明：
  gics_l1 = Sector（11个，如"通讯服务"）        用于筛选/汇总
  gics_l2 = Industry Group（25个，如"媒体与娱乐服务"）  前端表格显示
  gics_l3 = Industry（74个）
  gics_l4 = Sub-Industry（163个）

特性：
- 增量更新：已有 gics_l1 非空的记录不重复抓取
- 多线程并发：默认 6 个 worker
- 格式检测：若发现旧格式（含 industry 列）自动清空并全量重抓
"""
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import pandas as pd

ROOT = Path(__file__).parent.parent
HOLDINGS_DIR = ROOT / "data" / "hkdata" / "holdings"
INDUSTRY_FILE = ROOT / "data" / "hkdata" / "industry.csv"

WORKERS = 6
_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


def fetch_one(code: str) -> tuple:
    """返回 (code, gics_l1, gics_l2, gics_l3, gics_l4)"""
    params = {
        "reportName": "RPT_HKF10_INFO_ORGPROFILE",
        "columns": "SECURITY_CODE,INDUSTRY_TYPE",
        "filter": f'(SECUCODE="{code}.HK")',
        "pageNumber": "1",
        "pageSize": "1",
        "source": "F10",
        "client": "PC",
    }
    try:
        r = requests.get(_URL, params=params, headers=_HEADERS, timeout=12)
        data = r.json()
        if not data.get("result") or not data["result"].get("data"):
            return code, "", "", "", ""
        industry_type = data["result"]["data"][0].get("INDUSTRY_TYPE") or ""
        parts = [p.strip() for p in str(industry_type).split("-") if p.strip()]
        while len(parts) < 4:
            parts.append("")
        return (code, parts[0], parts[1], parts[2], parts[3])
    except Exception as e:
        log.debug(f"{code} 失败: {e}")
        return code, "", "", "", ""


def main():
    # 收集历史持股中出现过的全部 code
    files = sorted(HOLDINGS_DIR.glob("*.csv"))
    if not files:
        log.warning("data/hkdata/holdings/ 目录为空，无股票可抓")
        return
    codes_all = set()
    for f in files:
        df = pd.read_csv(f, dtype={"code": str}, usecols=["code"])
        codes_all.update(df["code"].tolist())
    log.info(f"持股股票总数: {len(codes_all)}")

    # 读取已有映射（检测格式）
    existing: dict[str, tuple] = {}
    if INDUSTRY_FILE.exists():
        ex = pd.read_csv(INDUSTRY_FILE, dtype=str)
        ex = ex.fillna("")
        if "gics_l1" in ex.columns:
            # 新格式：只保留 gics_l1 非空的记录
            for _, row in ex.iterrows():
                if row.get("gics_l1", ""):
                    existing[row["code"]] = (
                        row.get("gics_l1", ""), row.get("gics_l2", ""),
                        row.get("gics_l3", ""), row.get("gics_l4", ""),
                    )
            log.info(f"已有 GICS 映射（新格式）: {len(existing)} 条")
        else:
            log.info("检测到旧格式 industry.csv（HKEX行业），将全量重新抓取 GICS 分类")

    missing = sorted(c for c in codes_all if c not in existing)
    log.info(f"需要抓取: {len(missing)} 只")

    results: dict[str, tuple] = dict(existing)

    if missing:
        done = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(fetch_one, c): c for c in missing}
            for fut in as_completed(futures):
                code, l1, l2, l3, l4 = fut.result()
                results[code] = (l1, l2, l3, l4)
                done += 1
                if done % 50 == 0 or done == len(missing):
                    log.info(f"  进度: {done}/{len(missing)}")

    # 保存
    rows = sorted(
        [{"code": c, "gics_l1": t[0], "gics_l2": t[1], "gics_l3": t[2], "gics_l4": t[3]}
         for c, t in results.items()],
        key=lambda x: x["code"]
    )
    # 补充未抓到的 code（返回空串）
    fetched_codes = {r["code"] for r in rows}
    for c in codes_all:
        if c not in fetched_codes:
            rows.append({"code": c, "gics_l1": "", "gics_l2": "", "gics_l3": "", "gics_l4": ""})
    rows.sort(key=lambda x: x["code"])

    pd.DataFrame(rows).to_csv(INDUSTRY_FILE, index=False)
    filled = sum(1 for r in rows if r["gics_l1"])
    log.info(f"已保存 {INDUSTRY_FILE.name}：{len(rows)} 条，有效 {filled} 条")

    # 输出 L1 分布统计
    from collections import Counter
    c = Counter(r["gics_l1"] for r in rows if r["gics_l1"])
    log.info("GICS Sector 分布：")
    for ind, cnt in c.most_common():
        log.info(f"  {ind}: {cnt}")


if __name__ == "__main__":
    main()
