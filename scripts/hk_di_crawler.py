'use strict' if False else None  # noqa — Python file

"""
港股权益披露爬虫 (HKEX Disclosure of Interests)
数据源：https://di.hkex.com.hk/di/NSAllFormList.aspx
触发方式：
  - 无参数：抓取最近 90 天增量
  - DATE_FROM / DATE_TO 环境变量：历史回填模式

输出：data/hk-di.json
"""

import re, time, json, os, sys, urllib.parse, urllib.request, http.cookiejar
from datetime import date, timedelta
from pathlib import Path

# ── 公司配置（14家，与港股回购共用） ─────────────────────────────────────────
COMPANIES = [
    {'code': '0100', 'sid': '528986', 'corpn': 'MiniMax+Group+Inc.+-+W',                                          'cn': 'MiniMax',   'en': 'MiniMax'},
    {'code': '0700', 'sid': '6893',   'corpn': '%e9%a8%b0%e8%a8%8a%e6%8e%a7%e8%82%a1%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8', 'cn': '腾讯',    'en': 'Tencent'},
    {'code': '0772', 'sid': '183732', 'corpn': '%e9%96%b1%e6%96%87%e9%9b%86%e5%9c%98',                            'cn': '阅文',    'en': 'China Literature'},
    {'code': '0780', 'sid': '224699', 'corpn': '%e5%90%8c%e7%a8%8b%e6%97%85%e8%a1%8c%e6%8e%a7%e8%82%a1%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8', 'cn': '同程旅行', 'en': 'Tongcheng Travel'},
    {'code': '1024', 'sid': '322631', 'corpn': '%e5%bf%ab%e6%89%8b%e7%a7%91%e6%8a%80+%e2%80%93+W',               'cn': '快手',    'en': 'Kuaishou'},
    {'code': '1357', 'sid': '146166', 'corpn': '%e7%be%8e%e5%9c%96%e5%85%ac%e5%8f%b8',                            'cn': '美图',    'en': 'Meitu'},
    {'code': '2076', 'sid': '421844', 'corpn': '%e7%9c%8b%e6%ba%96%e7%a7%91%e6%8a%80%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8+-+W', 'cn': 'BOSS直聘', 'en': 'BOSS Zhipin'},
    {'code': '2400', 'sid': '261096', 'corpn': '%e5%bf%83%e5%8b%95%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8',         'cn': '心动公司', 'en': 'XD Inc.'},
    {'code': '2423', 'sid': '394775', 'corpn': '%e8%b2%9d%e6%ae%bc%e6%8e%a7%e8%82%a1%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8+-+W', 'cn': '贝壳',    'en': 'KE Holdings'},
    {'code': '2513', 'sid': '528801', 'corpn': '%e5%8c%97%e4%ba%ac%e6%99%ba%e8%ad%9c%e8%8f%af%e7%ab%a0%e7%a7%91%e6%8a%80%e8%82%a1%e4%bb%bd%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8++-+H%e8%82%a1', 'cn': '智谱', 'en': 'Zhipu AI'},
    {'code': '3690', 'sid': '217030', 'corpn': '%e7%be%8e%e5%9c%98+-+W',                                          'cn': '美团',    'en': 'Meituan'},
    {'code': '9626', 'sid': '331630', 'corpn': '%e5%97%b6%e5%93%a9%e5%97%b6%e5%93%a9%e8%82%a1%e4%bb%bd%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8+-+W', 'cn': 'B站', 'en': 'Bilibili'},
    {'code': '9899', 'sid': '369850', 'corpn': '%e7%b6%b2%e6%98%93%e9%9b%b2%e9%9f%b3%e6%a8%82%e8%82%a1%e4%bb%bd%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8', 'cn': '网易云音乐', 'en': 'NetEase Cloud Music'},
    {'code': '9988', 'sid': '259524', 'corpn': '%e9%98%bf%e9%87%8c%e5%b7%b4%e5%b7%b4%e9%9b%86%e5%9c%98%e6%8e%a7%e8%82%a1%e6%9c%89%e9%99%90%e5%85%ac%e5%8f%b8+-+W', 'cn': '阿里巴巴', 'en': 'Alibaba'},
]

# 交易类型代码 → 可读标签
TX_LABELS = {
    '1201': '市场买卖', '1104': '场外增持', '1205': '场外减持',
    '1202': '场外交易', '1113': '内部变化',  '1316': '股权激励',
    '1311': '行权',     '1401': '衍生品',    '1405': '衍生品',
    '1703': '质押',     '1704': '质押变动',  '1303': '类别变更',
    '1314': '类别变更', '1313': '类别变更',
}

UA       = 'HK-DI-Tracker/1.0 research-contact@researchuse.com'
BASE_URL = 'https://di.hkex.com.hk/di'
DELAY    = 0.8   # seconds between requests
DATA_FILE = Path(__file__).parent.parent / 'data' / 'hk-di.json'

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def make_opener():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

def fetch(opener, url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8'})
            return opener.open(req, timeout=30).read().decode('utf-8')
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise

# ── HTML parsing ──────────────────────────────────────────────────────────────

def parse_rows(html, stock_code, company):
    """解析 NSAllFormList 页面的表格行，返回结构化 list。"""
    records = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
    for row in rows:
        if 'tbCell' not in row:
            continue
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        if len(tds) < 8:
            continue

        def clean(td):
            t = re.sub(r'<br\s*/?>', '|', td, flags=re.IGNORECASE)
            t = re.sub(r'<[^>]+>', '', t)
            return re.sub(r'\s+', ' ', t).replace('&nbsp;', '').replace('&quot;', '"').strip()

        c = [clean(td) for td in tds]
        notice_id = c[0]
        if not re.match(r'^[A-Z]{2}\d+', notice_id):
            continue

        # 类别：CS/IS=大股东, DA=董事高管
        prefix = notice_id[:2]
        if prefix in ('CS', 'IS'):
            category = 'SS'          # Substantial Shareholder
        elif prefix == 'DA':
            category = 'DIR'         # Director
        else:
            category = 'OTHER'

        # 交易类型代码（去掉(L)(S)）
        tx_code_raw = c[2].split('|')[0].replace('(L)', '').replace('(S)', '').strip()
        tx_code = tx_code_raw if tx_code_raw else ''
        tx_label = TX_LABELS.get(tx_code, f'代码{tx_code}' if tx_code else '—')

        # 变动股数（取L方向；符号由compareMethod=比较前后持仓 决定，先存绝对值）
        delta_raw = c[3].split('|')[0].replace(',', '').replace('(L)', '').replace('(S)', '').strip()
        try:
            delta_abs = int(delta_raw)
        except ValueError:
            delta_abs = 0

        # 持股总量（L方向）
        total_raw = c[5].split('|')[0].replace(',', '').replace('(L)', '').replace('(S)', '').strip()
        try:
            total_shares = int(total_raw)
        except ValueError:
            total_shares = 0

        # 持股比例
        pct_raw = c[6].split('|')[0].replace('(L)', '').replace('(S)', '').strip()
        try:
            pct_holding = float(pct_raw)
        except ValueError:
            pct_holding = 0.0

        # 成交价（可能为空/&nbsp;）
        price = c[4].strip() if c[4].strip() and c[4].strip() != '&nbsp;' else ''

        # 披露日期 DD/MM/YYYY → YYYY-MM-DD
        date_raw = c[7].strip()
        date_m = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_raw)
        disclosure_date = f'{date_m.group(3)}-{date_m.group(2)}-{date_m.group(1)}' if date_m else date_raw

        # 原文 URL（使用 notice_id 构建，Form 2=大股东, Form 3A=董事）
        if prefix in ('CS', 'IS'):
            form_page = 'NSForm2'
        elif prefix == 'DA':
            form_page = 'NSForm3A'
        else:
            form_page = 'NSForm2'
        notice_url = (f'{BASE_URL}/{form_page}.aspx?fn={notice_id}'
                      f'&sa2=an&sid={company["sid"]}'
                      f'&corpn={company["corpn"]}'
                      f'&sd=01%2f01%2f2020&ed=31%2f12%2f2030'
                      f'&lang=ZH&g_lang=zh-HK')

        records.append({
            'id':             notice_id,
            'stockCode':      stock_code,
            'companyCN':      company['cn'],
            'companyEN':      company['en'],
            'filerName':      c[1].strip(),
            'category':       category,
            'txCode':         tx_code,
            'txLabel':        tx_label,
            'deltaSharesAbs': delta_abs,   # 绝对值；方向由前端或后处理计算
            'price':          price,
            'totalShares':    total_shares,
            'pctHolding':     pct_holding,
            'disclosureDate': disclosure_date,
            'noticeUrl':      notice_url,
        })
    return records

def get_total_count(html):
    m = re.search(r'id="lblRecCount"[^>]*>(\d+)<', html)
    return int(m.group(1)) if m else 0

# ── Per-company fetch ─────────────────────────────────────────────────────────

def fetch_company(company, date_from, date_to):
    """抓取单个公司在 [date_from, date_to] 区间内的全部披露记录。"""
    opener = make_opener()
    # 建立 session
    fetch(opener, f'{BASE_URL}/NSSrchMethod.aspx?src=MAIN&lang=ZH&g_lang=zh-HK')
    time.sleep(DELAY)

    def build_url(page=1):
        sd = date_from.strftime('%d/%m/%Y')
        ed = date_to.strftime('%d/%m/%Y')
        sd_enc = sd.replace('/', '%2f')
        ed_enc = ed.replace('/', '%2f')
        return (f'{BASE_URL}/NSAllFormList.aspx?sa2=an&sid={company["sid"]}'
                f'&corpn={company["corpn"]}'
                f'&sd={sd}&ed={ed}'
                f'&cid=0&sa1=cl&scsd={sd_enc}&sced={ed_enc}'
                f'&sc={company["code"]}&src=MAIN&lang=ZH&g_lang=zh-HK&pg={page}')

    html1 = fetch(opener, build_url(1))
    total = get_total_count(html1)
    records = parse_rows(html1, company['code'], company)
    print(f'  [{company["code"]} {company["cn"]}] 共{total}条，第1页{len(records)}条')

    page = 2
    while len(records) < total and page <= 20:   # 安全上限20页
        time.sleep(DELAY)
        html_n = fetch(opener, build_url(page))
        recs_n = parse_rows(html_n, company['code'], company)
        if not recs_n:
            break
        records.extend(recs_n)
        print(f'    第{page}页 +{len(recs_n)}条，累计{len(records)}条')
        page += 1

    return records

# ── Direction calculation ─────────────────────────────────────────────────────

def compute_signed_delta(records):
    """
    基于同一披露人前后持仓量对比，为每条记录添加 deltaShares（有符号）。
    增持为正，减持为负，内部变化（无真实买卖）保留为0。
    """
    NO_TRADE_CODES = {'1113', '1303', '1314', '1313', '1704', '1703'}
    # 按公司+披露人+日期升序排序，计算方向
    from collections import defaultdict
    last_total = defaultdict(lambda: None)   # key=(stockCode, filerName)

    for r in sorted(records, key=lambda x: x['disclosureDate']):
        key = (r['stockCode'], r['filerName'])
        prev = last_total[key]
        cur  = r['totalShares']
        last_total[key] = cur

        if r['txCode'] in NO_TRADE_CODES or r['deltaSharesAbs'] == 0:
            r['deltaShares'] = 0
        elif prev is not None and cur != 0:
            r['deltaShares'] = cur - prev    # 正=增持，负=减持
        else:
            r['deltaShares'] = r['deltaSharesAbs']   # 首条记录，方向未知，存绝对值

    return records

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # 确定日期范围
    if os.environ.get('DATE_FROM') and os.environ.get('DATE_TO'):
        date_from = date.fromisoformat(os.environ['DATE_FROM'])
        date_to   = date.fromisoformat(os.environ['DATE_TO'])
        print(f'[mode] 历史回填 {date_from} → {date_to}')
    else:
        date_to   = date.today()
        date_from = date_to - timedelta(days=90)   # 默认抓最近90天
        print(f'[mode] 增量更新，最近90天 {date_from} → {date_to}')

    # 加载现有数据
    db = {'lastUpdated': None, 'totalCount': 0, 'disclosures': []}
    if DATA_FILE.exists():
        db = json.loads(DATA_FILE.read_text('utf-8'))
    existing_ids = {r['id'] for r in db['disclosures']}
    print(f'已有记录: {len(existing_ids)} 条')

    # 逐公司抓取
    all_new = []
    for company in COMPANIES:
        print(f'\n处理 {company["code"]} {company["cn"]}...')
        try:
            recs = fetch_company(company, date_from, date_to)
            new_recs = [r for r in recs if r['id'] not in existing_ids]
            all_new.extend(new_recs)
            print(f'  → 新增 {len(new_recs)} 条')
        except Exception as e:
            print(f'  [ERROR] {e}')
        time.sleep(DELAY)

    if not all_new:
        print('\n无新数据，JSON 不变。')
        return

    # 合并 + 计算方向
    merged = db['disclosures'] + all_new
    merged = compute_signed_delta(merged)

    # 按日期倒序、公司升序
    merged.sort(key=lambda r: (r['disclosureDate'], r['stockCode']), reverse=True)

    import datetime as _dt
    db['lastUpdated'] = _dt.datetime.now(_dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    db['totalCount']  = len(merged)
    db['disclosures'] = merged

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), 'utf-8')
    print(f'\n已保存 {len(merged)} 条（+{len(all_new)} 新） → {DATA_FILE}')

if __name__ == '__main__':
    main()
