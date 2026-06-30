/**
 * build-buyback-weekly.js
 *
 * 读取 data/hk-buybacks.json（全量回购记录，多币种），按交易日的日频汇率
 * 将每条回购金额折算为人民币，按 ISO 周（周一至周日）分桶、按公司汇总，
 * 输出 data/hk-buyback-weekly.json 供前端「回购」tab 顶部的堆叠柱状图使用。
 *
 * 设计要点：
 *  - 全量重算、幂等、无状态：每次都从全量数据重新计算，能自愈迟报/回填记录。
 *  - 汇率源：frankfurter.app（ECB 参考汇率，免费无 key），一次性取回 USD→CNY、
 *    HKD→CNY 的日频时间序列；ECB 不发布周末/节假日，缺口用最近的前一个可用汇率前向填充。
 *    （CNY 为在岸人民币，与离岸 CNH 差异极小，仅用于归一化展示。）
 *  - 容错：若汇率接口调用失败，保留已有的 weekly 文件、不报错退出，避免拖垮整条工作流。
 *
 * 单位：金额统一为「亿元人民币」（数值已除以 1e8）。
 */

const fs   = require('fs');
const path = require('path');

const SRC_FILE = path.join(__dirname, 'data', 'hk-buybacks.json');
const OUT_FILE = path.join(__dirname, 'data', 'hk-buyback-weekly.json');
const FX_BASE  = 'https://api.frankfurter.app';

// ── 币种判定（与前端 index.html 的 getCurrency / isHKMarket 保持一致）──────────
function isHKMarket(method) {
  const m = (method || '').toLowerCase();
  return (m.includes('exchange') && !m.includes('stock exchange')) || m === 'exchange';
}
function getCurrency(row) {
  if (row.currency) return row.currency;
  return isHKMarket(row.method) ? 'HKD' : 'USD';
}

// ── ISO 周起始日（周一），与前端 getWeekStart 一致 ─────────────────────────────
function getWeekStart(dateStr) {
  const d = new Date(dateStr + 'T00:00:00Z');
  const day = d.getUTCDay();
  d.setUTCDate(d.getUTCDate() + (day === 0 ? -6 : 1 - day)); // 移到周一
  return d.toISOString().slice(0, 10);
}
function addDays(dateStr, n) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

// ── 拉取某基础货币兑 CNY 的日频时间序列 ────────────────────────────────────────
async function fetchSeries(base, start, end) {
  const url = `${FX_BASE}/${start}..${end}?from=${base}&to=CNY`;
  const res = await fetch(url, { headers: { 'User-Agent': 'filingtracker-buyback-weekly/1.0' } });
  if (!res.ok) throw new Error(`FX ${base}->CNY HTTP ${res.status}`);
  const json = await res.json();
  const rates = json.rates || {};
  // 转成 [date, rate] 升序数组
  const arr = Object.keys(rates).sort().map(date => [date, rates[date].CNY]);
  if (!arr.length) throw new Error(`FX ${base}->CNY 无数据`);
  return arr;
}

// 在升序的 [date, rate] 序列里取 <= targetDate 的最近一个汇率（前向填充）
function rateOnOrBefore(series, targetDate) {
  let lo = 0, hi = series.length - 1, ans = series[0][1];
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (series[mid][0] <= targetDate) { ans = series[mid][1]; lo = mid + 1; }
    else hi = mid - 1;
  }
  return ans;
}

async function main() {
  if (!fs.existsSync(SRC_FILE)) {
    console.error(`找不到 ${SRC_FILE}，跳过`);
    return;
  }
  const db = JSON.parse(fs.readFileSync(SRC_FILE, 'utf8'));
  const records = (db.buybacks || []).filter(r => r.tradingDate && r.aggregateHKD);
  if (!records.length) { console.error('无回购记录，跳过'); return; }

  const dates = records.map(r => r.tradingDate).sort();
  const minDate = dates[0];
  const maxDate = dates[dates.length - 1];
  // 起点再往前挪几天，确保最早交易日也能命中“前一个可用汇率”
  const fxStart = addDays(minDate, -10);

  let usdSeries, hkdSeries;
  try {
    [usdSeries, hkdSeries] = await Promise.all([
      fetchSeries('USD', fxStart, maxDate),
      fetchSeries('HKD', fxStart, maxDate),
    ]);
  } catch (e) {
    console.error(`⚠ 汇率获取失败，保留现有 ${path.basename(OUT_FILE)} 不变：${e.message}`);
    return; // 不抛错，避免工作流失败
  }
  const seriesByCcy = { USD: usdSeries, HKD: hkdSeries };

  // 公司中文名映射 + 累计总额（用于公司排序）
  const cnByCode = {};
  const totalByCode = {};
  // 按周聚合：weekStart -> { weekEnd, byCompany: {code: cny}, total }
  const weeks = {};

  for (const r of records) {
    const ccy = getCurrency(r);
    const series = seriesByCcy[ccy];
    if (!series) { console.error(`未知币种 ${ccy}（${r.id}），跳过`); continue; }
    const rate = rateOnOrBefore(series, r.tradingDate);   // 1 单位外币 = rate 元 CNY
    const cny  = r.aggregateHKD * rate;                    // aggregateHKD 实为原币种金额

    const code = r.stockCode;
    cnByCode[code]    = r.companyCN || code;
    totalByCode[code] = (totalByCode[code] || 0) + cny;

    const ws = getWeekStart(r.tradingDate);
    if (!weeks[ws]) weeks[ws] = { weekEnd: addDays(ws, 6), byCompany: {}, total: 0 };
    weeks[ws].byCompany[code] = (weeks[ws].byCompany[code] || 0) + cny;
    weeks[ws].total += cny;
  }

  // 公司列表：按累计回购额降序（决定默认配色/图例顺序）
  const YI = 1e8;
  const companies = Object.keys(totalByCode)
    .sort((a, b) => totalByCode[b] - totalByCode[a])
    .map(code => ({ code, cn: cnByCode[code], totalYi: +(totalByCode[code] / YI).toFixed(4) }));

  const weekList = Object.keys(weeks).sort().map(ws => {
    const w = weeks[ws];
    const byCompany = {};
    for (const code of Object.keys(w.byCompany)) {
      byCompany[code] = +(w.byCompany[code] / YI).toFixed(4);  // 亿元
    }
    return {
      weekStart: ws,
      weekEnd:   w.weekEnd,
      totalYi:   +(w.total / YI).toFixed(4),
      byCompany,
    };
  });

  const out = {
    generatedAt: new Date().toISOString(),
    unit: '亿元人民币',
    fxNote: '汇率源 ECB(frankfurter.app) 日频，缺口前向填充；CNY 为在岸人民币',
    weekDef: 'ISO 周一至周日，weekStart 为周一',
    companies,
    weeks: weekList,
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(out, null, 2));
  console.log(`✓ 已生成 ${path.basename(OUT_FILE)}：${weekList.length} 周，${companies.length} 家公司，` +
              `区间 ${weekList[0]?.weekStart} ~ ${weekList[weekList.length - 1]?.weekStart}`);
}

main().catch(e => { console.error('构建失败：', e); process.exit(1); });
