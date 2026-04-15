'use strict';

/**
 * HK Buyback Crawler
 * Source: https://www3.hkexnews.hk/reports/sharerepur/documents/SRRPT{YYYYMMDD}.xls
 * T+1: SRRPT20260410.xls contains trading data for 2026-04-09
 *
 * Env vars (set by GitHub Actions workflow_dispatch inputs):
 *   DATE_FROM  YYYY-MM-DD  start trading date for historical backfill
 *   DATE_TO    YYYY-MM-DD  end trading date for historical backfill
 *   (both absent → fetch last 8 calendar days)
 */

const https = require('https');
const fs    = require('fs');
const path  = require('path');
const XLSX  = require('xlsx');

const DATA_FILE = path.join(__dirname, 'data', 'hk-buybacks.json');
const UA        = 'HK-Buyback-Tracker/1.0 research-contact@researchuse.com';
const DELAY_MS  = 300;

// ── Target stocks ────────────────────────────────────────────────────────────

const TARGET = {
  700:  { cn: '腾讯',    en: 'Tencent',        code: '0700' },
  9988: { cn: '阿里巴巴', en: 'Alibaba',         code: '9988' },
  9999: { cn: '网易',    en: 'NetEase',         code: '9999' },
  3690: { cn: '美团',    en: 'Meituan',         code: '3690' },
  9961: { cn: '携程',    en: 'Trip.com',        code: '9961' },
  9618: { cn: '京东',    en: 'JD.com',          code: '9618' },
  9888: { cn: '百度',    en: 'Baidu',           code: '9888' },
  1024: { cn: '快手',    en: 'Kuaishou',        code: '1024' },
  1698: { cn: '腾讯音乐', en: 'TME',             code: '1698' },
  2423: { cn: '贝壳',    en: 'KE Holdings',     code: '2423' },
  9626: { cn: 'B站',     en: 'Bilibili',        code: '9626' },
  2076: { cn: 'BOSS直聘', en: 'BOSS Zhipin',    code: '2076' },
  9901: { cn: '新东方',   en: 'New Oriental',   code: '9901' },
  772:  { cn: '阅文',    en: 'China Literature', code: '0772' },
  9898: { cn: '微博',    en: 'Weibo',           code: '9898' },
};

// ── Utilities ────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/** Download binary content as Buffer */
async function getBuffer(url, retries = 3) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': UA },
      timeout: 20000,
    }, res => {
      if (res.statusCode === 404) {
        res.resume();
        return resolve(null);   // non-trading day
      }
      if ([301, 302, 307, 308].includes(res.statusCode)) {
        res.resume();
        const loc = res.headers.location;
        if (!loc) return reject(new Error('Redirect without location'));
        return getBuffer(loc.startsWith('http') ? loc : 'https://www3.hkexnews.hk' + loc, retries)
          .then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end',  () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', reject);
  }).catch(err => {
    if (retries > 0) {
      return sleep(1000).then(() => getBuffer(url, retries - 1));
    }
    throw err;
  });
}

// ── XLS parsing ──────────────────────────────────────────────────────────────

function parseNum(val) {
  if (val == null || val === '') return null;
  const s = String(val).replace(/HKD|USD/g, '').replace(/,/g, '').trim();
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

function parseDate(val) {
  // '2026/04/09' → '2026-04-09'
  if (!val) return null;
  const m = String(val).trim().match(/^(\d{4})\/(\d{2})\/(\d{2})$/);
  return m ? `${m[1]}-${m[2]}-${m[3]}` : null;
}

/**
 * Parse an SRRPT .xls buffer.
 * Returns array of buyback records for target stocks only.
 */
function parseXLS(buffer) {
  const wb   = XLSX.read(buffer, { type: 'buffer' });
  const ws   = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });

  // Find header row: the row where col-0 === 'Company'
  let headerIdx = -1;
  for (let i = 0; i < rows.length; i++) {
    if (String(rows[i][0]).trim() === 'Company') { headerIdx = i; break; }
  }
  if (headerIdx === -1) return [];

  const results = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const row = rows[i];
    const companyHK = String(row[0] || '').trim();
    if (!companyHK || companyHK.startsWith('*')) continue;

    const rawCode = parseFloat(row[1]);
    if (isNaN(rawCode)) continue;
    const code = Math.round(rawCode);
    if (!TARGET[code]) continue;

    const tradingDate = parseDate(String(row[3]));
    if (!tradingDate) continue;

    const sharesRaw = parseNum(String(row[4]));
    results.push({
      id:           `${tradingDate}-${code}`,
      stockCode:    TARGET[code].code,
      companyCN:    TARGET[code].cn,
      companyEN:    TARGET[code].en,
      companyHK:    companyHK,
      tradingDate:  tradingDate,
      shares:       sharesRaw != null ? Math.round(sharesRaw) : null,
      priceHigh:    parseNum(String(row[5])),
      priceLow:     parseNum(String(row[6])),
      aggregateHKD: parseNum(String(row[7])),
      method:       String(row[8] || '').trim(),
      mandatePct:   parseNum(String(row[13])),
    });
  }
  return results;
}

// ── Date helpers ─────────────────────────────────────────────────────────────

function toYYYYMMDD(d) {
  const y  = d.getUTCFullYear();
  const mo = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dy = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${mo}${dy}`;
}

function addDays(d, n) {
  const r = new Date(d);
  r.setUTCDate(r.getUTCDate() + n);
  return r;
}

function parseYMD(s) {
  // 'YYYY-MM-DD' → UTC Date
  const [y, m, d] = s.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // 1. Load existing data
  let db = { lastUpdated: null, totalCount: 0, buybacks: [] };
  if (fs.existsSync(DATA_FILE)) {
    db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  }
  const existingIds = new Set(db.buybacks.map(b => b.id));

  // 2. Determine date range
  //    SRRPT file_date = trading_date + 1 day
  //    We iterate over file_dates and request each one.
  let fileStart, fileEnd;

  if (process.env.DATE_FROM && process.env.DATE_TO) {
    // Historical backfill mode
    fileStart = addDays(parseYMD(process.env.DATE_FROM), 1);
    fileEnd   = addDays(parseYMD(process.env.DATE_TO),   1);
    console.log(`[mode] backfill: trading ${process.env.DATE_FROM} → ${process.env.DATE_TO}`);
  } else {
    // Default: last 8 calendar days (covers ~5 trading days)
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    fileStart = addDays(today, -8);
    fileEnd   = today;
    console.log('[mode] weekly update: last 8 days');
  }

  // 3. Fetch and parse each file date
  const allNew = [];
  let current  = fileStart;

  while (current <= fileEnd) {
    const dateStr = toYYYYMMDD(current);
    const url     = `https://www3.hkexnews.hk/reports/sharerepur/documents/SRRPT${dateStr}.xls`;

    let buffer;
    try {
      buffer = await getBuffer(url);
    } catch (e) {
      console.error(`  [${dateStr}] error: ${e.message}`);
      current = addDays(current, 1);
      await sleep(DELAY_MS);
      continue;
    }

    if (!buffer) {
      console.log(`  [${dateStr}] 404 (non-trading day)`);
    } else {
      const records = parseXLS(buffer);
      const newRecs = records.filter(r => !existingIds.has(r.id));
      newRecs.forEach(r => existingIds.add(r.id));
      allNew.push(...newRecs);

      if (newRecs.length > 0) {
        console.log(`  [${dateStr}] +${newRecs.length} records`);
        for (const r of newRecs) {
          const agg = r.aggregateHKD ? `HKD ${(r.aggregateHKD / 1e8).toFixed(3)}bn` : '';
          console.log(`    ${r.companyCN}(${r.stockCode}) ${r.tradingDate} ${(r.shares||0).toLocaleString()} shares ${agg}`);
        }
      } else {
        console.log(`  [${dateStr}] no new target records`);
      }
    }

    current = addDays(current, 1);
    await sleep(DELAY_MS);
  }

  // 4. Merge and save
  if (allNew.length === 0) {
    console.log('\nNo new records. JSON unchanged.');
    return;
  }

  const merged = [...db.buybacks, ...allNew];
  merged.sort((a, b) => b.tradingDate.localeCompare(a.tradingDate) || a.stockCode.localeCompare(b.stockCode));

  const output = {
    lastUpdated: new Date().toISOString(),
    totalCount:  merged.length,
    buybacks:    merged,
  };

  fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2), 'utf8');
  console.log(`\nSaved ${merged.length} total records (+${allNew.length} new) → data/hk-buybacks.json`);
}

main().catch(e => { console.error('[Fatal]', e.message); process.exit(1); });
