'use strict';

/**
 * SEC Filing Crawler v2
 * 数据来源：EDGAR Atom Feed（按日期降序，覆盖全部历史）
 * 每条 filing 解析：申报人、持仓截止日、持股数、占总股本%
 * 首次运行自动补充历史数据；之后每次只处理新 filing
 */

const https = require('https');
const fs    = require('fs');
const path  = require('path');

const COMPANIES_FILE = path.join(__dirname, 'companies.json');
const DATA_FILE      = path.join(__dirname, 'data', 'filings.json');
const UA             = 'SEC-Filing-Tracker/2.0 research-contact@researchuse.com';
const DELAY_MS       = 150;   // ~6 req/s，远低于 EDGAR 限速 10 req/s

// 旧格式（2024 及之前）：SC 13D / SC 13G
// 新格式（2025 起 SEC 强制结构化提交）：SCHEDULE 13D / SCHEDULE 13G
const VALID_FORMS = new Set([
  'SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A',
  'SCHEDULE 13D', 'SCHEDULE 13D/A', 'SCHEDULE 13G', 'SCHEDULE 13G/A',
]);

// 前端展示统一用短格式（去掉 SCHEDULE 前缀）
function normalizeFormType(raw) {
  return raw.replace(/^SCHEDULE /, 'SC ');
}

// ── 工具 ───────────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function get(url, retries = 3) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': UA, 'Accept-Encoding': 'identity' },
      timeout: 30000
    }, res => {
      if ([301, 302, 307, 308].includes(res.statusCode)) {
        res.resume();
        const loc = res.headers.location;
        if (!loc) return reject(new Error('Redirect without location'));
        return get(loc.startsWith('http') ? loc : 'https://www.sec.gov' + loc, retries)
          .then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', async () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if ([500, 503].includes(res.statusCode) && retries > 0) {
          console.log(`    HTTP ${res.statusCode} — 重试`);
          await sleep(3000);
          return get(url, retries - 1).then(resolve).catch(reject);
        }
        resolve({ status: res.statusCode, body });
      });
      res.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', async e => {
      if (retries > 0) { await sleep(2000); return get(url, retries - 1).then(resolve).catch(reject); }
      reject(e);
    });
  });
}

// ── Atom Feed 解析 ────────────────────────────────────────────────────────────

function parseAtom(xml) {
  const entries = [];
  const re = /<entry>([\s\S]*?)<\/entry>/g;
  let m;
  while ((m = re.exec(xml)) !== null) {
    const b = m[1];
    const type = (b.match(/<filing-type>([^<]+)/) || [])[1]?.trim();
    if (!type || !VALID_FORMS.has(type)) continue;
    const accn = (b.match(/<accession-number>([^<]+)/) || [])[1]?.trim();
    const date = (b.match(/<filing-date>([^<]+)/)     || [])[1]?.trim();
    const href = (b.match(/<filing-href>([^<]+)/)     || [])[1]?.trim();
    if (!accn || !date) continue;
    entries.push({ formType: type, accession: accn, filedDate: date, indexUrl: href || '' });
  }
  return entries;
}

// ── Index 页 → 主文档 URL ────────────────────────────────────────────────────

function findPrimaryDoc(html) {
  // 按优先级查找主文档：.xml（新格式）> .htm > .txt
  // 均取第一个非 -index 的 /Archives/ 链接
  const base = 'https://www.sec.gov';
  for (const ext of ['xml', 'htm', 'txt']) {
    const re = new RegExp(`href="(\\/Archives\\/edgar\\/data\\/[^"]+\\.${ext})"`, 'gi');
    let m;
    while ((m = re.exec(html)) !== null) {
      if (!m[1].toLowerCase().includes('-index')) {
        return base + m[1];
      }
    }
  }
  return null;
}

// ── 新格式 XML 解析（SCHEDULE 13G/D，2025 年起）────────────────────────────

function parseXmlDoc(xml) {
  function tag(t)  { const m = xml.match(new RegExp(`<${t}[^>]*>([^<]+)</${t}>`,'i')); return m ? m[1].trim() : null; }
  function tags(t) { return [...xml.matchAll(new RegExp(`<${t}[^>]*>([^<]+)</${t}>`,'gi'))].map(m=>m[1].trim()); }

  // 申报人：取所有 reportingPersonName，第一个即主申报人
  const filerName = tag('reportingPersonName');

  // 持仓截止日：MM/DD/YYYY 转 YYYY-MM-DD
  let eventDate = null;
  const rawDate = tag('eventDateRequiresFilingThisStatement');
  if (rawDate) {
    const p = rawDate.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (p) eventDate = `${p[3]}-${p[1].padStart(2,'0')}-${p[2].padStart(2,'0')}`;
  }

  // 持股数：取所有 reportingPersonBeneficiallyOwnedAggregateNumberOfShares，取最大值
  let sharesOwned = null;
  for (const v of tags('reportingPersonBeneficiallyOwnedAggregateNumberOfShares')) {
    const n = parseNum(v);
    if (n !== null && (sharesOwned === null || n > sharesOwned)) sharesOwned = n;
  }

  // 占总股本：取所有 classPercent，取最大值
  let pctOwned = null;
  for (const v of tags('classPercent')) {
    const n = parsePct(v);
    if (n !== null && (pctOwned === null || n > pctOwned)) pctOwned = n;
  }

  return { filerName, eventDate, sharesOwned, pctOwned };
}

// ── 文档解析 ──────────────────────────────────────────────────────────────────

function toText(html) {
  return html
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;|&#160;/g, ' ')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&#\d+;|&[a-z]+;/gi, ' ')
    .replace(/\s+/g, ' ').trim();
}

function parseNum(s) {
  const n = parseInt(String(s || '').replace(/[,\s]/g, ''), 10);
  return isNaN(n) ? null : n;
}

function parsePct(s) {
  const n = parseFloat(String(s || '').replace(/[%,\s]/g, ''));
  return isNaN(n) ? null : n;
}

/**
 * 从文档文本提取关键字段
 *
 * 持股数/占比：取文档中所有封面页的最大值
 *   - 单一申报人：只有一个值，直接取
 *   - GROUP 申报：多个子实体 + 一个合并实体（通常值最大），取最大值即为集团合计
 */
function parseDoc(text) {
  // 持仓截止日期（Date of Event）
  // 文档格式：日期出现在 "(Date of Event...)" 标签之前
  const MONTHS = {january:'01',february:'02',march:'03',april:'04',may:'05',june:'06',
                  july:'07',august:'08',september:'09',october:'10',november:'11',december:'12'};
  let eventDate = null;
  const evM = text.match(/(\w+ \d{1,2},? \d{4})[^(]{0,120}\(?Date of Event/i)
           || text.match(/(\d{4}-\d{2}-\d{2})[^(]{0,120}\(?Date of Event/i);
  if (evM) {
    const raw = evM[1].trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
      eventDate = raw;
    } else {
      const mp = raw.match(/^(\w+)\s+(\d{1,2}),?\s+(\d{4})$/);
      if (mp && MONTHS[mp[1].toLowerCase()]) {
        eventDate = `${mp[3]}-${MONTHS[mp[1].toLowerCase()]}-${mp[2].padStart(2, '0')}`;
      }
    }
  }

  // 申报人姓名（第一个 "NAME OF REPORTING PERSON" — 主申报人/第一封面页）
  let filerName = null;
  const nmM = text.match(/NAME OF REPORTING PERSON\s+([A-Za-z][^\d\n\r]{2,80}?)(?=\s+\d\s+CHECK|\s+I\.R\.S\.|\s+SEC USE)/i);
  if (nmM) filerName = nmM[1].trim().replace(/\s+/g, ' ');
  // 纯文本 SC 13G 格式 "(1) Names of reporting persons."
  if (!filerName) {
    const nm2 = text.match(/\(?1\)?\s*Names? of (?:reporting )?persons?\s*[.:]?\s*([^\n\r(]{3,80})/i);
    if (nm2) filerName = nm2[1].trim();
  }

  // 持股数：找所有封面页 "AGGREGATE AMOUNT BENEFICIALLY OWNED"，取最大值
  let sharesOwned = null;
  const shRe = /AGGREGATE AMOUNT BENEFICIALLY OWNED[\s\S]{0,80}?(\d[\d,]*)/gi;
  let shM;
  while ((shM = shRe.exec(text)) !== null) {
    const n = parseNum(shM[1]);
    if (n !== null && (sharesOwned === null || n > sharesOwned)) sharesOwned = n;
  }
  // 纯文本 13G 备用格式
  if (sharesOwned === null) {
    const alt = text.match(/Amount beneficially owned:\s*(\d[\d,]*)/i);
    if (alt) sharesOwned = parseNum(alt[1]);
  }

  // 占总股本%：找所有封面页，取最大值
  let pctOwned = null;
  const pcRe = /PERCENT OF CLASS[^%]{0,200}?(\d+\.?\d*)\s*%/gi;
  let pcM;
  while ((pcM = pcRe.exec(text)) !== null) {
    const n = parsePct(pcM[1]);
    if (n !== null && (pctOwned === null || n > pctOwned)) pctOwned = n;
  }
  // 纯文本 13G 备用格式
  if (pctOwned === null) {
    const alt = text.match(/Percent of class[\s\S]{0,80}?(\d+\.?\d*)\s*%/i);
    if (alt) pctOwned = parsePct(alt[1]);
  }

  return { filerName, eventDate, sharesOwned, pctOwned };
}

// ── 解析单条 filing：index → 主文档 → 提取字段 ───────────────────────────────

async function enrichFiling(f) {
  if (!f.edgarUrl) return;

  const { status: is, body: iHtml } = await get(f.edgarUrl);
  await sleep(DELAY_MS);
  if (is !== 200) return;

  const docUrl = findPrimaryDoc(iHtml);
  if (!docUrl) return;

  const { status: ds, body: dBody } = await get(docUrl);
  await sleep(DELAY_MS);
  if (ds !== 200) return;

  // 新格式（2025 起）为 XML，旧格式为 HTML/TXT
  const isXml = docUrl.toLowerCase().endsWith('.xml');
  const p = isXml ? parseXmlDoc(dBody) : parseDoc(toText(dBody));

  // 若已有申报人名称（旧数据库），保留；否则用解析结果填充
  if (p.filerName && !f.filerName)  f.filerName  = p.filerName;
  if (p.eventDate  != null)          f.eventDate  = p.eventDate;
  if (p.sharesOwned != null)         f.sharesOwned = p.sharesOwned;
  if (p.pctOwned   != null)          f.pctOwned   = p.pctOwned;
}

// ── 变动量计算 ────────────────────────────────────────────────────────────────

function calculateDeltas(filings) {
  // 按申报日期升序处理，以便逐条计算 delta
  const sorted = [...filings].sort((a, b) =>
    (a.filedDate || '').localeCompare(b.filedDate || '') ||
    (a.accession || '').localeCompare(b.accession || '')
  );

  const last = {}; // key → { sharesOwned, pctOwned }

  for (const f of sorted) {
    // 用规范化后的申报人名作为匹配键（比 CIK 更稳定，跨数据源一致）
    const nameKey = (f.filerName || '').toLowerCase().replace(/[^a-z0-9]/g, '');
    const key = `${f.ticker}||${nameKey || f.filerCik || f.accession}`;

    const prev = last[key];
    f.sharesDelta = (prev && f.sharesOwned != null && prev.sharesOwned != null)
      ? f.sharesOwned - prev.sharesOwned : null;
    f.pctDelta = (prev && f.pctOwned != null && prev.pctOwned != null)
      ? Math.round((f.pctOwned - prev.pctOwned) * 1000) / 1000 : null;

    if (f.sharesOwned != null || f.pctOwned != null) {
      last[key] = { sharesOwned: f.sharesOwned, pctOwned: f.pctOwned };
    }
  }
}

// ── 拉取某公司的新 filing ─────────────────────────────────────────────────────

async function fetchCompany(company, existingIds) {
  // 查两次：旧格式 SC 13（2024 及之前）+ 新格式 SCHEDULE 13（2025 起）
  const base = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${company.cik}` +
               `&dateb=&owner=include&count=100&output=atom`;

  const entries = [];
  for (const type of ['SC+13', 'SCHEDULE+13']) {
    const { status, body } = await get(`${base}&type=${type}`);
    await sleep(DELAY_MS);
    if (status !== 200) { console.log(`  Atom feed [${type}] 错误: HTTP ${status}`); continue; }
    entries.push(...parseAtom(body));
  }

  // 去重（两次查询可能有重叠）
  const seen = new Set();
  const uniq = entries.filter(e => seen.has(e.accession) ? false : (seen.add(e.accession), true));
  console.log(`  Atom feed: ${uniq.length} 条 SC/SCHEDULE 13D/G`);

  const newFilings = [];
  for (const e of uniq) {
    if (existingIds.has(e.accession)) continue;

    // accession 前 10 位是提交方 CIK
    const filerCik = e.accession.replace(/-/g, '').slice(0, 10);

    const f = {
      id:          e.accession,
      ticker:      company.ticker,
      company:     company.name,
      formType:    normalizeFormType(e.formType),
      filedDate:   e.filedDate,
      eventDate:   null,
      filerName:   null,
      filerCik,
      sharesOwned: null,
      pctOwned:    null,
      sharesDelta: null,
      pctDelta:    null,
      edgarUrl:    e.indexUrl,
      accession:   e.accession,
    };

    console.log(`  NEW ${e.formType} ${e.filedDate}  ${e.accession}`);
    try {
      await enrichFiling(f);
      console.log(`    申报人: ${f.filerName || '?'}  持股: ${f.sharesOwned ?? '?'}  占比: ${f.pctOwned ?? '?'}%`);
    } catch (e2) {
      console.log(`    解析失败: ${e2.message}`);
    }
    newFilings.push(f);
  }

  return newFilings;
}

// ── 主程序 ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('╔══════════════════════════════════════╗');
  console.log('║   SEC Filing Crawler v2  中概股追踪  ║');
  console.log('╚══════════════════════════════════════╝\n');

  const companies = JSON.parse(fs.readFileSync(COMPANIES_FILE, 'utf8'));

  let db = { lastUpdated: null, totalCount: 0, filings: [] };
  if (fs.existsSync(DATA_FILE)) {
    try { db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
    catch (_) { console.warn('[警告] 数据文件损坏，从空白开始\n'); }
  }

  const existingIds = new Set(db.filings.map(f => f.id).filter(Boolean));
  console.log(`已有记录: ${db.filings.length} 条 | 追踪公司: ${companies.length} 家\n`);

  // ── 补充历史数据（首次运行，或有旧记录缺少持股字段）─────────────────────
  const toBackfill = db.filings.filter(f => f.sharesOwned == null && f.pctOwned == null);
  if (toBackfill.length > 0) {
    console.log(`补充历史持股数据: ${toBackfill.length} 条记录...\n`);
    let i = 0;
    for (const f of toBackfill) {
      i++;
      process.stdout.write(`  [${i}/${toBackfill.length}] ${f.ticker} ${f.filedDate}  `);
      try {
        await enrichFiling(f);
        console.log(`shares=${f.sharesOwned ?? '?'}  pct=${f.pctOwned ?? '?'}%`);
      } catch (e) {
        console.log(`错误: ${e.message}`);
      }
    }
    console.log('\n历史数据补充完成\n');
  }

  // ── 拉取各公司的新 filing ─────────────────────────────────────────────────
  const allNew = [];
  for (let i = 0; i < companies.length; i++) {
    const co = companies[i];
    process.stdout.write(`[${i + 1}/${companies.length}] ${co.ticker} — `);
    try {
      const fresh = await fetchCompany(co, existingIds);
      allNew.push(...fresh);
      console.log(`+${fresh.length} 条新记录`);
    } catch (e) {
      console.log(`错误: ${e.message}`);
    }
  }

  // ── 合并、重算变动量、排序、保存 ─────────────────────────────────────────
  const merged = [...db.filings, ...allNew];
  calculateDeltas(merged);
  merged.sort((a, b) => (b.filedDate || '').localeCompare(a.filedDate || ''));

  const dir = path.dirname(DATA_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify({
    lastUpdated: new Date().toISOString(),
    totalCount:  merged.length,
    filings:     merged,
  }, null, 2), 'utf8');

  console.log(`\n${'─'.repeat(40)}`);
  console.log(`本次新增: ${allNew.length} 条`);
  console.log(`数据库合计: ${merged.length} 条`);
  console.log(`已保存: data/filings.json`);
}

main().catch(e => { console.error('\n[Fatal]', e.message); process.exit(1); });
