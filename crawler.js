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
// 内部人交易披露
const VALID_FORMS = new Set([
  'SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A',
  'SCHEDULE 13D', 'SCHEDULE 13D/A', 'SCHEDULE 13G', 'SCHEDULE 13G/A',
  '4', '4/A',
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
      const p = m[1].toLowerCase();
      if (!p.includes('-index') && !p.includes('/xsl')) {
        return base + m[1];
      }
    }
  }
  return null;
}

// ── Form 4 XML 解析（内部人交易）─────────────────────────────────────────────

/**
 * 从 Form 4 XML 提取关键字段
 * 一份 Form 4 对应一个申报人，可含多笔 nonDerivativeTransaction
 * 汇总全部非衍生品交易：净股数、最终持仓、主要交易类型、成交均价
 */
function parseForm4(xml) {
  const filerName  = (xml.match(/<rptOwnerName>([^<]+)<\/rptOwnerName>/i)   || [])[1]?.trim() || null;
  const eventDate  = (xml.match(/<periodOfReport>([^<]+)<\/periodOfReport>/i) || [])[1]?.trim() || null;

  // 职务
  const isDir    = /<isDirector>1<\/isDirector>/i.test(xml);
  const is10Pct  = /<isTenPercentOwner>1<\/isTenPercentOwner>/i.test(xml);
  const titleM   = xml.match(/<officerTitle>([^<]+)<\/officerTitle>/i);
  const roles    = [];
  if (isDir) roles.push('Director');
  if (titleM) roles.push(titleM[1].trim());
  if (is10Pct) roles.push('>10% Owner');
  const insiderTitle = roles.join(' / ') || null;

  // 汇总所有 nonDerivativeTransaction
  const TX_PRIORITY = { S: 5, P: 4, M: 3, F: 2, A: 1 };
  let netShares = 0, lastSharesOwned = null;
  let primaryTxCode = null, primaryPriority = 0;
  let totalValue = 0, pricedShares = 0;

  const blocks = [...xml.matchAll(/<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/gi)];
  for (const [, blk] of blocks) {
    const code    = (blk.match(/<transactionCode>([^<]+)<\/transactionCode>/i)                              || [])[1]?.trim();
    const sharesV = (blk.match(/<transactionShares>[\s\S]*?<value>([^<]+)<\/value>/i)                       || [])[1]?.trim();
    const adCode  = (blk.match(/<transactionAcquiredDisposedCode>[\s\S]*?<value>([^<]+)<\/value>/i)         || [])[1]?.trim();
    const priceV  = (blk.match(/<transactionPricePerShare>[\s\S]*?<value>([^<]+)<\/value>/i)                || [])[1]?.trim();
    const postV   = (blk.match(/<sharesOwnedFollowingTransaction>[\s\S]*?<value>([^<]+)<\/value>/i)         || [])[1]?.trim();

    const shares = parseNum(sharesV);
    if (shares !== null) {
      netShares += (adCode === 'D' ? -shares : shares);
      const price = priceV ? parseFloat(priceV) : NaN;
      if (!isNaN(price) && price > 0 && (code === 'S' || code === 'P')) {
        totalValue  += price * shares;
        pricedShares += shares;
      }
      const prio = TX_PRIORITY[code] || 0;
      if (prio > primaryPriority) { primaryTxCode = code; primaryPriority = prio; }
    }
    if (postV) lastSharesOwned = parseNum(postV);
  }

  const txPrice = pricedShares > 0 ? Math.round(totalValue / pricedShares * 100) / 100 : null;

  return {
    filerName,
    eventDate,
    insiderTitle,
    txCode:     primaryTxCode,
    txShares:   netShares !== 0 ? netShares : null,
    txPrice,
    sharesOwned: lastSharesOwned,
  };
}

// ── 新格式 XML 解析（SCHEDULE 13G/D，2025 年起）────────────────────────────

/**
 * 返回 { pages: [{filerName, sharesOwned, pctOwned}], eventDate }
 * 每个 <coverPageHeaderReportingPersonDetails> 块对应一个申报人
 */
function parseXmlDoc(xml) {
  function tag(t) { const m = xml.match(new RegExp(`<${t}[^>]*>([^<]+)</${t}>`,'i')); return m ? m[1].trim() : null; }

  // 持仓截止日（文档级别，所有申报人共用）
  let eventDate = null;
  const rawDate = tag('eventDateRequiresFilingThisStatement');
  if (rawDate) {
    const p = rawDate.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (p) eventDate = `${p[3]}-${p[1].padStart(2,'0')}-${p[2].padStart(2,'0')}`;
  }

  // 按 <coverPageHeaderReportingPersonDetails> 块解析每个申报人
  const pages = [];
  const blockRe = /<coverPageHeaderReportingPersonDetails>([\s\S]*?)<\/coverPageHeaderReportingPersonDetails>/gi;
  let m;
  while ((m = blockRe.exec(xml)) !== null) {
    const blk = m[1];
    const btag = t => { const r = blk.match(new RegExp(`<${t}[^>]*>([^<]+)</${t}>`,'i')); return r ? r[1].trim() : null; };
    pages.push({
      filerName:   btag('reportingPersonName'),
      sharesOwned: parseNum(btag('reportingPersonBeneficiallyOwnedAggregateNumberOfShares')),
      pctOwned:    parsePct(btag('classPercent')),
    });
  }

  // 兜底：老版 XML 没有 coverPage 块时，退化为全文单次匹配
  if (pages.length === 0) {
    pages.push({
      filerName:   tag('reportingPersonName'),
      sharesOwned: parseNum(tag('reportingPersonBeneficiallyOwnedAggregateNumberOfShares')),
      pctOwned:    parsePct(tag('classPercent')),
    });
  }

  return { pages, eventDate };
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
 * 从文档文本提取所有封面页数据
 * 返回 { pages: [{filerName, sharesOwned, pctOwned}], eventDate }
 *
 * 核心改动：不再全局取最大值，而是把文档按"Name of Reporting Person(s)"标签
 * 切成若干封面页块，每块内独立提取 (name, shares, pct)，保证三者来自同一页。
 */
function parseDoc(text) {
  // 持仓截止日（文档级别）
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

  // 找到所有封面页起始位置（支持全大写和混合大小写两种格式）
  const labelRe = /(?:NAME\s+OF\s+REPORTING\s+PERSON[S]?|Names?\s+of\s+(?:Reporting\s+)?Persons?)\s*:?\s*/gi;
  const labels  = [...text.matchAll(labelRe)];

  const pages = [];

  if (labels.length > 0) {
    for (let i = 0; i < labels.length; i++) {
      // 每个封面页块：从标签结尾到下一个标签起始（或 +4000 字符）
      const contentStart = labels[i].index + labels[i][0].length;
      const blockEnd     = i + 1 < labels.length ? labels[i + 1].index : Math.min(contentStart + 4000, text.length);
      const blk          = text.slice(contentStart, blockEnd);

      // 申报人名称：只取字母/空格/标点，遇到数字即停（数字是下一行行号）
      const nameM    = blk.match(/^([A-Za-z][A-Za-z .,'\-]{1,79})/);
      const filerName = nameM ? nameM[1].trim().replace(/\s+/g, ' ') : null;

      // 持股数（优先全大写格式，兜底混合大小写）
      const shM = blk.match(/AGGREGATE AMOUNT BENEFICIALLY OWNED[\s\S]{0,80}?(\d[\d,]{2,})/i)
               || blk.match(/Amount beneficially owned:?\s*(\d[\d,]{2,})/i);
      const sharesOwned = shM ? parseNum(shM[1]) : null;

      // 占总股本%
      const pcM = blk.match(/PERCENT OF CLASS[^%]{0,200}?(\d+\.?\d*)\s*%/i)
               || blk.match(/Percent of class[\s\S]{0,80}?(\d+\.?\d*)\s*%/i);
      const pctOwned = pcM ? parsePct(pcM[1]) : null;

      pages.push({ filerName, sharesOwned, pctOwned });
    }
  }

  // 兜底：找不到任何封面页标签 → 全文取一次（老式纯文本格式）
  if (pages.length === 0) {
    let sharesOwned = null;
    const shRe = /AGGREGATE AMOUNT BENEFICIALLY OWNED[\s\S]{0,80}?(\d[\d,]*)/gi;
    let shM;
    while ((shM = shRe.exec(text)) !== null) {
      const n = parseNum(shM[1]);
      if (n !== null && (sharesOwned === null || n > sharesOwned)) sharesOwned = n;
    }
    if (sharesOwned === null) {
      const alt = text.match(/Amount beneficially owned:\s*(\d[\d,]*)/i);
      if (alt) sharesOwned = parseNum(alt[1]);
    }
    const pcM = text.match(/PERCENT OF CLASS[^%]{0,200}?(\d+\.?\d*)\s*%/i)
             || text.match(/Percent of class[\s\S]{0,80}?(\d+\.?\d*)\s*%/i);
    pages.push({ filerName: null, sharesOwned, pctOwned: pcM ? parsePct(pcM[1]) : null });
  }

  return { pages, eventDate };
}

/**
 * 判断 GROUP filing 是"机构母子结构"还是"独立个体联合"
 *
 * 机构母子（Scenario B）：所有申报人名称含共同关键词（如都含"BlackRock"）
 *   → 取持股最多那条（母公司合并口径），返回单条
 *
 * 独立个体（Scenario A）：名称无共同关键词（如 Jack Ma + Tsai Joseph C）
 *   → 每人独立，返回全部
 */
function groupOrSplit(pages) {
  if (pages.length <= 1) return pages;

  const STOP = new Set([
    'inc', 'ltd', 'llc', 'lp', 'corp', 'plc', 'gmbh', 'bv',
    'fund', 'funds', 'trust', 'group', 'holdings', 'holding',
    'management', 'advisors', 'advisor', 'capital', 'partners', 'partner',
    'investment', 'investments', 'asset', 'assets', 'securities',
    'financial', 'company', 'international', 'global', 'limited',
    'association', 'bank', 'the', 'and', 'for', 'of',
  ]);

  const kws = pages.map(p =>
    new Set((p.filerName || '').toLowerCase().split(/\W+/).filter(w => w.length > 3 && !STOP.has(w)))
  );

  // 若所有名称都有关键词且存在全员共享的关键词 → 同一机构
  if (kws.every(s => s.size > 0)) {
    const shared = [...kws[0]].find(w => kws.every(s => s.has(w)));
    if (shared) {
      // 取持股最多的一条（母公司合并口径）
      return [pages.reduce((a, b) => (a.sharesOwned || 0) >= (b.sharesOwned || 0) ? a : b)];
    }
  }

  // 独立个体 → 全部返回
  return pages;
}

// ── 解析单条 filing：index → 主文档 → 提取字段 ───────────────────────────────

/**
 * 解析单条 filing，返回一个或多个记录（数组）
 * - Form 4：始终单条
 * - SC 13G/D：若是独立个体 GROUP，返回多条（每人一条）；机构母子返回一条
 * 第一条复用原始对象 f；后续条目是 f 的浅拷贝，id 改为 accession#N
 */
async function enrichFiling(f) {
  if (!f.edgarUrl) return [f];

  const { status: is, body: iHtml } = await get(f.edgarUrl);
  await sleep(DELAY_MS);
  if (is !== 200) return [f];

  const docUrl = findPrimaryDoc(iHtml);
  if (!docUrl) return [f];

  const { status: ds, body: dBody } = await get(docUrl);
  await sleep(DELAY_MS);
  if (ds !== 200) return [f];

  // Form 4：单申报人，直接赋值后返回
  if (f.formType === '4' || f.formType === '4/A') {
    const p = parseForm4(dBody);
    if (p.filerName)                    f.filerName    = p.filerName;
    if (p.eventDate   != null)          f.eventDate    = p.eventDate;
    if (p.insiderTitle)                 f.insiderTitle = p.insiderTitle;
    if (p.txCode)                       f.txCode       = p.txCode;
    if (p.txShares    != null)          f.txShares     = p.txShares;
    if (p.txPrice     != null)          f.txPrice      = p.txPrice;
    if (p.sharesOwned != null)          f.sharesOwned  = p.sharesOwned;
    return [f];
  }

  // SC 13G/D：解析所有封面页，再判断是否拆分
  const isXml = docUrl.toLowerCase().endsWith('.xml');
  const { pages, eventDate } = isXml ? parseXmlDoc(dBody) : parseDoc(toText(dBody));

  if (eventDate) f.eventDate = eventDate;
  if (!pages.length) return [f];

  const effectivePages = groupOrSplit(pages);

  return effectivePages.map((page, idx) => {
    // 第一条：直接修改原始对象 f
    // 后续条：浅拷贝 + 修改 id（accession#1, #2…）
    const rec = idx === 0 ? f : { ...f, id: `${f.accession}#${idx}` };
    if (page.filerName)           rec.filerName  = page.filerName;
    if (page.sharesOwned != null) rec.sharesOwned = page.sharesOwned;
    if (page.pctOwned    != null) rec.pctOwned    = page.pctOwned;
    return rec;
  });
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
    // Form 4 自身已含单笔交易净变动（txShares），不参与跨 filing 的持仓变动计算
    if (f.formType === '4' || f.formType === '4/A') {
      f.sharesDelta = null;
      f.pctDelta    = null;
      continue;
    }

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
  for (const type of ['SC+13', 'SCHEDULE+13', '4']) {
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
      id:           e.accession,
      ticker:       company.ticker,
      company:      company.name,
      formType:     normalizeFormType(e.formType),
      filedDate:    e.filedDate,
      eventDate:    null,
      filerName:    null,
      filerCik,
      insiderTitle: null,
      txCode:       null,
      txShares:     null,
      txPrice:      null,
      sharesOwned:  null,
      pctOwned:     null,
      sharesDelta:  null,
      pctDelta:     null,
      edgarUrl:     e.indexUrl,
      accession:    e.accession,
    };

    console.log(`  NEW ${e.formType} ${e.filedDate}  ${e.accession}`);
    try {
      const records = await enrichFiling(f);
      for (const rec of records) {
        console.log(`    申报人: ${rec.filerName || '?'}  持股: ${rec.sharesOwned ?? '?'}  占比: ${rec.pctOwned ?? '?'}%`);
        newFilings.push(rec);
      }
    } catch (e2) {
      console.log(`    解析失败: ${e2.message}`);
      newFilings.push(f);
    }
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

  // existingIds：完整 id（含 #N 后缀）用于去重子记录
  // existingBaseIds：基础 accession（去掉 #N）用于跳过已处理的 filing
  const existingIds     = new Set(db.filings.map(f => f.id).filter(Boolean));
  const existingBaseIds = new Set(db.filings.map(f => (f.id || '').split('#')[0]).filter(Boolean));
  console.log(`已有记录: ${db.filings.length} 条 | 追踪公司: ${companies.length} 家\n`);

  // ── 补充历史数据（首次运行，或有旧记录缺少持股字段）─────────────────────
  const toBackfill = db.filings.filter(f => f.sharesOwned == null && f.pctOwned == null);
  if (toBackfill.length > 0) {
    console.log(`补充历史持股数据: ${toBackfill.length} 条记录...\n`);
    let i = 0;
    const backfillExtras = []; // GROUP filing 拆分产生的额外新记录
    for (const f of toBackfill) {
      i++;
      process.stdout.write(`  [${i}/${toBackfill.length}] ${f.ticker} ${f.filedDate}  `);
      try {
        const records = await enrichFiling(f);
        // records[0] === f（已原地修改），records[1..] 是拆分的新子记录
        for (let j = 1; j < records.length; j++) {
          if (!existingIds.has(records[j].id)) backfillExtras.push(records[j]);
        }
        console.log(`shares=${f.sharesOwned ?? '?'}  pct=${f.pctOwned ?? '?'}%`);
      } catch (e) {
        console.log(`错误: ${e.message}`);
      }
    }
    if (backfillExtras.length) {
      db.filings.push(...backfillExtras);
      console.log(`  + ${backfillExtras.length} 条 GROUP 拆分新记录`);
    }
    console.log('\n历史数据补充完成\n');
  }

  // ── 拉取各公司的新 filing ─────────────────────────────────────────────────
  const allNew = [];
  for (let i = 0; i < companies.length; i++) {
    const co = companies[i];
    process.stdout.write(`[${i + 1}/${companies.length}] ${co.ticker} — `);
    try {
      const fresh = await fetchCompany(co, existingBaseIds);
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
