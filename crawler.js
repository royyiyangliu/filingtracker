'use strict';

/**
 * SEC Filing Crawler
 * 追踪 17 家中概股的 Schedule 13D/G（及 Form 4）披露变化
 * 数据来源：SEC EDGAR Full-Text Search API (EFTS)
 */

const https = require('https');
const fs    = require('fs');
const path  = require('path');

// ─── 配置 ──────────────────────────────────────────────────────────────────────
const COMPANIES_FILE  = path.join(__dirname, 'companies.json');
const DATA_FILE       = path.join(__dirname, 'data', 'filings.json');
const USER_AGENT      = 'SEC-Filing-Tracker/1.0 research contact@researchuse.com';
const DELAY_MS        = 350;          // 每次请求间隔（EDGAR 限速 10 req/s，保守用 350ms）
const PAGE_SIZE       = 20;           // EFTS 单页结果数
const MAX_PAGES       = 30;           // 单公司最大翻页数（600 条上限）
const FORM_FILTER     = 'SC+13D%2CSC+13D%2FA%2CSC+13G%2CSC+13G%2FA%2C4%2C4%2FA';
const VALID_FORMS     = new Set(['SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A', '4', '4/A']);

// ─── HTTP ──────────────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function httpsGet(url, retries = 3) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': USER_AGENT, 'Accept': '*/*', 'Accept-Encoding': 'identity' },
      timeout: 30000
    }, res => {
      if ([301, 302, 307, 308].includes(res.statusCode)) {
        res.resume();
        const loc = res.headers.location;
        if (!loc) return reject(new Error('Redirect without location'));
        return httpsGet(loc.startsWith('http') ? loc : 'https://www.sec.gov' + loc, retries)
          .then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', async () => {
        const body = Buffer.concat(chunks).toString('utf8');
        // 对 EDGAR 偶发 500 自动重试
        if (res.statusCode === 500 && retries > 0) {
          await sleep(3000);
          return httpsGet(url, retries - 1).then(resolve).catch(reject);
        }
        resolve({ status: res.statusCode, body });
      });
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', async e => {
      if (retries > 0) { await sleep(2000); return httpsGet(url, retries - 1).then(resolve).catch(reject); }
      reject(e);
    });
  });
}

// ─── EDGAR 工具 ────────────────────────────────────────────────────────────────

/** 标准化表格类型字符串 */
function normalizeForm(raw) {
  const t = (raw || '').toUpperCase().trim();
  return VALID_FORMS.has(t) ? t : null;
}

/**
 * 从 EFTS display_names 中提取"申报人"信息
 * display_names 格式：["COMPANY NAME  (TICKER)  (CIK 0001234567)", ...]
 * 申报人 = 非目标公司的那一方
 */
function extractFiler(displayNames, targetCikFull, targetCikNoZero) {
  for (const entry of displayNames) {
    // 跳过目标公司
    if (entry.includes(targetCikFull) || entry.includes('(' + targetCikNoZero + ')')) continue;
    // 提取姓名（括号前部分）
    const m = entry.match(/^(.*?)\s*(?:\([A-Z][^)]*\)\s*)?\(CIK/);
    const c = entry.match(/CIK\s+(\d+)/);
    return {
      name: m ? m[1].trim() : entry.split('(')[0].trim(),
      cik:  c ? c[1].padStart(10, '0') : ''
    };
  }
  return { name: '—', cik: '' };
}

/**
 * 根据 accession number 构建 EDGAR 申报索引页链接
 * 规则：acc 前 10 位（去前导零后）作为存储目录的 CIK
 */
function buildEdgarUrl(adsh) {
  if (!adsh || adsh.length < 18) return '';
  const nodash  = adsh.replace(/-/g, '');
  const cikNum  = parseInt(adsh.slice(0, 10), 10);
  return `https://www.sec.gov/Archives/edgar/data/${cikNum}/${nodash}/${adsh}-index.htm`;
}

// ─── 核心搜索 ──────────────────────────────────────────────────────────────────

/**
 * 用 EFTS 搜索某公司的相关申报，返回新记录列表
 * @param {object} company       公司对象 { ticker, cik, name, searchName }
 * @param {Set}    existingIds   已入库的 accession number set（本函数会更新它）
 */
async function fetchCompanyFilings(company, existingIds) {
  const { ticker, cik: cikFull, name, searchName } = company;
  const cikNoZero = cikFull.replace(/^0+/, '');
  const query     = encodeURIComponent('"' + searchName + '"');
  const newRows   = [];

  let from  = 0;
  let total = Infinity;
  let page  = 0;

  while (from < total && page < MAX_PAGES) {
    const url = `https://efts.sec.gov/LATEST/search-index?q=${query}` +
                `&forms=${FORM_FILTER}&from=${from}&size=${PAGE_SIZE}`;
    await sleep(DELAY_MS);

    let res;
    try { res = await httpsGet(url); }
    catch (e) { console.warn(`  [网络错误] ${e.message}`); break; }

    if (res.status !== 200) { console.warn(`  [HTTP ${res.status}]`); break; }

    let data;
    try { data = JSON.parse(res.body); }
    catch (e) { console.warn(`  [JSON 解析失败]`); break; }

    const hits = data.hits?.hits || [];
    if (total === Infinity) {
      total = data.hits?.total?.value ?? hits.length;
      console.log(`  ${ticker}: EFTS 总命中 ${total} 条`);
    }
    if (hits.length === 0) break;

    let newThisPage = 0;

    for (const hit of hits) {
      const src   = hit._source || {};
      const adsh  = src.adsh || (hit._id || '').split(':')[0];
      const form  = normalizeForm(src.form || src.file_type || '');
      const filed = src.file_date || '';

      if (!adsh || !filed || !form) continue;

      // 过滤：确认目标公司 CIK 出现在此申报中
      const ciks  = src.ciks       || [];
      const names = src.display_names || [];
      const isOur = ciks.some(c => c === cikFull || c === cikNoZero) ||
                    names.some(n => n.includes(cikFull) || n.includes('(' + cikNoZero + ')'));
      if (!isOur) continue;

      // 去重
      if (existingIds.has(adsh)) continue;
      existingIds.add(adsh);
      newThisPage++;

      const filer = extractFiler(names, cikFull, cikNoZero);

      newRows.push({
        id:         adsh,
        ticker,
        company:    name,
        formType:   form,
        filedDate:  filed,
        periodDate: src.period_ending || '',
        filerName:  filer.name,
        filerCik:   filer.cik,
        edgarUrl:   buildEdgarUrl(adsh),
        accession:  adsh
      });
    }

    from += hits.length;
    page++;

    // 优化：如果翻过 3 页且全是已有数据，停止（数据一般按相关性排序，旧记录在后）
    if (newThisPage === 0 && page >= 3) break;
  }

  return newRows;
}

// ─── 主程序 ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('╔══════════════════════════════════════╗');
  console.log('║   SEC Filing Crawler  中概股追踪     ║');
  console.log('╚══════════════════════════════════════╝\n');

  const companies = JSON.parse(fs.readFileSync(COMPANIES_FILE, 'utf8'));

  // 读取现有数据
  let existing = { lastUpdated: null, totalCount: 0, filings: [] };
  if (fs.existsSync(DATA_FILE)) {
    try { existing = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
    catch (_) { console.warn('[警告] 现有数据文件损坏，从空白开始\n'); }
  }

  const isFirstRun = !existing.lastUpdated;
  console.log(isFirstRun
    ? '模式: 首次运行 — 抓取全部历史数据\n'
    : `模式: 增量更新 — 上次运行: ${existing.lastUpdated.slice(0, 10)}\n`
  );
  console.log(`已有记录: ${existing.filings.length} 条 | 追踪公司: ${companies.length} 家\n`);

  // 建立 accession 去重集合
  const existingIds = new Set(existing.filings.map(f => f.id).filter(Boolean));

  // 逐公司抓取
  const allNew = [];
  for (let i = 0; i < companies.length; i++) {
    const co = companies[i];
    process.stdout.write(`[${i + 1}/${companies.length}] ${co.ticker} — `);
    try {
      const rows = await fetchCompanyFilings(co, existingIds);
      allNew.push(...rows);
      console.log(`+${rows.length} 条新记录`);
    } catch (e) {
      console.log(`错误: ${e.message}`);
    }
  }

  // 合并 + 按日期降序排列
  const merged = [...existing.filings, ...allNew];
  merged.sort((a, b) => (b.filedDate || '').localeCompare(a.filedDate || ''));

  // 写入文件
  const output = {
    lastUpdated: new Date().toISOString(),
    totalCount:  merged.length,
    filings:     merged
  };
  const dir = path.dirname(DATA_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2), 'utf8');

  console.log(`\n${'─'.repeat(40)}`);
  console.log(`本次新增: ${allNew.length} 条`);
  console.log(`数据库合计: ${merged.length} 条`);
  console.log(`已保存至: data/filings.json`);
}

main().catch(err => { console.error('\n[Fatal]', err.message); process.exit(1); });
