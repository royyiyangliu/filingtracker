'use strict';

/**
 * 中概股周报邮件发送脚本
 * 由 crawl-hk-buyback.yml (schedule) 触发，手动 workflow_dispatch 时跳过
 * 读取港股回购 + SEC 持仓近 8 日数据，发送 HTML 邮件
 */

const fs         = require('fs');
const path       = require('path');
const nodemailer = require('nodemailer');

const HK_FILE  = path.join(__dirname, 'data', 'hk-buybacks.json');
const SEC_FILE = path.join(__dirname, 'data', 'filings.json');

function getRecent(file, dateField, days = 8) {
  if (!fs.existsSync(file)) return [];
  const db    = JSON.parse(fs.readFileSync(file, 'utf8'));
  const items = db.buybacks || db.filings || [];
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cut = cutoff.toISOString().slice(0, 10);
  return items
    .filter(r => (r[dateField] || '') >= cut)
    .sort((a, b) => (b[dateField] || '').localeCompare(a[dateField] || ''));
}

function fmtNum(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-US');
}

function aggregateHKByCompany(rows) {
  const groups = {};
  rows.forEach(r => {
    const key = r.stockCode;
    if (!groups[key]) {
      groups[key] = {
        stockCode: r.stockCode, companyCN: r.companyCN,
        currency: r.currency || 'HKD', method: r.method,
        shares: 0, aggregateHKD: 0, priceHigh: null, priceLow: null,
        mandatePct: null, _lastDate: '',
      };
    }
    const g = groups[key];
    g.shares       += (r.shares || 0);
    g.aggregateHKD += (r.aggregateHKD || 0);
    if (r.priceHigh != null) g.priceHigh = g.priceHigh == null ? r.priceHigh : Math.max(g.priceHigh, r.priceHigh);
    if (r.priceLow  != null) g.priceLow  = g.priceLow  == null ? r.priceLow  : Math.min(g.priceLow,  r.priceLow);
    if (r.tradingDate > g._lastDate) { g._lastDate = r.tradingDate; g.mandatePct = r.mandatePct; }
  });
  return Object.values(groups).sort((a, b) => b.aggregateHKD - a.aggregateHKD);
}

function fmtMoney(n, currency) {
  if (n == null) return '—';
  const sym = currency || 'HKD';
  if (n >= 1e8) return `${sym} ${(n / 1e8).toFixed(3)} 亿`;
  if (n >= 1e6) return `${sym} ${(n / 1e6).toFixed(2)}M`;
  return `${sym} ${Number(n).toLocaleString('en-US')}`;
}

function fmtTxShares(n) {
  if (n == null) return '—';
  if (n === 0)   return '—';
  return n > 0
    ? `<span style="color:#16a34a">▲ +${Number(n).toLocaleString('en-US')}</span>`
    : `<span style="color:#dc2626">▼ ${Number(n).toLocaleString('en-US')}</span>`;
}

function fmtDelta(n) {
  if (n == null) return '—';
  if (n === 0)   return '—';
  return n > 0
    ? `<span style="color:#16a34a">▲ +${Number(n).toLocaleString('en-US')}</span>`
    : `<span style="color:#dc2626">▼ ${Number(n).toLocaleString('en-US')}</span>`;
}

function fmtDeltaPct(n) {
  if (n == null) return '—';
  if (n === 0)   return '—';
  return n > 0
    ? `<span style="color:#16a34a">+${n.toFixed(2)}%</span>`
    : `<span style="color:#dc2626">${n.toFixed(2)}%</span>`;
}

function buildHtml(hkRows, secRows) {
  const today = new Date().toLocaleDateString('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit', timeZone: 'Asia/Shanghai',
  });

  const th = s => `<th style="padding:6px 10px;background:#e8edf8;text-align:left;white-space:nowrap;border:1px solid #d1d9f0">${s}</th>`;
  const td = (s, right) => `<td style="padding:6px 10px;border:1px solid #e2e8f0;${right ? 'text-align:right' : ''}">${s ?? '—'}</td>`;
  const trBg = i => `background:${i % 2 === 0 ? '#fff' : '#f8faff'}`;

  const hkAgg = aggregateHKByCompany(hkRows);
  const hkSection = hkAgg.length === 0
    ? '<p style="color:#64748b;margin:8px 0">本周无回购记录</p>'
    : `<table style="border-collapse:collapse;font-size:13px;width:100%;margin-top:8px">
        <thead><tr>
          ${[th('公司'), th('回购股数'), th('最高价'), th('最低价'), th('总金额'), th('授权比例%'), th('回购市场')].join('')}
        </tr></thead>
        <tbody>
          ${hkAgg.map((r, i) => `<tr style="${trBg(i)}">
            ${td(`<b>${r.companyCN}</b>`)}
            ${td(fmtNum(r.shares), true)}
            ${td(r.priceHigh != null ? `${r.currency} ${r.priceHigh}` : '—', true)}
            ${td(r.priceLow  != null ? `${r.currency} ${r.priceLow}`  : '—', true)}
            ${td(fmtMoney(r.aggregateHKD, r.currency), true)}
            ${td(r.mandatePct != null ? r.mandatePct.toFixed(3) + '%' : '—', true)}
            ${td(r.method || '—')}
          </tr>`).join('')}
        </tbody>
      </table>`;

  const secSection = secRows.length === 0
    ? '<p style="color:#64748b;margin:8px 0">本周无新申报</p>'
    : `<table style="border-collapse:collapse;font-size:13px;width:100%;margin-top:8px">
        <thead><tr>
          ${[th('申报日期'), th('公司'), th('表格'), th('申报人'), th('持股数'), th('占比%'), th('交易股数'), th('成交价'), th('股数变动'), th('%变动')].join('')}
        </tr></thead>
        <tbody>
          ${secRows.map((r, i) => `<tr style="${trBg(i)}">
            ${td(r.filedDate)}
            ${td(`<b>${r.company || r.ticker}</b>`)}
            ${td(r.formType)}
            ${td(r.filerName || '—')}
            ${td(fmtNum(r.sharesOwned), true)}
            ${td(r.pctOwned != null ? r.pctOwned.toFixed(2) + '%' : '—', true)}
            ${td(fmtTxShares(r.txShares), true)}
            ${td(r.txPrice != null ? `$${r.txPrice}` : '—', true)}
            ${td(fmtDelta(r.sharesDelta), true)}
            ${td(fmtDeltaPct(r.pctDelta), true)}
          </tr>`).join('')}
        </tbody>
      </table>`;

  return `
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:960px;margin:0 auto;color:#1a1a2e">
  <div style="background:#1a1a2e;padding:20px 28px;border-radius:8px 8px 0 0">
    <h2 style="margin:0;color:#fff;font-size:18px;font-weight:600">中概股周报 · ${today}</h2>
  </div>
  <div style="padding:24px 28px;background:#fff;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 8px 8px">

    <h3 style="color:#1d4ed8;margin:0 0 8px;font-size:15px">
      港股回购（周度汇总）<span style="font-weight:400;color:#64748b">近 8 日 · ${aggregateHKByCompany(hkRows).length} 家公司</span>
    </h3>
    ${hkSection}

    <h3 style="color:#1d4ed8;margin:32px 0 8px;font-size:15px">
      SEC 持仓披露 <span style="font-weight:400;color:#64748b">近 8 日 · ${secRows.length} 条</span>
    </h3>
    ${secSection}

    <p style="margin-top:32px;padding-top:16px;border-top:1px solid #f1f5f9;color:#94a3b8;font-size:12px">
      完整数据：<a href="https://royyiyangliu.github.io/filingtracker/" style="color:#3b82f6">royyiyangliu.github.io/filingtracker</a>
    </p>
  </div>
</div>`;
}

async function main() {
  const hkRows  = getRecent(HK_FILE,  'tradingDate', 8);
  const secRows = getRecent(SEC_FILE, 'filedDate',   8);

  if (hkRows.length + secRows.length === 0) {
    console.log('近 8 日无新数据，跳过发邮件');
    return;
  }

  const today = new Date().toLocaleDateString('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit', timeZone: 'Asia/Shanghai',
  });
  const subject = `中概股周报 ${today}｜港股回购 ${hkRows.length} 条 · SEC ${secRows.length} 条`;
  const html    = buildHtml(hkRows, secRows);

  const transporter = nodemailer.createTransport({
    host:   'smtp.gmail.com',
    port:   465,
    secure: true,
    auth: {
      user: process.env.GMAIL_USER,
      pass: process.env.GMAIL_APP_PASSWORD,
    },
  });

  await transporter.sendMail({
    from:    `"中概股周报" <${process.env.GMAIL_USER}>`,
    to:      process.env.EMAIL_TO,
    subject: subject,
    html:    html,
  });

  console.log(`邮件已发送：${subject}`);
}

main().catch(e => { console.error('[Fatal]', e.message); process.exit(1); });
