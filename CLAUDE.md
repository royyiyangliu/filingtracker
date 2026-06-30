# filingtracker

港美股资金追踪网站，托管于 GitHub Pages（仓库 `royyiyangliu/filingtracker`）。
前端是单个静态 `index.html`，数据由 GitHub Actions 爬虫定期抓取后写入 `data/` 并自动提交，Pages 自动部署。**无后端、无构建步骤**，前端只 `fetch` 仓库里的 JSON/CSV。

## 工作约定
- **开始前先 `git pull`**：爬虫会定期自动提交数据，先同步避免冲突。
- **结束后按需 `git push`**：推送即触发 Pages 部署。push 代码**不会**触发任何爬虫（工作流仅 schedule / 手动 dispatch 触发）。
- 改 `index.html` 后用本地静态服务器 + 浏览器实测再推（数据 fetch 需 http 环境）。

## 五个子系统（本项目由多个独立项目整合，每个有各自的爬虫/数据/工作流/前端 tab）

| 子系统 | 爬虫脚本 | 数据来源 | 输出 | 工作流（UTC 调度） | 前端 tab |
|---|---|---|---|---|---|
| SEC 股东持仓 | `crawler.js` (Node) | SEC EDGAR | `data/filings.json` | `crawl.yml`（周一 06:00） | 股东持仓(SEC) |
| 港股回购 | `hk-buyback.js` (Node) | 港交所 SRRPT xls | `data/hk-buybacks.json` | `crawl-hk-buyback.yml`（周一 08:00，跑完发周报邮件 `send-email.js`） | 回购 |
| 港股股东披露 | `scripts/hk_di_crawler.py` | 港交所 di.hkex.com.hk | `data/hk-di.json` | `crawl-hk-di.yml`（周六 10:30） | 股东持仓(港股) |
| AI 产业链 | `scripts/aichain_daily_update.py` | Yahoo Finance (yfinance) | `data/aichain/*` | `aichain-daily.yml`（每日 17:00） | AI 产业链 |
| 港股通南向 | `scripts/daily.py`→`backfill.py`+`build_site.py`+`fetch_industry.py` | AKShare / 东方财富 | `data/hkdata/*` | `hk-daily.yml`（周一至五 10:00）+ 手动 `hk-backfill.yml` | 南向资金流 / 南向个股持仓 |

前端共 **6 个 tab**（南向拆成「资金流」+「个股持仓」两个）。

## 数据目录
- 根 JSON：`data/filings.json`、`data/hk-buybacks.json`、`data/hk-di.json`、`data/hk-buyback-weekly.json`
- `data/aichain/`：`summary.json`、`history/<TICKER>.json`、`chart_categories.json`、`last_updated.json`
- `data/hkdata/`：CSV 源（`flow/*.csv`、`holdings/*.csv`、`industry.csv`）由爬虫写入；`build_site.py` 据此生成前端用的 `flow.json`、`ranking.json`、`stocks/<code>.json`

## 配置 / 标的列表
- `companies.json`（根）：**SEC 爬虫**用，17 只中概股（ticker + CIK）。
- `config/companies.json`：**港股回购 + 港股股东披露共用**。回购按 `numCode` 匹配；股东披露仅抓 `sid` 非空的条目，用 `sid`/`corpn` 拼港交所查询 URL。新增港股公司在此加一条；`sid`/`corpn` 需去 di.hkex.com.hk 按股票代码检索获取（sid 是检索主键，corpn 仅显示）。
- **港股通南向无标的列表**：AKShare 接口返回全市场南向持股，新纳入个股自动收录。

## 前端机制（index.html）
- 单页 SPA，CSS 显隐切换 tab；`switchTab()` 首次进入某 tab 才懒加载数据（`fetch(...?v=Date.now())` 绕缓存）。
- 表格用 **Tabulator v6**；图表用 **D3 v7**（`assets/d3.min.js` 本地内置）。
- 港股/南向相关样式作用域在 `.hk-panel` 下（`.card`/`.hk-btn`/`.chart-box`/`.controls` 等）；`.d3-tip`/`.sel-chips` 为全局。
- 回购 tab 顶部有「每周回购总额堆叠柱状图」：数据由 `build-buyback-weekly.js` 预先把各币种回购额按交易日汇率折成人民币、按 ISO 周/公司汇总到 `data/hk-buyback-weekly.json`（该脚本作为 `crawl-hk-buyback.yml` 的一步运行，全量重算、幂等）。

## 易踩坑
- 回购数据 `aggregateHKD` 字段名有误导，实为**原币种金额**；币种看 `currency`（缺失时由 `method` 推断：Exchange→HKD，NYSE/Nasdaq→USD）。
- 汇率折算用 frankfurter.app（ECB 参考汇率，免费无 key，支持历史日频），缺口前向填充；CNY 为在岸。
- 各工作流 push 前多会 `git pull --rebase`，错峰调度以避免互相冲突。
