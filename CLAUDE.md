# filingtracker

港美股资金追踪网站，托管于 GitHub Pages（仓库：royyiyangliu/filingtracker）。
前端为单页 `index.html`，数据由 GitHub Actions 爬虫定期写入 `data/` 目录。

## 工作约定

- **开始前先 `git pull`**：爬虫会定期自动更新数据，pull 确保本地与 GitHub 同步后再开始编辑。
- **结束后 `git push`**：前端改动推送到 GitHub，Pages 自动部署。
