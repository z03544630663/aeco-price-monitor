# AECO Price Monitor

天然气价格监测工具 - AECO 日度价格抓取与展示

## 功能

- 每日自动抓取 AECO-C 天然气现货价格
- 双源校验（主源 + 备源）
- 价格异常告警
- Web 可视化展示
- 自动部署到 Vercel

## 部署

### GitHub Actions 自动部署

本仓库配置了 GitHub Actions，每天 08:00（北京时间）自动：
1. 抓取最新价格数据
2. 导出为静态 JSON 文件
3. 推送变更到仓库
4. Vercel 自动部署更新

### 手动触发

在 GitHub Actions 页面点击 "Run workflow" 即可手动触发。

## 数据源

- **主源**: Gas Alberta Public (https://www.gasalberta.com)
- **备源**: Mock Fallback (当主源不可用时)

## 在线访问

https://aeco-price-monitor.vercel.app

## 本地开发

```bash
# 安装依赖
pip install requests

# 启动本地服务
python server.py

# 访问 http://127.0.0.1:8000
```

## 项目结构

```
├── .github/workflows/
│   └── daily-fetch.yml    # GitHub Actions 工作流
├── scripts/
│   └── fetch_prices.py    # 价格抓取脚本
├── vercel-deploy/
│   ├── index.html         # 前端入口
│   ├── static/            # 静态资源
│   ├── api/               # API 端点（静态 JSON）
│   └── data.json          # 完整数据
├── server.py              # 本地开发服务器
└── README.md
```

## License

MIT
