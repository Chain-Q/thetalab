# thetalab

**Θ** *theta* — 期权卖方每天收取的时间价值；*lab* — 把它放进实验室里观察。

境内 ETF 期权的日频模拟交易工作台。围绕一条保守的原则构建：**宁可低估收益，不可高估** —— 价差模型、成交量上限、流动性闸门全部保守化，模拟盘与回测共用同一撮合内核，让模拟收益可以被回测证伪。

<p align="center">
  <img src="docs/img/trade_light.png" alt="交易视图 — T 型报价与四方向下单面板" width="880">
</p>

---

## 它解决什么问题

个人期权交易者面对三个现实困难，thetalab 给出对应的设计答案：

| 现实困难 | thetalab 的答案 |
|---|---|
| 期权回测的复杂度在**合约生命周期**（加挂/摘牌/到期/调整），通用框架不适配 | 自研引擎：合约级生命周期管理，日频 T+1 纪律 |
| 免费数据的**口径陷阱**（IV 参数、Greeks 单位、成交量单位、沪深行权价段位数差异） | 每个口径决策都有实测依据（交付文档 §5），八道数据闸门 |
| 回测好看 ≠ 实盘能赚 | 参数平原体检（36 组网格，盈利占比 ≥70% 才上线）、逐腿 Greeks 归因、恒等式闭合审计 |

**明确不做**：不接券商、不下实盘单。唯一下单出口是模拟撮合，且撮合与回测共用同一 Broker 内核。

## 五视图工作台

- **交易** — T 型报价（红涨绿跌、精简/完整两档、到期月切换）、四方向下单面板（试算权利金/保证金/可开手数）、挂单 → 确认 → 推进撮合的完整闭环、持仓平仓与一键全撤
- **概览** — 净值曲线、信号 TOP5（带证据链）、策略推荐（12 模板打分）、持仓体检、市场指标
- **风险** — 盈亏结构图、压力矩阵、保证金占用与资金使用率警示
- **研究** — 希腊字母逐日累计归因、参数平原热力图、ATM IV 一年走势、POP/压力测试
- **学院** — 期权基础 30 秒入门、四大希腊字母仪表盘、12 策略图鉴、交互式盈亏实验台

<p align="center">
  <img src="docs/img/research_dark.png" alt="研究视图 — 归因分解与参数平原" width="880">
</p>

## 自动化数据流

交易日收盘后**无需任何手动操作**（服务器常开即自动）：

```
19:00  晚间调度窗口开启，每 10 分钟轻探测交易所风险指标是否发布
  ↓    ↓ 发布（实测 19:30~21:00+）↓
       collect_daily（风险指标 / 标的日线 / 深市快照 / OI / ATM IV）
  ↓    热更新 → 模拟时钟自动跳最新交易日（有未撮合挂单则不跳并提示先推进）
盘中    可选实时行情：新浪批量快照约 5s/轮（时钟条显示实际条数/耗时），页面双时间基准
        「盘中实时 HH:MM（价格=实时快照）｜ 数据基准 08-31（IV/Greeks/撮合）」
```

## 五品种

| 品种 | 代码 | 口径 |
|---|---|---|
| 沪深300ETF | 510300 | 沪市官方风险指标 + 逐合约日线 |
| 上证50ETF | 510050 | 同上 |
| 中证500ETF | 510500 | 同上 |
| 科创50ETF | 588000 | 同上 |
| 创业板ETF | 159915 | 深市快照口径（前结算价反推 IV，可下单） |

<p align="center">
  <img src="docs/img/academy_dark.png" alt="学院视图 — 策略图鉴与盈亏实验台" width="880">
</p>

## 快速开始

```bash
# 1. 安装依赖（Python 3.12+）
pip install -r requirements.txt

# 2. 采集历史数据（首次约 15 分钟：五品种 240 日风险指标 + 逐合约日线）
python -m thetalab.scripts.collect_risk_history 240
python -m thetalab.scripts.collect_contract_history all

# 3. 启动工作台（浏览器自动打开 http://127.0.0.1:8300）
python -m thetalab.server
```

Windows 亦可双击「启动工作台.bat」/「每日更新.bat」；或直接打开 `thetalab_data/dashboard.html` 静态浏览（只读）。

## 常用命令

| 命令 | 说明 |
|---|---|
| `python -m thetalab.server` | 启动交易工作台（含页面 + API + 晚间调度） |
| `python -m thetalab.scripts.collect_daily` | 采集当日数据（交易日收盘后手动兜底） |
| `python -m thetalab.scripts.collect_contract_history all` | 补采五品种逐合约历史日线 |
| `python -m thetalab.scripts.run_strangle_backtest` | 卖出宽跨式回测（约 1 年） |
| `python -m thetalab.scripts.sensitivity_analysis` | 参数平原 + 成本压力 + 未来函数审计 |

## 运行测试

```bash
# 11 套共 97 项：定价内核 / 撮合 / 组合 / 回测器 / 策略 DSL / 信号 / 模拟盘 / API / 集成 / 数据源解析 / 采集落库
python -m thetalab.tests.test_core
python -m thetalab.tests.test_integration
# …其余各套位于 thetalab/tests/，逐模块直跑
```

## 回测参考（宽跨式 Δ20/20，239 个交易日）

+2.23% 收益 · 夏普 1.01（rf=0）· 最大回撤 1.25% · 参数平原 36 组中 94% 盈利
归因：逐腿 Greeks 分解，逐日残差中位 15.9%（金额口径 37 元/日）
*引用回测数字必须注明胜率三口径：平仓笔 84.6% / 日 53.6% / 月 81.8%*

## 目录结构

```
thetalab/
├── core/       models · pricing(BS/IV求解) · spec(合约规则/保证金/涨跌停) · indicators
├── data/       provider(akshare封装) · validator(八道闸门) · persist(SQLite) · ParquetStore
├── engine/     broker(撮合) · portfolio · runner(回测) · paper(模拟盘) · metrics
├── strategy/   spec(DSL) · templates(12个) · advisor(打分) · signals
├── scripts/    collect_daily · collect_contract_history · build_dashboard · sensitivity_analysis
├── server.py   标准库 ThreadingHTTPServer（API + 晚间调度 + 实时行情，零 Web 框架）
├── tests/      11 套 97 项测试
└── docs/img    README 截图
```

## 设计原则

1. **纯模拟**：不接券商、不下实盘单；唯一下单出口是模拟撮合
2. **宁可低估收益**：价差模型用真实盘口校准（tick/价格主导），单笔 ≤ 当日成交量 2%
3. **T+1 纪律**：当日决策次日成交，杜绝"用收盘价给自己成交"的前视偏差
4. **数据诚实**：缺失就是缺失（NaN），不造 0 兜底；结算价以收盘价近似并在界面标注口径

## 已知局限

- 结算价以收盘价近似（交易所结算价无免费接口），保证金/涨跌停有约 1% 偏差
- 深市（159915）无逐合约历史日线，撮合走每日快照（前结算价），积累中
- 到期行权按现金结算简化（实物交割的份额交收/T+1 未建模）
- 模拟收益系统性偏高（无盘口深度、未建模保证金临时上调）；仅作策略间相对比较
- 上交所风险指标约 19:30~21:00 发布，当日页面需等待晚间调度采集完成
- 新浪逐合约日线发布滞后实测可达 4 个交易日：晚间调度按日级覆盖对账自动补缺口（每合约一次请求 + 失败退避重试）；
  未落库的当日由 OI 快照兜底盯市（价格=快照价，非交易所结算价），官方日线到库后自动优先
- 实时行情仅覆盖沪市品种（新浪期权快照无深市），159915 盘中只有快照口径

## 免责声明

本项目为个人研究与教学工具，**不构成任何投资建议**。期权交易风险极高，卖方策略可能产生无上限亏损。实盘交易需满足交易所适当性要求（三级权限 + 50 万验资等）。数据来源的授权与合规由使用者自行负责。

## License

MIT — 见 [LICENSE](LICENSE)
