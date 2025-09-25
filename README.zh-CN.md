<p align="center">
<img src="https://img.picgo.net/2024/05/27/2024-05-27-10.29.059a0defe3962c9f79.png" width="200" alt="CrabTapestry logo">
</p>
<h1 align="center">CrabTapestry</h1> 
<p align="center"><b>编织数据，驱动决策 | Weaving Data into Action</b></p><p align="center">
<a href="https://www.rust-lang.org/"> 
<img src="https://img.shields.io/badge/Made%20with-Rust-dea584?style=for-the-badge&logo=rust" alt="Made with Rust"> </a> 
<a href="LICENSE"> <img src="https://img.shields.io/badge/License-MIT%2FApache--2.0-blue?style=for-the-badge" alt="License"> </a> 
<a href="https://github.com/mountainsea-lab/crabtapestry/actions"> 
<img src="https://img.shields.io/github/actions/workflow/status/mountainsea-lab/crabtapestry/ci.yml?style=for-the-badge&logo=github" alt="CI Status"> 
</a> </p><p align="center"> <a href="README.md">English</a> |
<a href="README.zh-CN.md">简体中文</a> 
</p>
=======================================================================================
# ✨项目简介
> CrabTapestry 是一个基于 Rust 语言构建的高性能、实时数据驱动平台。它的命名创意来源于 Rust 的吉祥物螃蟹 (Ferris) 和“织锦” (Tapestry) 的融合，寓意着像螃蟹工匠一样，将纷繁复杂的数据流编织成有价值的智慧锦缎。
平台的核心是充当现代数据领域的强大“织机”，无缝聚合多源数据，并通过高效的事件驱动引擎，将原始数据转化为精准的决策与行动。
# 🚀 核心特性
 *  🌐 多源数据摄取: 统一接口接入链上（如 Sui, Dune）、链下（如 Binance, OKX）及社交媒体（Twitter, 新闻）数据。
 *  ⚡ 实时处理引擎: 基于 Rust 构建，提供无与伦比的性能、内存安全性与可靠性。
 *  🎯 事件驱动架构: 支持通过 YAML 或代码定义复杂规则（如“价格飙升 + 社交热议”），生成自动化触发信号。
 *  📊 强大的可视化: 提供现代化的 Web 仪表盘和终端文本 (TUI) 界面，实时监控市场动态与系统状态。
 *  🔧 高度可扩展: 模块化设计，轻松接入自定义数据源、处理逻辑及下游执行动作（如交易API、消息通知）。
-------------------------------------------------------------------------------------
# 📦 快速开始
## 前置条件
* Rust Toolchain: 安装最新的 Rust 编程环境（stable 版本即可）。
* 其他依赖: 例如 openssl 等库。请根据您的操作系统安装：
* Ubuntu/Debian:
  ```bash
  sudo apt update && sudo apt install libssl-dev pkg-config

## 安装与运行
 1. 克隆项目:
 ```bash
 git clone https://github.com/mountainsea-lab/crabtapestry.git
 cd crabtapestry
 ```
 2. 编译项目
  ```bash
cargo build --release
  ```
 3. 配置
  ```bash
  todo
 ```
 4. 运行
 ```bash
# 运行核心引擎
cargo run --release --bin crabtapestry-core

# 或者运行 Web 仪表板 (通常在不同终端)
cargo run --release --bin crabtapestry-dashboard
  
 ```
-------------------------------------------------------------------------------------
# 🏗️ 项目架构
```text
crabtapestry/
 todo
```
-------------------------------------------------------------------------------------
# 📖 使用文档
> 详细的使用指南、API 说明和配置项解释，请查阅我们的 项目文档 (待完善)。
-------------------------------------------------------------------------------------
# 🤝 如何贡献
> 我们热爱贡献！欢迎提交 Issue、提出功能请求或直接发起 Pull Request。
> 在贡献之前，请阅读我们的 贡献指南。
 1. Fork 本仓库
 2. 创建您的特性分支 (git checkout -b feature/AmazingFeature)
 3. 提交您的更改 (git commit -m 'Add some AmazingFeature')
 4. 推送到分支 (git push origin feature/AmazingFeature)
 5. 打开一个 Pull Request
-------------------------------------------------------------------------------------

# 📜 开源协议
本项目采用双重许可：
* MIT License: 查看 LICENSE-MIT 文件 了解更多细节。
-------------------------------------------------------------------------------------
# ☎️ 联系我们
* 项目主页: https://github.com/mountainsea-lab/crabtapestry
* 问题反馈: GitHub Issues
* 讨论区: GitHub Discussions

也就是说 交易所数据,新闻,平台热点信息,链上信息 进行预定义转换为事件。驱动下游策略进行操作(包括消息分发提醒等)
ChatGPT 说：

完全正确，你描述的就是一个 统一事件驱动体系的核心逻辑，可以抽象为如下流程：

1️⃣ 数据源层（Multi-Source Data）
数据类型	说明
交易所数据	实时行情、历史K线、交易明细
新闻	上架/下架、利好/利空、公告
平台热点信息	社交媒体、社区、论坛、热门话题
链上信息	转账、合约交互、账户行为、代币发行等
2️⃣ 事件转换（Predefined Event Transformation）

目标：将多源异构数据统一转化为 事件对象（Event）

步骤：

数据标准化：统一字段、时间戳、来源标签

分类/标签化：类型、主题、重要性

规则匹配：使用 YAML/代码预定义规则识别关键事件

示例事件：

enum Event {
Market { symbol: String, price: f64, volume: f64, timestamp: DateTime<Utc> },
News { title: String, type: NewsType, source: String, timestamp: DateTime<Utc> },
Social { author: String, content: String, sentiment: f64, timestamp: DateTime<Utc> },
OnChain { tx_hash: String, from: String, to: String, amount: f64, token: String, timestamp: DateTime<Utc> },
}

3️⃣ 事件驱动层（Event Engine）

作用：

接收转换后的事件

触发下游消费，包括策略执行和消息分发

机制：

事件总线 / PubSub

支持异步处理与多下游订阅

4️⃣ 下游服务（Downstream Consumption）
消费类型	功能
策略执行	量化交易、风控、跟单、投机策略等
消息分发	用户通知、App/WebSocket推送、邮件、日志记录
数据归档	历史事件存储、RAG知识库、统计分析
可视化/监控	实时行情、策略信号、事件触发信息
5️⃣ 整体数据流示意（文字版）
交易所数据/新闻/社交/链上信息
│
▼
预定义规则转换
(Event Transformation)
│
▼
事件对象 Event
│
▼
事件驱动层 (Event Engine)
│
┌───────┼────────┐
▼       ▼        ▼
策略执行  消息分发  数据归档/RAG


✅ 特点

多源数据统一事件化，形成标准化接口

规则驱动，可灵活扩展策略和通知逻辑

下游模块订阅事件即可进行实时或批量处理

支持历史数据回溯和语义分析

如果你需要，我可以帮你画一个 清晰的全流程架构图，把“交易所/新闻/社交/链上 → 事件转换 → 事件驱动 → 下游策略/消息分发/归档”全部可视化，方便团队理解。