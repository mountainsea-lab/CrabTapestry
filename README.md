<p align="center"> <img src="https://img.picgo.net/2024/05/27/2024-05-27-10.29.059a0defe3962c9f79.png" width="200" alt="CrabTapestry logo"> </p> <h1 align="center">CrabTapestry</h1> <p align="center"><b>Weaving Data into Action</b></p> <p align="center"> <a href="https://www.rust-lang.org/"> <img src="https://img.shields.io/badge/Made%20with-Rust-dea584?style=for-the-badge&logo=rust" alt="Made with Rust"> </a> <a href="LICENSE"> <img src="https://img.shields.io/badge/License-MIT%2FApache--2.0-blue?style=for-the-badge" alt="License"> </a> <a href="https://github.com/mountainsea-lab/crabtapestry/actions"> <img src="https://img.shields.io/github/actions/workflow/status/mountainsea-lab/crabtapestry/ci.yml?style=for-the-badge&logo=github" alt="CI Status"> </a> </p> <p align="center"> <a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a> </p>
=======================================================================================

# ✨ Project Description
> CrabTapestry is a high-performance, real-time data-driven platform built with Rust. Its name creatively combines Rust's mascot, the crab (Ferris), with "Tapestry," symbolizing how it weaves complex data streams into valuable insights, much like an artisan crab crafting a tapestry.
> At its core, CrabTapestry acts as a powerful "loom" in the modern data landscape, seamlessly aggregating multi-source data and transforming raw information into precise decisions and actions through an efficient event-driven engine.



# 🚀 Core Features
* 🌐 Multi-Source Data Ingestion: Unified interface for on-chain (e.g., Sui, Dune), off-chain (e.g., Binance, OKX), and social media data (Twitter, news).

* ⚡ Real-Time Processing Engine: Built with Rust, delivering unparalleled performance, memory safety, and reliability.

* 🎯Event-Driven Architecture: Supports defining complex rules (e.g., "price surge + social media buzz") via YAML or code to generate automated triggers.

* 📊 Powerful Visualization: Provides modern web dashboards and terminal (TUI) interfaces for real-time market monitoring and system status.
* 🔧 High Extensibility: Modular design allows easy integration of custom data sources, processing logic, and downstream actions (e.g., trading APIs, notifications). 

------------------------------------------------------------------
# 📦 Quick Start

## Prerequisites
 * Rust Toolchain: Install the latest Rust programming environment (stable version is sufficient).

 * Other Dependencies: Libraries such as openssl. Install based on your operating system:

## Ubuntu/Debian:

```bash
sudo apt update && sudo apt install libssl-dev pkg-config
```
## Installation & Running
1. Clone the project:

```bash
git clone https://github.com/mountainsea-lab/crabtapestry.git
cd crabtapestry
```

2. Build the project:
```bash
cargo build --release
```

3. Configuration:
```bash
# Todo: Add configuration setup instructions here
```

4. Run:
```bash
cp .env.dev .env   # 开发环境
./deploy/start-all.sh

cp .env.prod .env  # 生产环境
START_INFRA=no ./deploy/start-all.sh
```

---------------------------------------------------------------------------
# 🏗️ Project Architecture
```text
crabtapestry/
# Todo: Add project structure details here
```
---------------------------------------------------------------------------
# 📖 Documentation
> Detailed usage guides, API references, and configuration explanations can be found in our Project Documentation (to be completed).

---------------------------------------------------------------------------
# 🤝 Contributing
> We love contributions! Welcome to submit issues, feature requests, or directly open pull requests.
Please read our Contributing Guidelines before contributing.

1. Fork the repository

2.Create your feature branch (git checkout -b feature/AmazingFeature)

3. Commit your changes (git commit -m 'Add some AmazingFeature')

4. Push to the branch (git push origin feature/AmazingFeature)

5. Open a pull request

---------------------------------------------------------------------------
# 📜 License
### This project is dual-licensed under:

* MIT License: See the LICENSE-MIT file for details.

---------------------------------------------------------------------------
# ☎️ Contact
* Project Homepage: https://github.com/mountainsea-lab/crabtapestry

* Issue Tracker: GitHub Issues
* 
* Discussion Forum: GitHub Discussions