use crate::domain::tradingview_dto::{SymbolInfoUdf, SymbolQuery, SymbolSearchResult, TradingviewConfig};
use crate::global;
use crate::server::routes::handlers::tradingview_handlers::HistoryQuery;
use crab_infras::external::crab_hmds::DefaultHmdsExchange;
use crab_infras::external::crab_hmds::meta::OhlcvRecord;
use crab_types::time_frame::TimeFrame;

/// get tradingview config
pub async fn get_tradingview_config() -> anyhow::Result<TradingviewConfig> {
    let config = TradingviewConfig {
        supports_search: true,
        supports_group_request: false,
        supported_resolutions: default_supported_resolutions(),
        supports_marks: false,
        supports_timescale_marks: false,
        supports_time: true,
    };
    Ok(config)
}
/// get strategy symbols
pub async fn get_tradingview_symbols(query: SymbolQuery) -> anyhow::Result<SymbolInfoUdf> {
    let symbol = query.symbol.to_uppercase();

    // 获取全局配置
    let strategy_config = global::get_strategy_config().get();

    // 查找 symbol 所属的交易所和配置（可选）
    let mut matched_quote = "USDT".to_string();
    let mut supported_resolutions: Vec<String> = vec![];

    for sub in &strategy_config.subscriptions {
        for sym in &sub.symbols {
            if sym.name.eq_ignore_ascii_case(&symbol) {
                // 找到匹配的 symbol
                matched_quote = sym.quote.clone();

                // 周期解析（优先 symbol 自定义，其次 subscription 默认）
                let raw_periods = sym
                    .periods
                    .clone()
                    .or_else(|| sub.default_periods.clone())
                    .unwrap_or_else(|| vec!["1m".into(), "5m".into(), "1h".into(), "1d".into()]);

                supported_resolutions = raw_periods
                    .into_iter()
                    .filter_map(|s| TimeFrame::from_str(&s).ok()) // 转换为枚举
                    .map(|tf| tf.to_str().to_string()) // 再转成统一字符串
                    .collect();

                break;
            }
        }
    }

    // 如果找不到配置则 fallback 为默认支持周期
    if supported_resolutions.is_empty() {
        supported_resolutions = default_supported_resolutions();
    }

    // 构造 SymbolInfoUdf（可用于前端 TradingView）
    let info = SymbolInfoUdf {
        name: symbol.clone(),
        ticker: symbol.clone(),
        description: format!("{} trading pair", symbol),
        session: "24x7".into(),
        exchange: "BINANCE".into(), // 你也可以从 sub.exchange 填充
        minmov: 1,
        pricescale: 100, // 表示两位小数
        has_intraday: true,
        supported_resolutions,
        has_no_volume: false,
        type_: "crypto".into(),
        currency_code: matched_quote,
    };

    Ok(info)
}

/// 默认支持周期
fn default_supported_resolutions() -> Vec<String> {
    [
        TimeFrame::M1,
        TimeFrame::M5,
        TimeFrame::M15,
        TimeFrame::H1,
        TimeFrame::D1,
        TimeFrame::W1,
    ]
    .iter()
    .map(|tf| tf.to_str().to_string())
    .collect()
}

/// 根据 query 查询策略配置中可用的交易对
pub async fn get_strategy_symbol_data(query: &str) -> anyhow::Result<Vec<SymbolSearchResult>> {
    let q = query.trim().to_uppercase();

    // 获取全局策略配置
    let strategy_config = global::get_strategy_config().get();

    let mut results: Vec<SymbolSearchResult> = Vec::new();

    for sub in &strategy_config.subscriptions {
        let exchange = sub.exchange.to_uppercase();

        for sym in &sub.symbols {
            let base = sym.name.to_uppercase();
            let quote = sym.quote.to_uppercase();
            let mut symbol_name = format!("{}{}", base, quote);

            // 构建返回对象
            let result = SymbolSearchResult {
                name: symbol_name.clone(),
                ticker: symbol_name.clone(),
                description: Some(format!("{}/{} trading pair on {}", base, quote, exchange)),
                exchange: exchange.clone(),
                type_: "crypto".into(),
                currency_code: quote.clone(),
            };

            // 过滤逻辑：query 为空则返回全部，否则匹配 symbol、quote、组合名、exchange
            if q.is_empty()
                || base.contains(&q)
                || quote.contains(&q)
                || symbol_name.contains(&q)
                || exchange.contains(&q)
            {
                results.push(result);
            }
        }
    }

    // 排序：先按 exchange 再按 symbol name
    results.sort_by(|a, b| a.exchange.cmp(&b.exchange).then(a.name.cmp(&b.name)));

    Ok(results)
}

/// 获取币种历史数据
pub async fn fetch_history_data(query: &HistoryQuery) -> anyhow::Result<Vec<OhlcvRecord>> {
    let dbe = DefaultHmdsExchange::default();

    // symbol: 例如 "BTC/USDT"
    let symbol = &query.symbol;

    // resolution: 例如 "1", "5", "15", "60", "D"
    let period = &query.resolution;

    // 时间范围 from/to 秒级 -> 转为毫秒
    let from_ts = query.from * 1000;
    let to_ts = query.to * 1000;

    // limit: 如果 countback 有值，用它；否则不限制
    let limit = query.countback.map(|c| c as usize);

    // 拉取数据
    let klines: Vec<OhlcvRecord> = dbe.get_klines(symbol, period, limit, Some(from_ts), Some(to_ts)).await?;

    Ok(klines)
}
