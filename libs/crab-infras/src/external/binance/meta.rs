use crate::external::binance::constant;
use barter_integration::protocol::http::rest::RestRequest;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Binance 交易所返回的交易所基本信息结构
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceExchangeInfo {
    /// 时区，例如 "UTC"
    pub timezone: Option<String>,

    /// 服务器时间戳
    pub server_time: Option<u64>,

    /// 期货类型（通常为字符串类型）
    pub futures_type: Option<String>,

    /// 所有支持的交易对信息
    pub symbols: Option<Vec<Symbol>>,

    /// 限速策略列表（如请求次数限制等）
    pub rate_limits: Option<Vec<RateLimit>>,

    /// 全局性过滤器列表
    pub exchange_filters: Option<Vec<String>>,
}

/// 合约市场信息结构（适用于 Binance Futures）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
    /// 交易对，如 "ONEUSDT"
    pub symbol: String,

    /// 交易对名称，通常与 symbol 一致
    pub pair: String,

    /// 合约类型，如 "PERPETUAL"
    pub contract_type: String,

    /// 交割日期（毫秒时间戳）
    pub delivery_date: i64,

    /// 上线日期（毫秒时间戳）
    pub onboard_date: i64,

    /// 状态，如 "TRADING"
    pub status: String,

    /// 维持保证金百分比，字符串表示浮点数
    pub maint_margin_percent: String,

    /// 起始保证金百分比
    pub required_margin_percent: String,

    /// 基础资产，如 "ONE"
    pub base_asset: String,

    /// 报价资产，如 "USDT"
    pub quote_asset: String,

    /// 保证金资产
    pub margin_asset: String,

    /// 价格精度，如 5 表示小数点后 5 位
    pub price_precision: u64,

    /// 数量精度（最小下单单位）
    pub quantity_precision: u64,

    /// 基础资产精度
    pub base_asset_precision: u64,

    /// 报价资产精度
    pub quote_precision: u64,

    /// 合约基础类型，如 "COIN"
    pub underlying_type: String,

    /// 合约子类型，例如 ["Layer-2"]
    pub underlying_sub_type: Option<Vec<String>>,

    /// 触发保护百分比
    pub trigger_protect: String,

    /// 清算手续费比例
    pub liquidation_fee: String,

    /// 市价吃单最大偏移保护
    pub market_take_bound: String,

    /// 最大移动下单限制
    pub max_move_order_limit: u64,

    /// 过滤器列表（JSON 类型）
    pub filters: Option<Vec<Filter>>,

    /// 支持的下单类型
    pub order_types: Option<Vec<String>>,

    /// 支持的 TIF 策略，如 GTC、IOC
    pub time_in_force: Option<Vec<String>>,

    /// 权限集，例如 ["GRID", "COPY"]
    pub permission_sets: Option<Vec<String>>,
}

/// 合约市场过滤器定义
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filter {
    pub filter_type: String,

    /// 价格过滤器（可选）
    pub tick_size: Option<String>,
    pub min_price: Option<String>,
    pub max_price: Option<String>,

    /// 数量过滤器
    pub step_size: Option<String>,
    pub min_qty: Option<String>,
    pub max_qty: Option<String>,

    /// 数量限制
    pub limit: Option<u32>,

    /// 最小名义金额
    pub notional: Option<String>,

    /// 百分比价格限制
    pub multiplier_decimal: Option<String>,
    pub multiplier_up: Option<String>,
    pub multiplier_down: Option<String>,

    /// 风控字段
    pub position_control_side: Option<String>,
}

/// 表示 Binance 中的 API 访问速率限制规则
/// API 限速信息（来自 Binance 接口的 rateLimits 字段）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimit {
    /// 限速类型，如 REQUEST_WEIGHT、ORDERS
    pub rate_limit_type: Option<String>,

    /// 限速区间单位，如 MINUTE、SECOND
    pub interval: Option<String>,

    /// 区间单位数量，例如 1 表示每 1 分钟/秒
    pub interval_num: Option<u32>,

    /// 在此时间段内的请求上限
    pub limit: Option<u32>,
}

pub struct FetchExchangeInfoRequest;

impl RestRequest for FetchExchangeInfoRequest {
    type Response = ExchangeInfoResponse;
    type QueryParams = ();
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed(constant::EXCHANGE_INFO)
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }
}

#[derive(Debug, Deserialize)]
pub struct ExchangeInfoResponse(pub BinanceExchangeInfo);
