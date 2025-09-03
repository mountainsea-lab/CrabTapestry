/// CoinMarketCap API. All requests should target domain
pub const BASE_URL: &str = "https://fapi.binance.com";

/// https://docs.coingecko.com/v3.0.1/reference/coins-markets
/// This endpoint allows you to query all the supported coins with price, market cap, volume and market related data
pub const EXCHANGE_INFO: &str = "/fapi/v1/exchangeInfo";

/// https://docs.coingecko.com/v3.0.1/reference/coins-markets
/// This endpoint allows you to query all the supported coins with price, market cap, volume and market related data
pub const KLINES: &str = "/fapi/v1/klines";
