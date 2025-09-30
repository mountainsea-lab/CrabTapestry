use crate::domain::model::PageResult;
use crate::domain::model::market_fill_range::{FillRangeFilter, HmdsMarketFillRange};
use crate::domain::model::ohlcv_record::{CrabOhlcvRecord, NewCrabOhlcvRecord, OhlcvFilter};
use crate::domain::repository::market_fill_range_repository::MarketFillRangeRepository;
use crate::domain::repository::ohlcv_record_repository::OhlcvRecordRepository;
use crate::domain::service::market_fill_range_service::MarketFillRangeService;
use crate::domain::service::ohlcv_record_service::OhlcvRecordService;
use crate::global::get_mysql_pool;

pub mod market_fill_range_service;
mod ohlcv_record_service;

/// 批量保存K线数据
pub async fn save_ohlcv_records_batch(datas: &[NewCrabOhlcvRecord]) -> Result<(), anyhow::Error> {
    let mut conn = get_mysql_pool().get()?;
    let repo = OhlcvRecordRepository::new(&mut conn);
    let mut ohlcv_record_service = OhlcvRecordService { repo };
    ohlcv_record_service.insert_new_ohlcv_records_batch(datas).await
}
/// 分页查询ohlcv records数据
pub async fn query_ohlcv_page(ohlcv_filter: OhlcvFilter) -> Result<PageResult<CrabOhlcvRecord>, anyhow::Error> {
    let mut conn = get_mysql_pool().get()?;
    let repo = OhlcvRecordRepository::new(&mut conn);
    let mut ohlcv_record_service = OhlcvRecordService { repo };
    let page = ohlcv_filter.page.map_or_else(|| 1, |p| p);
    let page_size = ohlcv_filter.page_size.map_or_else(|| 20, |p| p);
    let result = ohlcv_record_service
        .query_page_with_total(ohlcv_filter, page as i64, page_size as i64)
        .map_err(|e| anyhow::Error::new(e))?;

    Ok(result)
}

/// 条件查询ohlcv records线集合
pub async fn query_ohlcv_list(ohlcv_filter: OhlcvFilter) -> Result<Vec<CrabOhlcvRecord>, anyhow::Error> {
    let mut conn = get_mysql_pool().get()?;
    let repo = OhlcvRecordRepository::new(&mut conn);
    let mut ohlcv_record_service = OhlcvRecordService { repo };
    let result = ohlcv_record_service
        .query_list(ohlcv_filter)
        .await
        .map_err(|e| anyhow::Error::new(e))?;

    Ok(result)
}
/// 生成维护任务
pub async fn generate_and_insert_fill_ranges() -> Result<(), anyhow::Error> {
    let mut conn = get_mysql_pool().get()?;
    let repo = MarketFillRangeRepository::new(&mut conn);
    let mut market_fill_range_service = MarketFillRangeService { repo };
    market_fill_range_service.generate_and_insert_fill_ranges().await?;
    Ok(())
}
/// 查询区间任务集合
pub async fn query_fill_range_list() -> Result<Vec<HmdsMarketFillRange>, anyhow::Error> {
    let mut conn = get_mysql_pool().get()?;
    let repo = MarketFillRangeRepository::new(&mut conn);
    let mut market_fill_range_service = MarketFillRangeService { repo };
    let range_filter = FillRangeFilter {
        exchange: None,
        symbol: None,
        period: None,
        status: None,
        retry_count: None,
        last_try_time: None,
        sort_by_start_time: None,
        limit: Some(1000),
        page: None,
        page_size: None,
    };
    let result = market_fill_range_service.query_list(range_filter).await?;
    Ok(result)
}

/// 查询交易所、币种、周期最新区间任务
pub async fn query_latest_ranges() -> Result<Vec<HmdsMarketFillRange>, anyhow::Error> {
    let mut conn = get_mysql_pool().get()?;
    let repo = MarketFillRangeRepository::new(&mut conn);
    let mut market_fill_range_service = MarketFillRangeService { repo };
    let result = market_fill_range_service.query_latest_ranges().await?;
    Ok(result)
}
