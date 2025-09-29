use crate::domain::model::market_fill_range::{
    FillRangeFilter, HmdsMarketFillRange, NewHmdsMarketFillRange, UpdateHmdsMarketFillRange,
};
use crate::domain::model::{AppError, AppResult, PageResult, SortOrder};
use crate::domain::repository::Repository;
use crate::domain::repository::UpdatableRepository;
use crate::domain::repository::market_fill_range_repository::MarketFillRangeRepository;
use crate::domain::repository::{FilterableRepository, InsertableRepository};
use crate::impl_full_service;
use crate::schema::hmds_market_fill_range::dsl::hmds_market_fill_range;
use crate::schema::hmds_market_fill_range::{exchange, period, start_time, status, symbol};
use anyhow::Result;
use diesel::{ExpressionMethods, MysqlConnection, QueryDsl, RunQueryDsl};

impl_full_service!(
    MarketFillRangeService,
    MarketFillRangeRepository,
    HmdsMarketFillRange,
    NewHmdsMarketFillRange,
    UpdateHmdsMarketFillRange
);

impl<'a> MarketFillRangeService<'a> {
    pub fn query_page_with_total(
        &mut self,
        filter: FillRangeFilter,
        page: i64,
        per_page: i64,
    ) -> AppResult<PageResult<HmdsMarketFillRange>> {
        let data = self.repo.filter_paginated(&filter, page, per_page)?;
        let total = self.repo.count_filtered(&filter)?;
        Ok(PageResult { data, total, page, per_page })
    }

    pub async fn insert_new_records_batch(&mut self, _datas: &[NewHmdsMarketFillRange]) -> Result<()> {
        todo!();
        Ok(())
    }

    pub async fn query_list(&mut self, filter: FillRangeFilter) -> AppResult<Vec<HmdsMarketFillRange>> {
        let data = query_list_by_filter(&mut self.repo.conn, &filter).await?;
        Ok(data)
    }
}

pub async fn query_list_by_filter(
    conn: &mut MysqlConnection,
    ohlcv_filter: &FillRangeFilter,
) -> AppResult<Vec<HmdsMarketFillRange>> {
    let mut query = hmds_market_fill_range.into_boxed(); // 初始化为可扩展查询

    // 根据 `OhlcvFilter` 动态添加筛选条件
    if let Some(ref symbol_val) = ohlcv_filter.symbol {
        query = query.filter(symbol.eq(symbol_val)); // 确保符号匹配
    }

    if let Some(ref exchange_val) = ohlcv_filter.exchange {
        query = query.filter(exchange.eq(exchange_val)); // 确保交易所匹配
    }

    if let Some(ref period_arg) = ohlcv_filter.period {
        query = query.filter(period.eq(period_arg)); // 确保周期匹配
    }

    if let Some(status_val) = ohlcv_filter.status {
        query = query.filter(status.eq(status_val)); // 根据时间戳过滤
    }

    // 默认排序：按 `period_start_ts` 升序排序
    query = query.order_by(start_time.desc());

    // 添加排序功能：如果 `sort_by_close_time` 被指定，则按时间排序
    if let Some(sort_order) = &ohlcv_filter.sort_by_start_time {
        match sort_order {
            SortOrder::Asc => {
                query = query.order_by(start_time.asc());
            }
            SortOrder::Desc => {
                query = query.order_by(start_time.desc());
            }
        }
    }

    // 执行查询并返回结果
    let result = query
        .load::<HmdsMarketFillRange>(conn)
        .map_err(|e| AppError::DatabaseError(e.into()))?;

    Ok(result)
}
