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
use diesel::{BoolExpressionMethods, ExpressionMethods, MysqlConnection, QueryDsl, RunQueryDsl};

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
    filter: &FillRangeFilter,
) -> AppResult<Vec<HmdsMarketFillRange>> {
    use crate::schema::hmds_market_fill_range::dsl::*;

    let mut query = hmds_market_fill_range.into_boxed();

    // 默认条件：未同步或失败的区间，重试次数 < 5
    query = query.filter(
        status.eq(0).or(status.eq(3))
    ).filter(retry_count.lt(5));

    // 可选筛选条件
    if let Some(ref ex) = filter.exchange {
        query = query.filter(exchange.eq(ex));
    }
    if let Some(ref sym) = filter.symbol {
        query = query.filter(symbol.eq(sym));
    }
    if let Some(ref p) = filter.period {
        query = query.filter(period.eq(p));
    }
    if let Some(ref last_try) = filter.last_try_time {
        query = query.filter(last_try_time.le(last_try));
    }

    // 排序：按 start_time 最近的记录
    let sort_order = filter.sort_by_start_time.as_ref().unwrap_or(&SortOrder::Desc);
    query = match sort_order {
        SortOrder::Asc => query.order(start_time.asc()),
        SortOrder::Desc => query.order(start_time.desc()),
    };

    // 限制查询条数
    let limit_count = filter.limit.unwrap_or(100);
    query = query.limit(limit_count as i64);

    // 执行查询
    let result = query
        .load::<HmdsMarketFillRange>(conn)
        .map_err(|e| AppError::DatabaseError(e.into()))?;

    Ok(result)
}

