use crate::domain::model::market_fill_range::{
    FillRangeFilter, HmdsMarketFillRange, NewHmdsMarketFillRange, UpdateHmdsMarketFillRange,
};
use crate::domain::model::{AppError, AppResult, PageResult, SortOrder};
use crate::domain::repository::Repository;
use crate::domain::repository::UpdatableRepository;
use crate::domain::repository::market_fill_range_repository::MarketFillRangeRepository;
use crate::domain::repository::{FilterableRepository, InsertableRepository};
use crate::global::get_app_config;
use crate::ingestor::generate_fill_range::generate_fill_ranges;
use crate::schema::hmds_market_fill_range::dsl::hmds_market_fill_range;
use crate::{impl_full_service, load_subscriptions};
use anyhow::Result;
use chrono::Utc;
use crab_infras::config::sub_config::Subscription;
use diesel::associations::HasTable;
use diesel::dsl::{max, min};
use diesel::{BoolExpressionMethods, ExpressionMethods, MysqlConnection, QueryDsl, RunQueryDsl};
use std::collections::HashMap;

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

    pub async fn generate_and_insert_fill_ranges(&mut self) -> Result<()> {
        generate_and_insert_fill_ranges(&mut self.repo.conn).await
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
    query = query.filter(status.eq(0).or(status.eq(3))).filter(retry_count.lt(5));

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

/// 返回每个周期的 `(earliest_start_time, latest_end_time)`
fn query_period_time_ranges_optimized(
    conn: &mut MysqlConnection,
    exchange_name: &str,
    symbol_name: &str,
) -> Result<HashMap<String, (i64, i64)>> {
    use crate::schema::hmds_market_fill_range::dsl::*;

    let results = hmds_market_fill_range
        .filter(exchange.eq(exchange_name))
        .filter(symbol.eq(symbol_name))
        .group_by(period) // ✅ 必须 group by 非聚合列
        .select((period, min(start_time), max(end_time)))
        .load::<(String, Option<i64>, Option<i64>)>(conn)?;

    let mut map = HashMap::new();
    for (p, min_st, max_et) in results {
        if let (Some(st), Some(et)) = (min_st, max_et) {
            map.insert(p, (st, et));
        }
    }

    Ok(map)
}

/// 统一生成历史回溯 + 实时增量区间，并插入数据库
pub async fn generate_and_insert_fill_ranges(conn: &mut MysqlConnection) -> Result<()> {
    let max_count = 500; // 默认 500 后续通过配置或者枚举获取
    // 假设 hmds.toml 在当前目录
    let app_config = get_app_config();
    let lookback_days = app_config.app.lookback_days;
    // -------------------------------
    // 2️⃣ 加载订阅配置
    // -------------------------------
    let subscriptions = load_subscriptions()?;

    let now_ts = Utc::now().timestamp_millis();
    let lookback_ts = now_ts - (lookback_days as i64 * 24 * 3600 * 1000);

    let subs_vec: Vec<Subscription> = subscriptions.iter().map(|e| e.value().clone()).collect();

    for sub in subs_vec {
        // 1️⃣ 查询每个周期的最早和最新时间
        let period_times = query_period_time_ranges_optimized(conn, &sub.exchange, &sub.symbol)?;

        let mut all_ranges = Vec::new();

        for period_str in &sub.periods {
            let (start_ts, _last_end_ts) = period_times
                .get(period_str.as_ref())
                .cloned()
                .unwrap_or((lookback_ts, lookback_ts));

            // 2️⃣ 生成区间（历史 + 增量）
            let ranges = generate_fill_ranges(&sub, start_ts, now_ts, max_count, period_str);

            all_ranges.extend(ranges);
        }

        // 3️⃣ 批量插入数据库
        if !all_ranges.is_empty() {
            diesel::insert_into(hmds_market_fill_range::table())
                .values(&all_ranges)
                .on_conflict_do_nothing()
                .execute(conn)?;
        }
    }

    Ok(())
}
