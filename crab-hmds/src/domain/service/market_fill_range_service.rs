use crate::domain::model::market_fill_range::{
    FillRangeFilter, HmdsMarketFillRange, NewHmdsMarketFillRange, UpdateHmdsMarketFillRange,
};
use crate::domain::model::{AppError, AppResult, PageResult, SortOrder};
use crate::domain::repository::Repository;
use crate::domain::repository::UpdatableRepository;
use crate::domain::repository::market_fill_range_repository::MarketFillRangeRepository;
use crate::domain::repository::{FilterableRepository, InsertableRepository};
use crate::global::get_app_config;
use crate::ingestor::generate_fill_range::{generate_fill_ranges_full, should_generate_ranges};
use crate::schema::hmds_market_fill_range::dsl::hmds_market_fill_range;
use crate::{impl_full_service, load_subscriptions};
use anyhow::Result;
use chrono::Utc;
use crab_infras::config::sub_config::Subscription;
use diesel::associations::HasTable;
use diesel::dsl::{max, min};
use diesel::{BoolExpressionMethods, ExpressionMethods, MysqlConnection, QueryDsl, RunQueryDsl};
use ms_tracing::tracing_utils::internal::{debug, info};
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

    pub async fn generate_and_insert_fill_ranges(&mut self) -> AppResult<()> {
        generate_and_insert_fill_ranges(&mut self.repo.conn).await
    }

    pub async fn query_list(&mut self, filter: FillRangeFilter) -> AppResult<Vec<HmdsMarketFillRange>> {
        let data = query_list_by_filter(&mut self.repo.conn, &filter).await?;
        Ok(data)
    }

    pub async fn query_latest_ranges(&mut self) -> AppResult<Vec<HmdsMarketFillRange>> {
        query_latest_ranges(&mut self.repo.conn).await
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
/// 查询交易所、币种、周期最新区间任务
pub async fn query_latest_ranges(conn: &mut MysqlConnection) -> AppResult<Vec<HmdsMarketFillRange>> {
    use diesel::RunQueryDsl;
    use diesel::sql_query;

    let sql = r#"
        SELECT t.*
        FROM hmds_market_fill_range t
        INNER JOIN (
            SELECT exchange, symbol, period, MAX(end_time) AS max_end_time
            FROM hmds_market_fill_range
            GROUP BY exchange, symbol, period
        ) latest
        ON t.exchange = latest.exchange
        AND t.symbol = latest.symbol
        AND t.period = latest.period
        AND t.end_time = latest.max_end_time
    "#;

    let result = sql_query(sql)
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
pub async fn generate_and_insert_fill_ranges(conn: &mut MysqlConnection) -> AppResult<()> {
    let max_count = 500;
    let app_config = get_app_config();
    let lookback_days = app_config.app.lookback_days;
    let subscriptions = load_subscriptions()?;

    let now_ts = Utc::now().timestamp_millis();
    let lookback_ts = now_ts - (lookback_days * 24 * 3600 * 1000);

    info!(
        "Starting fill range generation: lookback_ts={}, now_ts={}",
        lookback_ts, now_ts
    );

    let subs_vec: Vec<Subscription> = subscriptions.iter().map(|e| e.value().clone()).collect();
    let mut all_ranges = Vec::new();
    let mut total_generated = 0;

    for sub in &subs_vec {
        // 查询每个周期的最早开始时间和最晚结束时间
        let period_times = query_period_time_ranges_optimized(conn, &sub.exchange, &sub.symbol)?;

        for period_str in &sub.periods {
            let (existing_start_ts, existing_end_ts) = period_times
                .get(period_str.as_ref())
                .cloned()
                .unwrap_or((lookback_ts, lookback_ts));

            let (needs_lookback, needs_incremental) =
                should_generate_ranges(existing_start_ts, existing_end_ts, lookback_ts, now_ts);

            if !needs_lookback && !needs_incremental {
                debug!(
                    "No new ranges needed for {}/{}/{}",
                    sub.exchange, sub.symbol, period_str
                );
                continue;
            }

            // 使用新的生成函数
            let ranges = generate_fill_ranges_full(
                sub,
                existing_start_ts,
                existing_end_ts,
                lookback_ts,
                now_ts,
                max_count,
                period_str,
            );

            total_generated += ranges.len();
            all_ranges.extend(ranges);

            // 分批插入避免单次事务过大
            if all_ranges.len() >= 1000 {
                batch_insert_ranges(conn, &all_ranges)?;
                all_ranges.clear();
            }
        }
    }

    // 插入剩余的数据
    if !all_ranges.is_empty() {
        batch_insert_ranges(conn, &all_ranges)?;
    }

    info!(
        "Fill range generation completed: {} total ranges generated",
        total_generated
    );
    Ok(())
}

// /// 改进的查询函数，支持批量查询多个交易对
// fn batch_query_period_time_ranges(
//     conn: &mut MysqlConnection,
//     subscriptions: &[Subscription],
// ) -> Result<HashMap<(String, String), HashMap<String, (i64, i64)>>> {
//     use crate::schema::hmds_market_fill_range::dsl::*;
//
//     // 构建查询条件
//     let mut query = hmds_market_fill_range.into_boxed();
//
//     let mut conditions = Vec::new();
//     for sub in subscriptions {
//         conditions.push(exchange.eq(&sub.exchange).and(symbol.eq(&sub.symbol)));
//     }
//
//     if !conditions.is_empty() {
//         query = query.or_filter(conditions);
//     }
//
//     let results = query
//         .group_by((exchange, symbol, period))
//         .select((exchange, symbol, period, min(start_time), max(end_time)))
//         .load::<(String, String, String, Option<i64>, Option<i64>)>(conn)?;
//
//     let mut map = HashMap::new();
//     for (ex, sym, per, min_st, max_et) in results {
//         if let (Some(st), Some(et)) = (min_st, max_et) {
//             map.entry((ex, sym))
//                 .or_insert_with(HashMap::new)
//                 .insert(per, (st, et));
//         }
//     }
//
//     Ok(map)
// }

/// 批量插入填充范围
fn batch_insert_ranges(conn: &mut MysqlConnection, ranges: &[NewHmdsMarketFillRange]) -> AppResult<usize> {
    use crate::schema::hmds_market_fill_range::dsl::*;

    let inserted = diesel::insert_into(hmds_market_fill_range::table())
        .values(ranges)
        .on_conflict_do_nothing()
        .execute(conn)?;

    Ok(inserted)
}
