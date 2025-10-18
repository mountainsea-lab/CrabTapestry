use crate::domain::model::ohlcv_record::{HmdsOhlcvRecord, NewHmdsOhlcvRecord, OhlcvFilter, UpdateHmdsOhlcvRecord};
use crate::domain::model::{AppError, AppResult, PageResult, SortOrder};
use crate::domain::repository::Repository;
use crate::domain::repository::UpdatableRepository;
use crate::domain::repository::ohlcv_record_repository::OhlcvRecordRepository;
use crate::domain::repository::{FilterableRepository, InsertableRepository};
use crate::impl_full_service;
// use crate::schema::hmds_ohlcv_record::dsl::hmds_ohlcv_record;
// use crate::schema::hmds_ohlcv_record::{exchange, period, period_start_ts, symbol, ts};
use crate::global::get_mysql_pool;
use anyhow::Result;
use diesel::{Connection, ExpressionMethods, MysqlConnection, QueryDsl, RunQueryDsl, sql_query};
use hex;
use tokio::task;

impl_full_service!(
    OhlcvRecordService,
    OhlcvRecordRepository,
    HmdsOhlcvRecord,
    NewHmdsOhlcvRecord,
    UpdateHmdsOhlcvRecord
);

impl<'a> OhlcvRecordService<'a> {
    pub fn query_page_with_total(
        &mut self,
        filter: OhlcvFilter,
        page: i64,
        per_page: i64,
    ) -> AppResult<PageResult<HmdsOhlcvRecord>> {
        let data = self.repo.filter_paginated(&filter, page, per_page)?;
        let total = self.repo.count_filtered(&filter)?;
        Ok(PageResult { data, total, page, per_page })
    }

    pub async fn insert_new_ohlcv_records_batch(&mut self, datas: &[NewHmdsOhlcvRecord]) -> Result<()> {
        insert_new_ohlcv_records_batch(&mut self.repo.conn, datas, 500)?;
        Ok(())
    }

    pub async fn query_list(&mut self, filter: OhlcvFilter) -> AppResult<Vec<HmdsOhlcvRecord>> {
        let data = query_list_by_filter(&mut self.repo.conn, &filter).await?;
        Ok(data)
    }
}

/// 批量插入
pub fn insert_new_ohlcv_records_batch(
    conn: &mut MysqlConnection,
    records: &[NewHmdsOhlcvRecord],
    batch_size: usize,
) -> Result<usize, diesel::result::Error> {
    if records.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0;

    conn.transaction::<_, diesel::result::Error, _>(|tx_conn| {
        for batch in records.chunks(batch_size) {
            // 构造批量 SQL
            let mut sql = String::from(
                "INSERT IGNORE INTO hmds_ohlcv_record \
                 (hash_id, ts, period_start_ts, symbol, exchange, period, open, high, low, close, volume, turnover, num_trades, vwap, created_at) VALUES ",
            );

            let mut values: Vec<String> = Vec::with_capacity(batch.len());
            for rec in batch {
                values.push(format!(
                    "(X'{}', {}, {}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, CURRENT_TIMESTAMP)",
                    hex::encode(&rec.hash_id),
                    rec.ts,
                    rec.period_start_ts.map_or("NULL".to_string(), |v| v.to_string()),
                    rec.symbol.replace("'", "''"),
                    rec.exchange.replace("'", "''"),
                    rec.period.replace("'", "''"),
                    rec.open,
                    rec.high,
                    rec.low,
                    rec.close,
                    rec.volume,
                    rec.turnover.map_or("NULL".to_string(), |v| v.to_string()),
                    rec.num_trades.map_or("NULL".to_string(), |v| v.to_string()),
                    rec.vwap.map_or("NULL".to_string(), |v| v.to_string())
                ));
            }

            sql.push_str(&values.join(","));

            let inserted = sql_query(sql).execute(tx_conn)?;
            total_inserted += inserted;
        }

        Ok(total_inserted)
    })
}
pub async fn query_list_by_filter(
    conn: &mut MysqlConnection,
    ohlcv_filter: &OhlcvFilter,
) -> AppResult<Vec<HmdsOhlcvRecord>> {
    use crate::schema::hmds_ohlcv_record::dsl::*;

    let mut query = hmds_ohlcv_record.into_boxed(); // 支持动态查询组合

    // === 动态条件过滤 ===
    if let Some(ref sym) = ohlcv_filter.symbol {
        query = query.filter(symbol.eq(sym));
    }

    if let Some(ref ex) = ohlcv_filter.exchange {
        query = query.filter(exchange.eq(ex));
    }

    if let Some(ref per) = ohlcv_filter.period {
        query = query.filter(period.eq(per));
    }

    // ✅ 时间区间过滤
    if let Some(start_ts) = ohlcv_filter.start_time {
        query = query.filter(ts.ge(start_ts)); // ts >= start_time
    }
    if let Some(end_ts) = ohlcv_filter.end_time {
        query = query.filter(ts.le(end_ts)); // ts <= end_time
    }

    // === 排序逻辑 ===
    // 默认按 period_start_ts 降序
    query = query.order(period_start_ts.desc());

    // 若指定 sort_by_close_time，则覆盖默认排序
    if let Some(sort_order) = &ohlcv_filter.sort_by_close_time {
        query = match sort_order {
            SortOrder::Asc => query.order(ts.asc()),
            SortOrder::Desc => query.order(ts.desc()),
        };
    }

    // === 限制条数 ===
    if let Some(limit_val) = ohlcv_filter.limit {
        query = query.limit(limit_val as i64);
    }

    // === 执行查询 ===
    let result = query
        .load::<HmdsOhlcvRecord>(conn)
        .map_err(|e| AppError::DatabaseError(e.into()))?;

    Ok(result)
}
