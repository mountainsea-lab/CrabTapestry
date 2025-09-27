use crate::domain::model::ohlcv_record::{CrabOhlcvRecord, NewCrabOhlcvRecord, OhlcvFilter, UpdateCrabOhlcvRecord};
use crate::domain::model::{AppError, AppResult, PageResult, SortOrder};
use crate::domain::repository::Repository;
use crate::domain::repository::UpdatableRepository;
use crate::domain::repository::ohlcv_record_repository::OhlcvRecordRepository;
use crate::domain::repository::{FilterableRepository, InsertableRepository};
use crate::impl_full_service;
use crate::schema::crab_ohlcv_record::dsl::crab_ohlcv_record;
use crate::schema::crab_ohlcv_record::{exchange, period, symbol, ts};
use anyhow::Result;
use diesel::sql_types::*;
use diesel::{ExpressionMethods, MysqlConnection, QueryDsl, RunQueryDsl};

impl_full_service!(
    OhlcvRecordService,
    OhlcvRecordRepository,
    CrabOhlcvRecord,
    NewCrabOhlcvRecord,
    UpdateCrabOhlcvRecord
);

impl<'a> OhlcvRecordService<'a> {
    pub fn query_page_with_total(
        &mut self,
        filter: OhlcvFilter,
        page: i64,
        per_page: i64,
    ) -> AppResult<PageResult<CrabOhlcvRecord>> {
        let data = self.repo.filter_paginated(&filter, page, per_page)?;
        let total = self.repo.count_filtered(&filter)?;
        Ok(PageResult { data, total, page, per_page })
    }

    pub async fn insert_new_ohlcv_records_batch(&mut self, datas: &[NewCrabOhlcvRecord]) -> Result<()> {
        insert_new_ohlcv_records_batch(&mut self.repo.conn, datas, 500)?;
        Ok(())
    }

    pub fn query_list(&mut self, filter: OhlcvFilter) -> AppResult<Vec<CrabOhlcvRecord>> {
        let data = query_list_by_filter(&mut self.repo.conn, &filter)?;
        Ok(data)
    }
}

/// 批量安全插入新 K 线，遇到 hash_id 已存在自动忽略
/// 自动按 batch_size 拆分
pub fn insert_new_ohlcv_records_batch(
    conn: &mut MysqlConnection,
    ohlcv_records: &[NewCrabOhlcvRecord],
    batch_size: usize, // 每批大小，例如 500
) -> Result<usize, diesel::result::Error> {
    if ohlcv_records.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0;

    // 按 batch_size 拆分数据
    for batch in ohlcv_records.chunks(batch_size) {
        for rec in batch {
            diesel::sql_query(
                "INSERT IGNORE INTO crab_ohlcv_record \
                (hash_id, ts, period_start_ts, symbol, exchange, period, open, high, low, close, volume, turnover, num_trades, vwap, created_at) \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"
            )
                .bind::<Binary, _>(rec.hash_id.clone())
                .bind::<Bigint, _>(rec.ts)
                .bind::<Nullable<Bigint>, _>(rec.period_start_ts)
                .bind::<VarChar, _>(rec.symbol.clone())
                .bind::<VarChar, _>(rec.exchange.clone())
                .bind::<VarChar, _>(rec.period.clone())
                .bind::<Double, _>(rec.open)
                .bind::<Double, _>(rec.high)
                .bind::<Double, _>(rec.low)
                .bind::<Double, _>(rec.close)
                .bind::<Double, _>(rec.volume)
                .bind::<Nullable<Double>, _>(rec.turnover)
                .bind::<Nullable<Integer>, _>(rec.num_trades.map(|v| v as i32))
                .bind::<Nullable<Double>, _>(rec.vwap)
                .execute(conn)?;

            total_inserted += 1;
        }
    }

    Ok(total_inserted)
}

pub fn query_list_by_filter(conn: &mut MysqlConnection, ohlcv_filter: &OhlcvFilter) -> AppResult<Vec<CrabOhlcvRecord>> {
    let mut query = crab_ohlcv_record.into_boxed(); // 初始化为可扩展查询

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

    if let Some(close_time) = ohlcv_filter.close_time {
        query = query.filter(ts.eq(close_time)); // 根据时间戳过滤
    }

    // 添加排序功能：如果 `sort_by_close_time` 被指定，则按时间排序
    if let Some(sort_order) = &ohlcv_filter.sort_by_close_time {
        match sort_order {
            SortOrder::Asc => {
                query = query.order_by(ts.asc());
            }
            SortOrder::Desc => {
                query = query.order_by(ts.desc());
            }
        }
    }

    // 执行查询并返回结果
    let result = query
        .load::<CrabOhlcvRecord>(conn)
        .map_err(|e| AppError::DatabaseError(e.into()))?;

    Ok(result)
}
