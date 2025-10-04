use crate::domain::model::ohlcv_record::{HmdsOhlcvRecord, NewHmdsOhlcvRecord, OhlcvFilter, UpdateHmdsOhlcvRecord};
use crate::domain::model::{AppError, AppResult, PageResult, SortOrder};
use crate::domain::repository::Repository;
use crate::domain::repository::UpdatableRepository;
use crate::domain::repository::ohlcv_record_repository::OhlcvRecordRepository;
use crate::domain::repository::{FilterableRepository, InsertableRepository};
use crate::impl_full_service;
use crate::schema::hmds_ohlcv_record::dsl::hmds_ohlcv_record;
use crate::schema::hmds_ohlcv_record::{exchange, period, period_start_ts, symbol, ts};
use anyhow::Result;
use diesel::sql_types::*;
use diesel::{ExpressionMethods, MysqlConnection, QueryDsl, RunQueryDsl, sql_query};
use hex;
use std::error::Error;
use std::io::Write;

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
        insert_new_ohlcv_records_batch(&mut self.repo.conn, datas, 500).await?;
        Ok(())
    }

    pub async fn query_list(&mut self, filter: OhlcvFilter) -> AppResult<Vec<HmdsOhlcvRecord>> {
        let data = query_list_by_filter(&mut self.repo.conn, &filter).await?;
        Ok(data)
    }
}

/// 批量安全插入新 K 线，遇到 hash_id 已存在自动忽略
/// 自动按 batch_size 拆分
pub async fn insert_new_ohlcv_records_batch(
    conn: &mut MysqlConnection,
    ohlcv_records: &[NewHmdsOhlcvRecord],
    batch_size: usize, // 每批大小，例如 500
) -> Result<usize, diesel::result::Error> {
    if ohlcv_records.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0;

    // 按 batch_size 拆分数据
    for batch in ohlcv_records.chunks(batch_size) {
        for rec in batch {
            sql_query(
                "INSERT IGNORE INTO hmds_ohlcv_record \
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

/// 批量安全插入 K 线，重复 hash_id 自动忽略 批量优化版本
#[warn(unused_imports)]
pub fn insert_new_ohlcv_records_batch_file(
    conn: &mut MysqlConnection,
    ohlcv_records: &[NewHmdsOhlcvRecord],
    batch_size: usize, // 每批大小，例如 10_000
    created_at: &str,  // 固定时间戳，例如 "2025-10-03 12:00:00"
) -> Result<usize, Box<dyn Error>> {
    if ohlcv_records.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0;

    for batch in ohlcv_records.chunks(batch_size) {
        // 1️⃣ 创建临时文件
        let mut tmpfile = tempfile::NamedTempFile::new()?;

        // 2️⃣ 写入制表符分隔的 CSV 数据
        for rec in batch {
            writeln!(
                tmpfile,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                hex::encode(&rec.hash_id),
                rec.ts,
                rec.period_start_ts.unwrap_or(0),
                &rec.symbol,
                &rec.exchange,
                &rec.period,
                rec.open,
                rec.high,
                rec.low,
                rec.close,
                rec.volume,
                rec.turnover.unwrap_or(0.0),
                rec.num_trades.unwrap_or(0),
                rec.vwap.unwrap_or(0.0),
                created_at // 固定 created_at
            )?;
        }
        tmpfile.flush()?;
        let path = tmpfile.path().to_str().unwrap();

        // 3️⃣ 构造 LOAD DATA LOCAL INFILE IGNORE SQL
        let sql = format!(
            r#"LOAD DATA LOCAL INFILE '{}'
            IGNORE
            INTO TABLE hmds_ohlcv_record
            FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n'
            (hash_id, ts, period_start_ts, symbol, exchange, period,
             open, high, low, close, volume, turnover, num_trades, vwap, created_at)"#,
            path
        );

        // 4️⃣ 执行 SQL
        sql_query(sql).execute(conn)?;

        total_inserted += batch.len();
    }

    Ok(total_inserted)
}

pub async fn query_list_by_filter(
    conn: &mut MysqlConnection,
    ohlcv_filter: &OhlcvFilter,
) -> AppResult<Vec<HmdsOhlcvRecord>> {
    let mut query = hmds_ohlcv_record.into_boxed(); // 初始化为可扩展查询

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

    // 默认排序：按 `period_start_ts` 升序排序
    query = query.order_by(period_start_ts.asc());

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
        .load::<HmdsOhlcvRecord>(conn)
        .map_err(|e| AppError::DatabaseError(e.into()))?;

    Ok(result)
}
