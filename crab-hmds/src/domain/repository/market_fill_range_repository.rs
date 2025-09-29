use crate::domain::model::market_fill_range::{
    FillRangeFilter, HmdsMarketFillRange, NewHmdsMarketFillRange, UpdateHmdsMarketFillRange,
};
use crate::domain::model::{AppError, AppResult, SortOrder};
use crate::domain::repository::Repository;
use crate::{impl_full_repository, impl_repository_with_filter};
use diesel::{MysqlConnection, OptionalExtension, QueryDsl, RunQueryDsl, SelectableHelper};

// ohlcv_record_repository
pub struct MarketFillRangeRepository<'a> {
    pub conn: &'a mut MysqlConnection,
}

impl<'a> MarketFillRangeRepository<'a> {
    pub fn new(conn: &'a mut MysqlConnection) -> Self {
        Self { conn }
    }
}

impl_full_repository!(
    MarketFillRangeRepository, // Repository struct
    hmds_market_fill_range,    // Table name from schema.rs
    HmdsMarketFillRange,       // Model
    NewHmdsMarketFillRange,    // Insert model
    UpdateHmdsMarketFillRange  // Update model
);

impl_repository_with_filter!(
    MarketFillRangeRepository,
    hmds_market_fill_range,
    HmdsMarketFillRange,
    FillRangeFilter,
    @filter_var = filter,
    {
        use crate::schema::hmds_market_fill_range::dsl::*;
        let mut q = hmds_market_fill_range.into_boxed();


        if let Some(ref exchange_arg) = filter.exchange {
            q = q.filter(exchange.eq(exchange_arg));
        }

        if let Some(ref symbol_val) = filter.symbol {
            q = q.filter(symbol.eq(symbol_val));
        }

        if let Some(ref time_frame_val) = filter.period {
            q = q.filter(period.eq(time_frame_val));
        }

        if let Some(ref status_val) = filter.status {
            q = q.filter(status.eq(status_val));
        }

        if let Some(ref retry_count_val) = filter.retry_count {
            q = q.filter(retry_count.gt(retry_count_val));
        }

        if let Some(ref last_try_time_val) = filter.last_try_time {
            q = q.filter(last_try_time.gt(last_try_time_val));
        }

        if let Some(order) = &filter.sort_by_start_time {
            q = {
                match order {
                    SortOrder::Asc => q.order(start_time.asc()),
                    SortOrder::Desc => q.order(start_time.desc()),
                }
            };
        }
        q
    }
);
