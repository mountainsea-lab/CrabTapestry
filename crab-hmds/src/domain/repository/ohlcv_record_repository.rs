use crate::domain::model::ohlcv_record::{CrabOhlcvRecord, NewCrabOhlcvRecord, OhlcvFilter, UpdateCrabOhlcvRecord};
use crate::domain::model::{AppError, AppResult, SortOrder};
use crate::domain::repository::Repository;
use crate::{impl_full_repository, impl_repository_with_filter};
use diesel::{MysqlConnection, OptionalExtension, QueryDsl, RunQueryDsl, SelectableHelper};

// ohlcv_record_repository
pub struct OhlcvRecordRepository<'a> {
    pub conn: &'a mut MysqlConnection,
}

impl<'a> OhlcvRecordRepository<'a> {
    pub fn new(conn: &'a mut MysqlConnection) -> Self {
        Self { conn }
    }
}

impl_full_repository!(
    OhlcvRecordRepository, // Repository struct
    crab_ohlcv_record,     // Table name from schema.rs
    CrabOhlcvRecord,       // Model
    NewCrabOhlcvRecord,    // Insert model
    UpdateCrabOhlcvRecord  // Update model
);

impl_repository_with_filter!(
    OhlcvRecordRepository,
    crab_ohlcv_record,
    CrabOhlcvRecord,
    OhlcvFilter,
    @filter_var = filter,
    {
        use crate::schema::crab_ohlcv_record::dsl::*;
        let mut q = crab_ohlcv_record.into_boxed();


        if let Some(ref exchange_arg) = filter.exchange {
            q = q.filter(exchange.eq(exchange_arg));
        }

        if let Some(ref symbol_arg) = filter.symbol {
            q = q.filter(symbol.eq(symbol_arg));
        }

        if let Some(ref time_frame_arg) = filter.period {
            q = q.filter(period.eq(time_frame_arg));
        }

         if let Some(order) = &filter.sort_by_close_time {
            q = {
                match order {
                    SortOrder::Asc => q.order(ts.asc()),
                    SortOrder::Desc => q.order(ts.desc()),
                }
            };
        }
        q
    }
);
