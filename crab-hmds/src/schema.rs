// @generated automatically by Diesel CLI.

diesel::table! {
    crab_ohlcv_record (id) {
        id -> Unsigned<Bigint>,
        #[max_length = 16]
        hash_id -> Binary,
        ts -> Bigint,
        period_start_ts -> Nullable<Bigint>,
        #[max_length = 64]
        symbol -> Varchar,
        #[max_length = 64]
        exchange -> Varchar,
        #[max_length = 16]
        period -> Varchar,
        open -> Double,
        high -> Double,
        low -> Double,
        close -> Double,
        volume -> Double,
        turnover -> Nullable<Double>,
        num_trades -> Nullable<Unsigned<Integer>>,
        vwap -> Nullable<Double>,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    market_backfill_meta (id) {
        id -> Unsigned<Bigint>,
        #[max_length = 64]
        exchange -> Varchar,
        #[max_length = 64]
        symbol -> Varchar,
        #[max_length = 16]
        interval -> Varchar,
        last_filled -> Nullable<Datetime>,
        last_checked -> Nullable<Datetime>,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    market_missing_range (id) {
        id -> Unsigned<Bigint>,
        market_id -> Unsigned<Bigint>,
        start_ts -> Datetime,
        end_ts -> Datetime,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::joinable!(market_missing_range -> market_backfill_meta (market_id));

diesel::allow_tables_to_appear_in_same_query!(crab_ohlcv_record, market_backfill_meta, market_missing_range,);
