// @generated automatically by Diesel CLI.

diesel::table! {
    hmds_market_fill_range (id) {
        id -> Unsigned<Bigint>,
        #[max_length = 50]
        exchange -> Varchar,
        #[max_length = 50]
        symbol -> Varchar,
        #[max_length = 50]
        quote -> Varchar,
        #[max_length = 10]
        period -> Varchar,
        start_time -> Bigint,
        end_time -> Bigint,
        status -> Tinyint,
        retry_count -> Integer,
        batch_size -> Integer,
        last_try_time -> Nullable<Timestamp>,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    hmds_ohlcv_record (id) {
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

diesel::allow_tables_to_appear_in_same_query!(hmds_market_fill_range, hmds_ohlcv_record,);
