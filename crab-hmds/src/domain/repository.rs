use crate::domain::model::AppResult;

pub mod market_fill_range_repository;
pub mod ohlcv_record_repository;

// 定义 Repository trait
pub trait Repository<T> {
    fn get_all(&mut self) -> AppResult<Vec<T>>;
    fn get_by_id(&mut self, id: u64) -> AppResult<Option<T>>;
    fn delete(&mut self, id: u64) -> AppResult<usize>;
}

pub trait InsertableRepository<E> {
    fn insert(&mut self, entity: &E) -> AppResult<usize>;
}

pub trait UpdatableRepository<E> {
    fn update(&mut self, entity: &E) -> AppResult<usize>;
}

pub trait FilterableRepository<F, T> {
    fn filter_paginated(&mut self, filter: &F, page: i64, per_page: i64) -> AppResult<Vec<T>>;

    fn count_filtered(&mut self, _filter: &F) -> AppResult<i64> {
        Ok(0) // 可重写，如需要计数
    }
}
