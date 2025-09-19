use crate::domain::model::ohlcv_record::NewCrabOhlcvRecord;
use crate::domain::repository::ohlcv_record_repository::OhlcvRecordRepository;
use crate::domain::service::ohlcv_record_service::OhlcvRecordService;
use crate::global::get_mysql_pool;

mod ohlcv_record_service;

/// 批量保存K线数据
pub async fn save_ohlcv_records_batch(datas: &[NewCrabOhlcvRecord]) -> Result<(), anyhow::Error> {
    let mut conn = get_mysql_pool().get()?;
    let repo = OhlcvRecordRepository::new(&mut conn);
    let mut ohlcv_record_service = OhlcvRecordService { repo };
    ohlcv_record_service.insert_new_ohlcv_records_batch(datas).await
}
