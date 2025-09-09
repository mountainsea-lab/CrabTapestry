// use crate::ingestor::types::{OHLCVRecord, PublicTradeEvent};
// use crab_types::time_frame::TimeFrame;
// use std::sync::Arc;
// use trade_aggregation::{
//     Aggregator, AlignedTimeRule, GenericAggregator, MillisecondPeriod, TimestampResolution, Trade,
// };
//
// /// 单周期聚合器
// pub struct SingleAggregator {
//     period: Arc<str>, // "1m", "5m", ...
// }
//
// impl SingleAggregator {
//     pub fn new(period: Arc<str>) -> Self {
//         Self { period }
//     }
//
//     /// 输入一个 tick/trade，返回周期完成的 OHLCVRecord
//     pub fn on_event(&mut self, event: PublicTradeEvent) -> Option<OHLCVRecord> {
//         if let Some(time_period) = TimeFrame::from_str_and_get_millis(&self.period) {
//             let mut aggregator = GenericAggregator::new(
//                 AlignedTimeRule::new(
//                     MillisecondPeriod::from_non_zero(time_period.1),
//                     TimestampResolution::Millisecond,
//                 ),
//                 false,
//             );
//
//             let trade: Trade = (&event).into();
//
//             if let Some(trade_candle) = aggregator.update(&trade) {
//                 let ohlcv_record = OHLCVRecord::from_event_and_candle(event, trade_candle, self.period.as_ref());
//                 Some(ohlcv_record)
//             } else {
//                 // debug!("[Worker-{id}] Failed to update aggregator for trade: {event:?}");
//                 None
//             }
//         } else {
//             None
//         }
//     }
// }
