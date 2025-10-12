#[macro_export]
macro_rules! define_strategy_bundle {
    ($name:ident, $num:ty, $cb:ty, $cs:ty, $series:ty, $tradingrec:ty, $entry:ty, $exit:ty) => {
        pub struct $name {
            pub strategy: Arc<BaseStrategy<$num, $cb, $cs, $series, $tradingrec, $entry, $exit>>,
            pub series: Arc<$series>,
            // 可加其他字段
        }

        impl StrategyBundleTypes for $name {
            type Num = $num;
            type CostBuy = $cb;
            type CostSell = $cs;
            type Series = $series;
            type TradingRec = $tradingrec;
        }

        impl StrategyBundle for $name {
            type EntryRule = $entry;
            type ExitRule = $exit;

            fn strategy_arc(&self) -> Arc<BaseStrategy<$num, $cb, $cs, $series, $tradingrec, $entry, $exit>> {
                self.strategy.clone()
            }

            fn name(&self) -> &'static str {
                stringify!($name)
            }

            fn indicators_for_viz(&self) -> Vec<Arc<dyn IndicatorAny>> {
                Vec::new()
            }

            fn series(&self) -> Arc<Self::Series> {
                self.series.clone()
            }

            fn series_len(&self) -> usize {
                self.series().get_bar_count()
            }

            fn params(&self) -> Option<HashMap<String, String>> {
                None
            }

            fn raw_indicators(&self) -> Option<Vec<Arc<dyn Any>>> {
                None
            }
        }
    };
}
