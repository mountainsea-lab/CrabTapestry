-- Your SQL goes here
-- ===============================================
-- Table: market_backfill_meta
-- 每个市场历史数据回补元信息
-- ===============================================
CREATE TABLE `market_backfill_meta` (
                                        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',

                                        `exchange` VARCHAR(64) NOT NULL COMMENT '交易所名称，例如 binance',
                                        `symbol` VARCHAR(64) NOT NULL COMMENT '交易对，例如 BTC/USDT',
                                        `interval` VARCHAR(16) NOT NULL COMMENT 'K线周期，例如 1m, 5m, 1h',

                                        `last_filled` DATETIME NULL COMMENT '最后一次成功回补到的时间（含）',
                                        `last_checked` DATETIME NULL COMMENT '最近一次完整维护检查时间',

                                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',

                                        PRIMARY KEY (`id`),
                                        UNIQUE KEY `uniq_market` (`exchange`, `symbol`, `interval`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每个市场历史数据回补元信息';
