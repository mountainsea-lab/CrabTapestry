-- Your SQL goes here
-- ===============================================
-- Table: hmds_market_fill_range
-- 市场数据区间维护任务信息表
-- ===============================================
CREATE TABLE `hmds_market_fill_range` (
                                          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',

                                          `exchange` VARCHAR(50) NOT NULL COMMENT '交易所名称，例如 binance',
                                          `symbol` VARCHAR(50) NOT NULL COMMENT '交易对，例如 BTC/USDT',
                                          `period` VARCHAR(10) NOT NULL COMMENT 'K线周期，例如 1m, 5m, 1h',

                                          `start_time` BIGINT NOT NULL COMMENT '数据区间起始时间戳（毫秒）',
                                          `end_time` BIGINT NOT NULL COMMENT '数据区间结束时间戳（毫秒）',

                                          `status` TINYINT NOT NULL DEFAULT 0 COMMENT '同步状态，0: 未同步, 1: 同步中, 2: 已同步, 3: 同步失败',
                                          `retry_count` INT NOT NULL DEFAULT 0 COMMENT '重试次数',
                                          `last_try_time` TIMESTAMP DEFAULT NULL  COMMENT '最后一次尝试同步时间',

                                          `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                          `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',

                                          PRIMARY KEY (`id`),
                                          UNIQUE KEY `uniq_market_period_range` (`exchange`, `symbol`, `period`, `start_time`, `end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='市场数据区间维护任务信息表';
