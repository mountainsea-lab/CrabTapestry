-- Your SQL goes here
-- ===============================================
-- Table: hmds_market_fill_range
-- 市场数据区间维护任务信息表
-- ===============================================
CREATE TABLE `hmds_market_fill_range` (
                                          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',

                                          `exchange` VARCHAR(50) NOT NULL COMMENT '交易所名称，例如 binance',
                                          `symbol` VARCHAR(50) NOT NULL COMMENT '交易对本币，例如 BTC',
                                          `quote` VARCHAR(50) NOT NULL COMMENT '交易对结算币种，例如 USDT',
                                          `period` VARCHAR(10) NOT NULL COMMENT 'K线周期，例如 1m, 5m, 1h',

                                          `start_time` BIGINT NOT NULL COMMENT '数据区间起始时间戳（毫秒）',
                                          `end_time` BIGINT NOT NULL COMMENT '数据区间结束时间戳（毫秒）',

                                          `status` TINYINT NOT NULL DEFAULT 0 COMMENT '同步状态，0: 未同步, 1: 同步中, 2: 已同步, 3: 同步失败',
                                          `retry_count` INT NOT NULL DEFAULT 0 COMMENT '重试次数',
                                          `batch_size` INT NOT NULL DEFAULT 0 COMMENT '每批次最大数据量',
                                          `last_try_time` TIMESTAMP DEFAULT NULL COMMENT '最后一次尝试同步时间',

                                          `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                          `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',

                                          PRIMARY KEY (`id`),
                                          UNIQUE KEY `uniq_market_period_range` (`exchange`, `symbol`, `period`, `start_time`, `end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='市场数据区间维护任务信息表';

-- 为 exchange, symbol, period 创建联合索引
CREATE INDEX idx_exchange_symbol_period ON hmds_market_fill_range (exchange, symbol, period);

-- 为 status, retry_count 创建联合索引
CREATE INDEX idx_status_retry_count ON hmds_market_fill_range (status, retry_count);

-- 为 start_time 创建索引
CREATE INDEX idx_start_time ON hmds_market_fill_range (start_time);

-- 为 end_time 创建索引
CREATE INDEX idx_end_time ON hmds_market_fill_range (end_time);

-- 为 start_time 和 end_time 创建复合索引
CREATE INDEX idx_time_range ON hmds_market_fill_range (start_time, end_time);
