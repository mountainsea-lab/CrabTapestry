-- Your SQL goes here
-- ===============================================
-- Table: crab_ohlcv_record
-- 低频 OHLCV 数据存储表
-- 对应 Rust 结构体 CrabOhlcvRecord
-- 设计特点：
--   1. 自增主键 id 便于聚簇索引和外键关联
--   2. hash_id (MD5 of symbol+exchange+period+ts) 唯一索引，用于幂等写入
--   3. symbol+exchange+period+ts 复合索引，用于范围查询
-- ===============================================
CREATE TABLE `hmds_ohlcv_record` (
                                 `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，用于表内唯一标识及外键关联',

                                 `hash_id` BINARY(16) NOT NULL COMMENT 'symbol+exchange+period+ts 的 MD5 哈希值，用于幂等插入',

                                 `ts` BIGINT NOT NULL COMMENT 'K 线结束时间 (UNIX 时间戳，可秒或毫秒)',
                                 `period_start_ts` BIGINT NULL COMMENT 'K 线开始时间 (可选 UNIX 时间戳)',

                                 `symbol` VARCHAR(64) NOT NULL COMMENT '交易对 (例如 BTC/USDT)',
                                 `exchange` VARCHAR(64) NOT NULL COMMENT '交易所名称 (例如 binance)',
                                 `period` VARCHAR(16) NOT NULL COMMENT 'K 线周期 (例如 1m, 5m, 1h)',

                                 `open` DOUBLE NOT NULL COMMENT '开盘价',
                                 `high` DOUBLE NOT NULL COMMENT '最高价',
                                 `low` DOUBLE NOT NULL COMMENT '最低价',
                                 `close` DOUBLE NOT NULL COMMENT '收盘价',
                                 `volume` DOUBLE NOT NULL COMMENT '成交量',

                                 `turnover` DOUBLE NULL COMMENT '成交额 (可选)',
                                 `num_trades` INT UNSIGNED NULL COMMENT '成交笔数 (可选)',
                                 `vwap` DOUBLE NULL COMMENT '成交量加权价格 (可选)',

                                 `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                 `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',

                                 PRIMARY KEY (`id`),
                                 UNIQUE KEY `uniq_hash_id` (`hash_id`),
                                 INDEX `idx_symbol_period_ts` (`symbol`, `exchange`, `period`, `ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='低频 OHLCV 数据表';
