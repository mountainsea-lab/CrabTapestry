-- Your SQL goes here
-- ===============================================
-- Table: market_missing_range
-- 存储每个市场缺口时间段
-- ===============================================
CREATE TABLE `market_missing_range` (
                                        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',

                                        `market_id` BIGINT UNSIGNED NOT NULL COMMENT '关联 market_backfill_meta.id',
                                        `start_ts` DATETIME NOT NULL COMMENT '缺口开始时间',
                                        `end_ts` DATETIME NOT NULL COMMENT '缺口结束时间',

                                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',

                                        PRIMARY KEY (`id`),
                                        KEY `idx_market_id` (`market_id`),
                                        CONSTRAINT `fk_market_backfill` FOREIGN KEY (`market_id`) REFERENCES `market_backfill_meta`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每个市场缺口时间段';
