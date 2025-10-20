-- 1. 创建触发器函数
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 创建表
CREATE TABLE hmds_ohlcv_record (
    id BIGSERIAL PRIMARY KEY, -- 自增主键，用于表内唯一标识及外键关联
    hash_id BYTEA NOT NULL, -- symbol+exchange+period+ts 的 MD5 哈希值，用于幂等插入
    ts BIGINT NOT NULL, -- K 线结束时间 (UNIX 时间戳，可秒或毫秒)
    period_start_ts BIGINT, -- K 线开始时间 (可选 UNIX 时间戳)
    symbol VARCHAR(64) NOT NULL, -- 交易对 (例如 BTC/USDT)
    exchange VARCHAR(64) NOT NULL, -- 交易所名称 (例如 binance)
    period VARCHAR(16) NOT NULL, -- K 线周期 (例如 1m, 5m, 1h)
    open DOUBLE PRECISION NOT NULL, -- 开盘价
    high DOUBLE PRECISION NOT NULL, -- 最高价
    low DOUBLE PRECISION NOT NULL, -- 最低价
    close DOUBLE PRECISION NOT NULL, -- 收盘价
    volume DOUBLE PRECISION NOT NULL, -- 成交量
    turnover DOUBLE PRECISION, -- 成交额 (可选)
    num_trades INTEGER, -- 成交笔数 (可选)
    vwap DOUBLE PRECISION, -- 成交量加权价格 (可选)
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- 记录创建时间
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- 记录更新时间
    CONSTRAINT uq_hash_id UNIQUE (hash_id), -- 唯一索引 hash_id
    CONSTRAINT idx_symbol_period_ts UNIQUE (symbol, exchange, period, ts) -- 复合索引
);

-- 3. 创建触发器，确保 updated_at 每次更新时自动更新
CREATE TRIGGER update_hmds_ohlcv_record_updated_at
BEFORE UPDATE ON hmds_ohlcv_record
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
