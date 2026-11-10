-- 可选缺货示例：按需手动执行，不随平台默认初始化。
USE demo_db;
CREATE TABLE IF NOT EXISTS stockout_task (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    inventory_id BIGINT NOT NULL,
    sku VARCHAR(64) NOT NULL,
    event_time_ms BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    handled_at DATETIME(3) NULL,
    UNIQUE KEY uk_stockout_event (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
