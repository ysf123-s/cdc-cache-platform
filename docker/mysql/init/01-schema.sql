-- 首次启动 MySQL 容器时自动执行。
-- 与 docs/demo-users.sql、docs/demo-departments.sql 保持一致。

CREATE DATABASE IF NOT EXISTS demo_db
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE demo_db;

CREATE TABLE IF NOT EXISTS users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(128),
    phone VARCHAR(32),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS departments (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    code VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS inventory (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(64) NOT NULL,
    available_qty INT NOT NULL DEFAULT 0,
    remark VARCHAR(255),
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO users (name, email, phone) VALUES
    ('Alice', 'alice@example.com', '10001'),
    ('Bob', 'bob@example.com', '10002');

INSERT INTO departments (name, code) VALUES
    ('Engineering', 'ENG'),
    ('Operations', 'OPS');

INSERT INTO inventory (sku, available_qty, remark) VALUES
    ('SKU-1001', 50, 'Initial warehouse stock');
