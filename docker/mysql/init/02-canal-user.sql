-- Canal Server 订阅 binlog 需要 REPLICATION 权限。
-- MySQL 8 的 canal 客户端使用 mysql_native_password，避免 caching_sha2_password 握手失败。

CREATE USER IF NOT EXISTS 'canal'@'%' IDENTIFIED WITH mysql_native_password BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
