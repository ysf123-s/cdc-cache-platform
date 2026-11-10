-- 为 demo 应用账号授予 cdc_platform 平台数据库操作权限
-- 在 Docker Compose MySQL 启动时自动执行，不污染 03-platform.sql 的 DDL 定义

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX ON cdc_platform.* TO 'demo'@'%';
-- 恢复基线读取 SHOW MASTER STATUS；业务库的 TRIGGER 可见性由 MYSQL_DATABASE 上的 ALL 授权提供。
GRANT REPLICATION CLIENT ON *.* TO 'demo'@'%';
FLUSH PRIVILEGES;
