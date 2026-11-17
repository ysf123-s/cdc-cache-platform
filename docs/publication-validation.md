# 发布验证

## 2026-11-17：恢复 SQL 类型扩展

- 补齐 BIT、TIMESTAMP、FLOAT/DOUBLE、JSON、ENUM/SET、YEAR、TIME、BINARY/VARBINARY/BLOB 的值生成及当前行比较。BIT(64) 按 Canal 的有符号 long 表示还原位模式；ENUM/SET 使用 Canal 记录的编号。
- TIMESTAMP 根据 `platform.binlog-time-zone` 解读，输出带 UTC 偏移的值，保护条件按 Unix 时间比较；恢复普通字段时显式保留时间值，避免 ON UPDATE 自动刷新。
- TIME 直接读取完整字符值，避免 TIME_TO_SEC 在云端版本中截断小数秒。TINYINT(1) 按数据库实际列类型识别，避免 JDBC 布尔转换丢失数值。
- Instabox 独立 MySQL 容器执行 `RecoveryMysqlTest`：29 项通过、0 失败、0 跳过。覆盖新类型的 UPDATE、INSERT、DELETE 逆向 SQL 实际执行，以及三种会话时区和后续写入保护。
- 保留已有外键事件限制、完整镜像、基线、当前结构和当前数据检查；生成列、触发器与空间类型仍有明确限制。

## 2026-11-17：普通字段恢复支持外键表

- 结构快照记录本表引用列和被引用列，基线不再因为存在外键就拒绝整张表。
- UPDATE 只修改普通字段时允许生成恢复 SQL；修改外键关联列及外键表的 INSERT/DELETE 恢复继续拒绝，保留当前行预检查和 SQL 条件保护。
- 新增双向外键、复合外键、被引用的非主键列、NULL 转换和旧快照序列化测试。
- Instabox 独立 MySQL 容器执行恢复测试：8 项通过，0 失败、0 跳过，包括双向外键下普通字段恢复 SQL 的实际执行。
- 本次发布保留仓库已有 Testcontainers 1.21.4 和 MySQLContainer JDBC 就绪检查。
- 本次发布副本执行 `mvn -o -B verify` 通过：246 项用例中 218 项通过、28 项按环境条件跳过，0 失败、0 错误。

## 2026-11-12：CI 修复

- `mvn -o -B -Dtest=CanalBatchRecoveryTest,SyncStatusTest test`：13 项通过，0 失败、0 错误、0 跳过。
- `mvn -o -B verify`：构建通过；237 项用例中 210 项通过，27 项跳过，0 失败、0 错误。
- 本机没有 Docker，跳过 10 项容器测试；另外 17 项外部 Redis 测试需要显式提供 `cache.test.redis.host`。
- `mvn -o -B dependency:tree -Dincludes=org.testcontainers:*,com.github.docker-java:*`：Testcontainers 各模块均解析为 1.21.4，docker-java 解析为 3.4.2。
- [GitHub Actions 修复分支验证](https://github.com/ysf123-s/cdc-cache-platform/actions/runs/43220769803)：`mvn -B verify` 通过；239 项用例中 222 项通过，17 项外部 Redis 测试按配置跳过，0 失败、0 错误。
- 云端环境为 Docker Engine 29.4.2；6 项 Redis 容器测试和 6 项 MySQL 恢复测试全部执行并通过。MySQL 使用 `MySQLContainer` 的 JDBC 就绪检查。

## 2026-11-10：首次发布

- `mvn -o -B -DskipTests package`：通过，包含主代码与测试代码编译及可执行 JAR 打包。
- YAML 配置、Python 脚本语法及公开端口配置检查：通过。
- 本次未运行 Docker Compose，执行环境没有 Docker CLI。
- 本记录描述发布构建；功能与性能证据见相应验证目录。
- Gitleaks 8.30.1 对待发布文件扫描：0 项发现；已知本机凭据逐值检查：0 项命中。
