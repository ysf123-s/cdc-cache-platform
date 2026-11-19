# 发布验证

## 2026-11-19：缓存失效链路加固

- 无法处理的事件（Entry 无法解析、FlatMessage 类型未知、前后镜像都缺主键）写入 Redis Stream `canal-cache:dead-letters`（约保留 1 万条）后跳过，同批其他事件照常处理。Redis 等外部故障以及死信写入失败仍整批回滚重试，重试间隔从 1s 倍增至 30s。
- 后镜像缺主键时改用前镜像，且只删除不刷新；刷新时行数据无法转换为实体则改为删除。
- 已登记表的 TRUNCATE、DROP、RENAME 事件用 SCAN + UNLINK 清空该表的主键与索引缓存命名空间。新增 `POST /cache/groups/{table}/purge` 和 `GET /cache/dead-letters`；缓存控制台显示死信并可清空分组。
- `@AutoCache.indexKeys` 声明二级索引缓存。users 表登记 `user-email:#email`，按修改前后的列值失效；`GET /users/by-email` 回填同样经过加载令牌校验，返回前核对邮箱。
- 新增指标 `canal.events.dead_lettered`、`canal.cache.purged`、`canal.cache.refresh.fallback`、`canal.cache.invalidation.lag`。
- 缓存控制台一并带上此前的可读性改造，使用共享的 `dashboard-ui.js` 与 `dashboard-ui.css`；平台页面不变。
- 本次发布副本执行 `mvn -o -B verify` 通过：306 项用例中 266 项通过、40 项按环境条件跳过，0 失败、0 错误。
- 使用云端 Redis 执行 `-Dtest=*RedisTest`：23 项通过（含 4 项二级索引测试），0 失败、0 跳过；测试只使用随机前缀 Key。
- `node src/test/js/cache-dashboard.test.cjs` 与 `platform-dashboard.test.cjs` 通过。
- Gitleaks 8.30.1 对暂存文件扫描：0 项发现；已知本机凭据逐值检查：0 项命中。
- 尚未在真实 Canal 链路上端到端验证 RENAME 与 TRUNCATE。
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
