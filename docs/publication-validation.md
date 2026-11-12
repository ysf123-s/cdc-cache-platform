# 发布验证

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
