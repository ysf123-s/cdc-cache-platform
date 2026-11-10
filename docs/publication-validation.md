# 发布验证（2026-11-10）

- `mvn -o -B -DskipTests package`：通过，包含主代码与测试代码编译及可执行 JAR 打包。
- YAML 配置、Python 脚本语法及公开端口配置检查：通过。
- 本次未运行 Docker Compose，执行环境没有 Docker CLI。
- 本记录描述发布构建；功能与性能证据见相应验证目录。
- Gitleaks 8.30.1 对待发布文件扫描：0 项发现；已知本机凭据逐值检查：0 项命中。
