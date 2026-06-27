默认使用中文回答和写代码注释！
总是使用github工具(get_file_contents、search_code、search_repositories等)获取外部库的文档和代码，防止因为过时的记忆而导致的错误。

## 项目概述

单文件 Rust 项目（`src/main.rs`，~920 行），通过 rclone RC API 管理 Bilibili 录播文件的云存储上传和本地归档。使用 tokio-cron-scheduler 做定时调度。

## 构建与运行

```bash
cargo build --release
# 运行需要 .env 或环境变量，参考 .env.example
RUST_LOG=info ./target/release/bilirec-script
```

- 无测试套件、无 clippy/rustfmt 配置、无 CI lint 步骤
- CI（`.github/workflows/build-docker.yml`）仅构建 Docker 镜像并推送到 DockerHub
- Docker 构建基于 `rust:1.95-alpine` + musl，产物为静态链接二进制

## 关键架构

- 所有代码在 `src/main.rs`，无模块拆分
- 环境变量通过 `dotenvy` 从 `.env` 加载，使用 `LazyLock` 惰性初始化
- 核心依赖：`reqwest`（HTTP）、`tokio`（异步运行时）、`serde_json`（rclone API 交互）
- rclone RC API 是唯一的外部服务依赖，所有文件操作（list/move/copy/mkdir/du/about）都通过 HTTP 调用 rclone

## 环境变量

必需：`REC_BASE_DIR`、`ARCHIVE_BASE_DIR`、`CLOUD_FS`、`CLOUD_BASE_DIR`、`RCLONE_BASE_URL`
可选：`DAILY_UPLOAD_LIMIT`(50GB)、`ARCHIVE_THRESHOLD`(50GB)、`MIN_REMOTE_FREE_SPACE`(20GB)、`ENABLE_CLOUD_UPLOAD`(true)、`EXIT_ON_ERROR`(true)

- `EXIT_ON_ERROR=true` 时任何错误都会 `process::exit(1)`，改代码时注意错误处理路径

## 部署

- `compose.yml` 被 gitignore，用户自定义；`compose.example.yml` 是模板
- 部署包含两个服务：`rclone`（rclone rcd 守护进程）和 `bilirec_script`（本项目）
- rclone RC API 带 basic auth，URL 格式：`http://user:pass@host:port`

## 业务逻辑

- `move_small_files()`：将小文件（<1MB FLV + <20KB XML）移到 `smallfile/` 子目录
- `upload_and_move()`：上传文件到云端，然后移到本地 `uploaded/{month}月/` 目录
- `archive_uploaded_files()`：磁盘空间不足时将 uploaded 目录整体归档
- 文件名格式含日期段（如 `xxx-20240101-xxx.flv`），用于解析月份归档
- ModTime < 0.5 小时的文件会被跳过（正在录制中）
- 上传/归档有原子锁（`RunningGuard`），防止定时任务并发执行

## 编辑注意事项

- Rust edition 2021，最低支持 rustc 1.80+（使用 `LazyLock`）
- 添加新环境变量时：在 `main()` 的 `LazyLock::force` 块中添加初始化，确保启动时校验
- rclone API 响应结构定义在文件顶部数据模型区，字段使用 `camelCase` 或 `PascalCase` 反序列化
- 进度条在非 TTY 环境自动隐藏（`is_terminal()` 检查）
