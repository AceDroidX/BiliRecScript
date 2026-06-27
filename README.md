# BiliRecScript

Bilibili 录播文件上传与归档管理工具（Rust 复刻版）

通过 rclone RC API 管理录播文件的云存储上传和本地归档。

## 功能

- 定时上传录播文件到云存储（默认每小时）
- 自动将小文件（<1MB FLV + <20KB XML）移动到 smallfile 目录
- 每日上传限额控制（默认 50GB）
- 磁盘空间不足时自动归档已上传文件

## 环境变量

| 变量 | 必需 | 默认值 | 说明 |
|---|---|---|---|
| `REC_BASE_DIR` | 是 | - | 录播文件目录 |
| `ARCHIVE_BASE_DIR` | 是 | - | 归档目录 |
| `CLOUD_FS` | 是 | - | rclone 远程名称（如 `od:`） |
| `CLOUD_BASE_DIR` | 是 | - | 云端目录路径 |
| `RCLONE_BASE_URL` | 是 | - | rclone RC API 地址 |
| `DAILY_UPLOAD_LIMIT` | 否 | 50 | 每日上传限额（GB） |
| `ARCHIVE_THRESHOLD` | 否 | 50 | 归档阈值（GB） |
| `MIN_REMOTE_FREE_SPACE` | 否 | 20 | 云存储最小剩余空间（GB），低于此值时停止上传 |
| `ENABLE_CLOUD_UPLOAD` | 否 | true | 是否启用云上传 |

## 构建与运行

```bash
cargo build --release
RUST_LOG=info ./target/release/bilirec-script
```

## Docker

```bash
docker compose up -d
```
