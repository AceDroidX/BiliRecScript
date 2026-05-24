use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::LazyLock;

#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio::time::{sleep, Duration};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

// ============================================================
// 环境变量（惰性初始化）
// ============================================================
static REC_BASE_DIR: LazyLock<String> = LazyLock::new(|| {
    std::env::var("REC_BASE_DIR").expect("环境变量 REC_BASE_DIR 未设置")
});
static ARCHIVE_BASE_DIR: LazyLock<String> = LazyLock::new(|| {
    std::env::var("ARCHIVE_BASE_DIR").expect("环境变量 ARCHIVE_BASE_DIR 未设置")
});
static CLOUD_FS: LazyLock<String> = LazyLock::new(|| {
    std::env::var("CLOUD_FS").expect("环境变量 CLOUD_FS 未设置")
});
static CLOUD_BASE_DIR: LazyLock<String> = LazyLock::new(|| {
    std::env::var("CLOUD_BASE_DIR").expect("环境变量 CLOUD_BASE_DIR 未设置")
});
static RCLONE_BASE_URL: LazyLock<String> = LazyLock::new(|| {
    std::env::var("RCLONE_BASE_URL").expect("环境变量 RCLONE_BASE_URL 未设置")
});

/// 每日上传限额（GB）
static DAILY_UPLOAD_LIMIT: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("DAILY_UPLOAD_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
});
/// 归档阈值（GB）
static ARCHIVE_THRESHOLD: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("ARCHIVE_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
});
/// 出错时是否退出程序（默认 true）
static EXIT_ON_ERROR: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("EXIT_ON_ERROR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(true)
});

// ============================================================
// 常量
// ============================================================
static DAILY_UPLOAD_LIMIT_BYTES: LazyLock<u64> = LazyLock::new(|| {
    *DAILY_UPLOAD_LIMIT * 1024 * 1024 * 1024
});
static ARCHIVE_THRESHOLD_BYTES: LazyLock<u64> = LazyLock::new(|| {
    *ARCHIVE_THRESHOLD * 1024 * 1024 * 1024
});
const SMALL_FILE_SUBDIR: &str = "smallfile";
const UPLOADED_SUBDIR: &str = "uploaded";
const FLV_SIZE_LIMIT: u64 = 1024 * 1024; // 1MB
const XML_SIZE_LIMIT: u64 = 20 * 1024; // 20KB

/// 全局内存计数：今日已上传字节数
static UPLOADED_TODAY: AtomicU64 = AtomicU64::new(0);

/// 上传/归档运行锁，防止定时任务并发执行
static UPLOAD_RUNNING: AtomicBool = AtomicBool::new(false);
static ARCHIVE_RUNNING: AtomicBool = AtomicBool::new(false);

/// 共享 HTTP 客户端（复用连接池）
static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .expect("创建 HTTP 客户端失败")
});

/// Drop 守卫，确保 panic 时也释放原子锁
struct RunningGuard(&'static AtomicBool);

impl RunningGuard {
    fn acquire(flag: &'static AtomicBool) -> Option<Self> {
        if flag.swap(true, Ordering::Acquire) {
            None
        } else {
            Some(Self(flag))
        }
    }
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// ============================================================
// 数据模型（rclone RC API 响应）
// ============================================================

#[derive(Debug, Deserialize)]
struct CopyFileResponse {
    jobid: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobStatusResponse {
    duration: f64,
    end_time: String,
    error: String,
    finished: bool,
    group: String,
    id: i64,
    output: Option<serde_json::Value>,
    start_time: String,
    success: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferringInfo {
    bytes: i64,
    dst_fs: String,
    eta: Option<i64>,
    group: String,
    name: String,
    percentage: Option<i64>,
    size: i64,
    speed: Option<f64>,
    speed_avg: Option<f64>,
    src_fs: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CoreStatsResponse {
    bytes: i64,
    checks: i64,
    deleted_dirs: i64,
    deletes: i64,
    elapsed_time: f64,
    errors: i64,
    eta: Option<i64>,
    fatal_error: bool,
    listed: Option<i64>,
    renames: i64,
    retry_error: bool,
    server_side_copies: i64,
    server_side_copy_bytes: i64,
    server_side_move_bytes: i64,
    server_side_moves: i64,
    speed: f64,
    total_bytes: i64,
    total_checks: i64,
    total_transfers: i64,
    transfer_time: f64,
    transferring: Option<Vec<TransferringInfo>>,
    transfers: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct FileInfo {
    path: String,
    name: String,
    size: i64,
    mime_type: String,
    mod_time: String,
    is_dir: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DuInfo {
    available: i64,
    free: i64,
    total: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct DuResponse {
    dir: String,
    info: DuInfo,
}

// ============================================================
// 工具函数
// ============================================================

fn bytes_to_gb_str(b: u64) -> String {
    format!("{:.2} GB", b as f64 / (1024.0 * 1024.0 * 1024.0))
}

fn get_file_info<'a>(files: &'a [FileInfo], name: &str) -> Option<&'a FileInfo> {
    files.iter().find(|f| f.name == name)
}

fn should_skip_recent_file(mod_time_str: &str, now: &DateTime<Utc>, hours: f64) -> Result<bool> {
    let mod_time = DateTime::parse_from_rfc3339(mod_time_str)
        .map_err(|e| anyhow!("解析ModTime失败: {}, 错误: {}", mod_time_str, e))?;
    let delta = *now - mod_time.with_timezone(&Utc);
    Ok(delta.num_seconds() < (hours * 3600.0) as i64)
}

fn parse_month_from_filename(filename: &str) -> Result<i32> {
    let parts: Vec<&str> = filename.split('-').collect();
    if parts.len() < 3 {
        return Err(anyhow!("文件名格式不正确: {}", filename));
    }
    let date_str = parts[2];
    if date_str.len() != 8 || !date_str.chars().all(|c| c.is_ascii_digit()) {
        return Err(anyhow!("日期段格式不正确: {}", date_str));
    }
    let month: i32 = date_str[4..6]
        .parse()
        .map_err(|_| anyhow!("月份解析失败: {}", &date_str[4..6]))?;
    Ok(month)
}

/// 创建默认的 _config 配置（用于 movefile / copyfile / sync/move）
fn default_rclone_config() -> serde_json::Value {
    json!({
        "CheckFirst": true,
        "Metadata": true,
        "PartialSuffix": ".partial",
    })
}

/// 判断 HTTP 响应是否为 rclone 的成功响应
fn is_rclone_success(status: reqwest::StatusCode, body: &serde_json::Value) -> bool {
    status.is_success()
        && body
            .get("error")
            .and_then(|v| v.as_str())
            .is_none_or(|s| s.is_empty())
}

/// 从 rclone 错误响应中提取错误消息
fn extract_rclone_error(status: reqwest::StatusCode, body: &serde_json::Value) -> String {
    if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        err.to_string()
    } else {
        format!("未知错误，状态码: {}, 响应: {}", status, body)
    }
}

// ============================================================
// Rclone RC API 调用
// ============================================================

async fn list_files(client: &Client, base_dir: &str) -> Result<Vec<FileInfo>> {
    let url = format!("{}/operations/list", *RCLONE_BASE_URL);
    let payload = json!({"fs": "/", "remote": base_dir});
    let resp = client.post(&url).json(&payload).send().await?;
    let data: serde_json::Value = resp.json().await?;
    let list = data
        .get("list")
        .and_then(|v| v.as_array())
        .context("list_files: 响应缺少 'list' 字段")?;
    let files: Vec<FileInfo> = serde_json::from_value(json!(list))
        .context("list_files: 解析 FileInfo 失败")?;
    Ok(files)
}

async fn disk_usage(client: &Client, dir: Option<&str>) -> Result<DuResponse> {
    let url = format!("{}/core/du", *RCLONE_BASE_URL);
    let payload = match dir {
        Some(d) => json!({"dir": d}),
        None => json!({}),
    };
    let resp = client.post(&url).json(&payload).send().await?;
    let data: serde_json::Value = resp.json().await?;
    let du: DuResponse =
        serde_json::from_value(data.clone()).with_context(|| {
            format!(
                "disk_usage: 响应解析为 DuResponse 失败, data: {}",
                data
            )
        })?;
    Ok(du)
}

async fn move_file(client: &Client, src_path: &str, dst_path: &str) -> Result<()> {
    let url = format!("{}/operations/movefile", *RCLONE_BASE_URL);
    let payload = json!({
        "srcFs": "/",
        "srcRemote": src_path,
        "dstFs": "/",
        "dstRemote": dst_path,
        "_config": default_rclone_config(),
    });
    let resp = client.post(&url).json(&payload).send().await?;
    let status = resp.status();
    let data: serde_json::Value = resp.json().await?;

    if is_rclone_success(status, &data) {
        info!("move_file: {} -> {} 移动成功", src_path, dst_path);
        return Ok(());
    }
    Err(anyhow!("move_file error: {}", extract_rclone_error(status, &data)))
}

async fn create_dir(client: &Client, fs: &str, dir_path: &str) -> Result<()> {
    let url = format!("{}/operations/mkdir", *RCLONE_BASE_URL);
    let payload = json!({"fs": fs, "remote": dir_path});
    let resp = client.post(&url).json(&payload).send().await?;
    let status = resp.status();
    let data: serde_json::Value = resp.json().await?;

    if is_rclone_success(status, &data) {
        return Ok(());
    }
    Err(anyhow!("create_dir error: {}", extract_rclone_error(status, &data)))
}

async fn copy_file(
    client: &Client,
    src_fs: &str,
    src_path: &str,
    dst_fs: &str,
    dst_path: &str,
    check_interval: u64,
) -> Result<()> {
    let url = format!("{}/operations/copyfile", *RCLONE_BASE_URL);
    let payload = json!({
        "srcFs": src_fs,
        "srcRemote": src_path,
        "dstFs": dst_fs,
        "dstRemote": dst_path,
        "_config": default_rclone_config(),
        "_async": true,
    });
    info!("copy_file: 开始任务 src={}, dst={}", src_path, dst_path);

    let resp = client.post(&url).json(&payload).send().await?;
    let data: serde_json::Value = resp.json().await?;
    let copy_resp: CopyFileResponse = serde_json::from_value(data.clone())
        .with_context(|| format!("copy_file: copyfile响应解析失败, data: {}", data))?;
    let jobid = copy_resp.jobid;

    let file_name = src_path.split('/').next_back().unwrap_or(src_path);
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40}] {bytes}/{total_bytes} ({eta})")
            .expect("无效的 indicatif 模板")
            .progress_chars("=> "),
    );
    pb.set_message(file_name.to_string());

    if !std::io::stdout().is_terminal() {
        pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }

    loop {
        // 查询任务状态
        let status_url = format!("{}/job/status", *RCLONE_BASE_URL);
        let status_payload = json!({"jobid": jobid.to_string()});
        let status_resp = client.post(&status_url).json(&status_payload).send().await?;
        let status_data: serde_json::Value = status_resp.json().await?;
        let job_status: JobStatusResponse = serde_json::from_value(status_data.clone())
            .with_context(|| format!("copy_file: job/status响应解析失败, data: {}", status_data))?;

        let finished = job_status.finished;
        let success = job_status.success;
        let error = job_status.error;

        // 查询进度
        let stats_url = format!("{}/core/stats", *RCLONE_BASE_URL);
        let stats_payload = json!({"group": format!("job/{}", jobid)});
        let stats_resp = client.post(&stats_url).json(&stats_payload).send().await?;
        let stats_data: serde_json::Value = stats_resp.json().await?;
        let stats: CoreStatsResponse = serde_json::from_value(stats_data.clone())
            .with_context(|| format!("copy_file: core/stats响应解析失败, data: {}", stats_data))?;

        let transferring = stats.transferring.unwrap_or_default();
        if let Some(progress) = transferring.first() {
            let total = progress.size.max(1) as u64;
            let current = progress.bytes.max(0) as u64;
            pb.set_length(total);
            pb.set_position(current);
        } else {
            pb.set_length(1);
            pb.set_position(0);
        }

        if finished {
            pb.finish_and_clear();
            if !success {
                return Err(anyhow!("copy_file: 任务失败, error: {}", error));
            }
            info!(
                "copy_file: 任务完成, jobid={}, src={}, dst={}",
                jobid, src_path, dst_path
            );
            return Ok(());
        }

        sleep(Duration::from_secs(check_interval)).await;
    }
}

async fn move_dir(client: &Client, src_dir: &str, dst_dir: &str) -> Result<()> {
    let url = format!("{}/sync/move", *RCLONE_BASE_URL);
    let payload = json!({
        "srcFs": src_dir,
        "dstFs": dst_dir,
        "_config": default_rclone_config(),
        "_async": true,
    });
    info!("move_dir: 启动移动 src={} dst={}", src_dir, dst_dir);

    let resp = client.post(&url).json(&payload).send().await?;
    let data: serde_json::Value = resp.json().await?;
    let copy_resp: CopyFileResponse = serde_json::from_value(data.clone())
        .with_context(|| format!("move_dir: sync/move 响应解析失败, data: {}", data))?;
    let jobid = copy_resp.jobid;

    // 取源目录的 basename 作为进度条描述
    let dir_name = src_dir.split('/').next_back().unwrap_or(src_dir);
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40}] {bytes}/{total_bytes} ({eta})")
            .expect("无效的 indicatif 模板")
            .progress_chars("=> "),
    );
    pb.set_message(format!("move_dir:{}", dir_name));

    if !std::io::stdout().is_terminal() {
        pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }

    loop {
        // 查询任务状态
        let status_url = format!("{}/job/status", *RCLONE_BASE_URL);
        let status_payload = json!({"jobid": jobid.to_string()});
        let status_resp = client.post(&status_url).json(&status_payload).send().await?;
        let status_data: serde_json::Value = status_resp.json().await?;
        let job_status: JobStatusResponse = serde_json::from_value(status_data.clone())
            .with_context(|| format!("move_dir: job/status 响应解析失败, data: {}", status_data))?;

        let finished = job_status.finished;
        let success = job_status.success;
        let error = job_status.error;

        // 聚合 core/stats 中属于该 job 的 transferring 信息
        let stats_url = format!("{}/core/stats", *RCLONE_BASE_URL);
        let stats_payload = json!({"group": format!("job/{}", jobid)});
        let stats_resp = client.post(&stats_url).json(&stats_payload).send().await?;
        let stats_data: serde_json::Value = stats_resp.json().await?;
        let stats: CoreStatsResponse = serde_json::from_value(stats_data.clone())
            .with_context(|| format!("move_dir: core/stats 响应解析失败, data: {}", stats_data))?;

        let transferring = stats.transferring.unwrap_or_default();

        let mut total_size: u64 = 0;
        let mut total_bytes: u64 = 0;
        for t in &transferring {
            total_size = total_size.saturating_add(t.size.max(0) as u64);
            total_bytes = total_bytes.saturating_add(t.bytes.max(0) as u64);
        }

        if total_size > 0 {
            pb.set_length(total_size);
            pb.set_position(total_bytes.min(total_size));
        }

        if finished {
            pb.finish_and_clear();
            if !success {
                return Err(anyhow!("move_dir: 任务失败, error: {}", error));
            }
            info!(
                "move_dir: 任务完成, jobid={}, src={}, dst={}",
                jobid, src_dir, dst_dir
            );
            return Ok(());
        }

        sleep(Duration::from_secs(1)).await;
    }
}

// ============================================================
// 业务逻辑函数
// ============================================================

async fn move_small_files() -> Result<()> {
    let client = &*CLIENT;
    let files = match list_files(client, &REC_BASE_DIR).await {
        Ok(f) => f,
        Err(e) => {
            error!("move_small_files: 列出文件失败: {:?}", e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            return Ok(());
        }
    };
    let now = Utc::now();

    for flv_info in &files {
        if flv_info.is_dir {
            continue;
        }
        let filename = &flv_info.name;
        if !filename.ends_with(".flv") {
            continue;
        }
        let Some(stem) = filename.strip_suffix(".flv") else { continue };
        let xml_filename = format!("{}.xml", stem);
        let Some(xml_info) = get_file_info(&files, &xml_filename) else {
            continue;
        };
        if xml_info.is_dir {
            continue;
        }

        // 跳过ModTime距离现在时间不到0.5小时的flv
        match should_skip_recent_file(&flv_info.mod_time, &now, 0.5) {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                error!("move_small_files: 解析文件时间失败 ({}): {:?}", filename, e);
                if *EXIT_ON_ERROR { std::process::exit(1); }
                continue;
            }
        }
        if (flv_info.size.max(0) as u64) >= FLV_SIZE_LIMIT {
            continue;
        }
        if (xml_info.size.max(0) as u64) >= XML_SIZE_LIMIT {
            continue;
        }

        let dest_dir = format!("{}/{}", *REC_BASE_DIR, SMALL_FILE_SUBDIR);
        // 检查目标目录是否存在
        let dest_dir_exists = files.iter().any(|f| f.path == dest_dir && f.is_dir);
        if !dest_dir_exists {
            if let Err(e) = create_dir(client, "/", &dest_dir).await {
                error!("move_small_files: 创建目录失败: {:?}", e);
                if *EXIT_ON_ERROR { std::process::exit(1); }
                continue;
            }
        }

        // 分别移动 FLV 和 XML，各自独立处理错误
        let flv_src = format!("{}/{}", *REC_BASE_DIR, filename);
        let flv_dst = format!("{}/{}", dest_dir, filename);
        if let Err(e) = move_file(client, &flv_src, &flv_dst).await {
            error!("move_small_files: 移动 FLV 失败 ({}): {:?}", filename, e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            continue;
        }
        let xml_src = format!("{}/{}", *REC_BASE_DIR, xml_filename);
        let xml_dst = format!("{}/{}", dest_dir, xml_filename);
        if let Err(e) = move_file(client, &xml_src, &xml_dst).await {
            error!("move_small_files: 移动 XML 失败 ({}): {:?}", xml_filename, e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            // FLV 已移动，记录错误但不回滚
        }
        info!("Moved: {} and {}", filename, xml_filename);
    }
    Ok(())
}

async fn upload_and_move() -> Result<()> {
    let uploaded_today = UPLOADED_TODAY.load(Ordering::Relaxed);
    info!(
        "今日已上传: {} / 限额: {}",
        bytes_to_gb_str(uploaded_today),
        bytes_to_gb_str(*DAILY_UPLOAD_LIMIT_BYTES)
    );

    let client = &*CLIENT;
    let files = match list_files(client, &REC_BASE_DIR).await {
        Ok(f) => f,
        Err(e) => {
            error!("upload_and_move: 列出文件失败: {:?}", e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            return Ok(());
        }
    };
    let now = Utc::now();

    for file in &files {
        if !(file.name.ends_with(".flv") || file.name.ends_with(".xml") || file.name.ends_with(".txt")) {
            continue;
        }
        // 跳过ModTime距离现在时间不到0.5小时的文件
        match should_skip_recent_file(&file.mod_time, &now, 0.5) {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                error!("upload_and_move: 解析文件时间失败 ({}): {:?}", file.name, e);
                if *EXIT_ON_ERROR { std::process::exit(1); }
                continue;
            }
        }

        let file_size = file.size.max(0) as u64;
        let current_uploaded = UPLOADED_TODAY.load(Ordering::Relaxed);

        // 如果已达到或超过当日限额，跳过剩余上传
        if current_uploaded.saturating_add(file_size) >= *DAILY_UPLOAD_LIMIT_BYTES {
            info!(
                "将要达到今日上传限额，跳过剩余上传任务: 当前 {} + 文件 {} >= 限额 {}",
                bytes_to_gb_str(current_uploaded),
                bytes_to_gb_str(file_size),
                bytes_to_gb_str(*DAILY_UPLOAD_LIMIT_BYTES)
            );
            break;
        }

        let month = match parse_month_from_filename(&file.name) {
            Ok(m) => m,
            Err(e) => {
                error!("upload_and_move: 解析月份失败 ({}): {:?}", file.name, e);
                if *EXIT_ON_ERROR { std::process::exit(1); }
                continue;
            }
        };
        let month_dir = format!("{}月", month);
        let cloud_dir = format!("{}/{}", *CLOUD_BASE_DIR, month_dir);
        let uploaded_dir = format!("{}/{}/{}", *REC_BASE_DIR, UPLOADED_SUBDIR, month_dir);

        if let Err(e) = create_dir(client, &CLOUD_FS, &cloud_dir).await {
            error!("upload_and_move: 创建云端目录失败: {:?}", e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            continue;
        }
        if let Err(e) = create_dir(client, "/", &uploaded_dir).await {
            error!("upload_and_move: 创建本地 uploaded 目录失败: {:?}", e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            continue;
        }

        let cloud_path = format!("{}/{}", cloud_dir, file.name);
        let uploaded_path = format!("{}/{}", uploaded_dir, file.name);

        // 执行上传
        if let Err(e) = copy_file(client, "/", &file.path, &CLOUD_FS, &cloud_path, 1).await {
            error!("upload_and_move: 上传失败 ({}): {:?}", file.name, e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            continue;
        }

        UPLOADED_TODAY.fetch_add(file_size, Ordering::Relaxed);
        let new_uploaded = UPLOADED_TODAY.load(Ordering::Relaxed);
        info!(
            "上传后今日已上传: {} (新增 {})",
            bytes_to_gb_str(new_uploaded),
            bytes_to_gb_str(file_size)
        );

        // 移动本地文件到 uploaded 目录
        if let Err(e) = move_file(client, &file.path, &uploaded_path).await {
            error!("upload_and_move: 移动本地文件失败 ({} -> {}): {:?}", file.path, uploaded_path, e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
        }
    }
    Ok(())
}

async fn batch_upload_files() -> Result<()> {
    let Some(_guard) = RunningGuard::acquire(&UPLOAD_RUNNING) else {
        info!("batch_upload_files: 上一轮尚未完成，跳过本次执行");
        return Ok(());
    };
    if let Err(e) = move_small_files().await {
        error!("move_small_files 出错: {:?}", e);
        if *EXIT_ON_ERROR { std::process::exit(1); }
    }
    if let Err(e) = upload_and_move().await {
        error!("upload_and_move 出错: {:?}", e);
        if *EXIT_ON_ERROR { std::process::exit(1); }
    }
    Ok(())
}

async fn archive_uploaded_files() -> Result<()> {
    let Some(_guard) = RunningGuard::acquire(&ARCHIVE_RUNNING) else {
        info!("archive_uploaded_files: 上一轮尚未完成，跳过本次执行");
        return Ok(());
    };
    let uploaded_root = format!("{}/{}", *REC_BASE_DIR, UPLOADED_SUBDIR);
    let client = &*CLIENT;

    let du = match disk_usage(client, Some(&uploaded_root)).await {
        Ok(du) => du,
        Err(e) => {
            error!("archive_uploaded_files: 无法查询磁盘使用情况: {:?}", e);
            if *EXIT_ON_ERROR { std::process::exit(1); }
            return Ok(());
        }
    };

    let available = du.info.available.max(0) as u64;
    info!(
        "archive_uploaded_files: {} 可用空间 {}",
        uploaded_root,
        bytes_to_gb_str(available)
    );

    if available >= *ARCHIVE_THRESHOLD_BYTES {
        info!(
            "archive_uploaded_files: 可用空间 >= 50GB ({})，无需归档",
            bytes_to_gb_str(*ARCHIVE_THRESHOLD_BYTES)
        );
        return Ok(());
    }

    info!(
        "archive_uploaded_files: 准备移动 {} -> {}",
        uploaded_root, *ARCHIVE_BASE_DIR
    );
    if let Err(e) = move_dir(client, &uploaded_root, &ARCHIVE_BASE_DIR).await {
        error!("archive_uploaded_files: 移动 {} 失败: {:?}", uploaded_root, e);
        if *EXIT_ON_ERROR { std::process::exit(1); }
    }
    Ok(())
}

// ============================================================
// 主入口
// ============================================================

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("debug,hyper_util::client::legacy::pool=info")),
        )
        .init();

    // 初始化常量（触发 LazyLock 求值，及早发现缺失变量）
    let _ = LazyLock::force(&REC_BASE_DIR);
    let _ = LazyLock::force(&ARCHIVE_BASE_DIR);
    let _ = LazyLock::force(&CLOUD_FS);
    let _ = LazyLock::force(&CLOUD_BASE_DIR);
    let _ = LazyLock::force(&RCLONE_BASE_URL);
    let _ = LazyLock::force(&DAILY_UPLOAD_LIMIT);
    let _ = LazyLock::force(&ARCHIVE_THRESHOLD);
    let _ = LazyLock::force(&EXIT_ON_ERROR);

    let mut scheduler = JobScheduler::new().await?;

    // 每日零点重置任务
    let reset_job = Job::new_async("0 0 0 * * *", |_uuid, _lock| {
        Box::pin(async move {
            UPLOADED_TODAY.store(0, Ordering::Relaxed);
            info!("已重置今日上传计数为 0");
        })
    })?;
    scheduler.add(reset_job).await?;

    // 每小时批量上传任务 (每分钟的 0 秒执行)
    let upload_job = Job::new_async("0 0 * * * *", {
        move |_uuid, _lock| {
            Box::pin(async move {
                if let Err(e) = batch_upload_files().await {
                    error!("定时上传任务出错: {:?}", e);
                    if *EXIT_ON_ERROR { std::process::exit(1); }
                }
            })
        }
    })?;
    scheduler.add(upload_job).await?;

    // 每小时归档检查任务
    let archive_job = Job::new_async("0 0 * * * *", {
        move |_uuid, _lock| {
            Box::pin(async move {
                if let Err(e) = archive_uploaded_files().await {
                    error!("定时归档任务出错: {:?}", e);
                    if *EXIT_ON_ERROR { std::process::exit(1); }
                }
            })
        }
    })?;
    scheduler.add(archive_job).await?;

    scheduler.start().await?;
    info!("调度器已启动");

    // 启动后立即运行一次上传和归档任务（对应 Python 的 next_run_time=datetime.now()）
    if let Err(e) = batch_upload_files().await {
        error!("初始上传任务出错: {:?}", e);
        if *EXIT_ON_ERROR { std::process::exit(1); }
    }
    if let Err(e) = archive_uploaded_files().await {
        error!("初始归档任务出错: {:?}", e);
        if *EXIT_ON_ERROR { std::process::exit(1); }
    }

    // 等待退出信号 (SIGINT / SIGTERM)
    #[cfg(unix)]
    {
        let mut term_signal = signal(SignalKind::terminate())?;
        let mut int_signal = signal(SignalKind::interrupt())?;

        tokio::select! {
            _ = term_signal.recv() => {
                info!("收到 SIGTERM 信号，开始清理...");
            }
            _ = int_signal.recv() => {
                info!("收到 SIGINT 信号，开始清理...");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        info!("收到 Ctrl+C 信号，开始清理...");
    }

    scheduler.shutdown().await?;
    info!("调度器已停止");

    // 取消所有正在运行的异步任务
    // tokio::spawn 的任务会在主函数返回时自动清理

    info!("清理完成，退出");
    Ok(())
}
