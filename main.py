import aiohttp
import asyncio
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta, date
from tqdm.asyncio import tqdm
from dotenv import load_dotenv
import os
import logging
import signal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()

REC_BASE_DIR: str = os.getenv("REC_BASE_DIR") or (_ for _ in ()).throw(
    ValueError("环境变量 REC_BASE_DIR 未设置")
)
CLOUD_FS: str = os.getenv("CLOUD_FS") or (_ for _ in ()).throw(
    ValueError("环境变量 CLOUD_FS 未设置")
)
CLOUD_BASE_DIR: str = os.getenv("CLOUD_BASE_DIR") or (_ for _ in ()).throw(
    ValueError("环境变量 CLOUD_BASE_DIR 未设置")
)
RCLONE_BASE_URL: str = os.getenv("RCLONE_BASE_URL") or (_ for _ in ()).throw(
    ValueError("环境变量 RCLONE_BASE_URL 未设置")
)
DEST_SUBDIR: str = "smallfile"
FLV_SIZE_LIMIT: int = 1 * 1024 * 1024  # 1MB
XML_SIZE_LIMIT: int = 20 * 1024  # 20KB
DAILY_UPLOAD_LIMIT: int = 50 * 1024 * 1024 * 1024  # 50GB

# 全局内存计数
uploaded_today: int = 0

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s"
)


class RcloneError(BaseModel):
    error: str
    input: dict
    status: int
    path: str


# copyfile接口响应
class CopyFileResponse(BaseModel):
    jobid: int


# job/status接口响应
class JobStatusResponse(BaseModel):
    duration: float
    endTime: str
    error: str
    finished: bool
    group: str
    id: int
    output: Optional[dict]
    startTime: str
    success: bool


# core/stats接口响应
class TransferringInfo(BaseModel):
    bytes: int
    dstFs: str
    eta: Optional[int]
    group: str
    name: str
    percentage: Optional[int]
    size: int
    speed: Optional[float]
    speedAvg: Optional[float]
    srcFs: str


class CoreStatsResponse(BaseModel):
    bytes: int
    checks: int
    deletedDirs: int
    deletes: int
    elapsedTime: float
    errors: int
    eta: Optional[int]
    fatalError: bool
    listed: int
    renames: int
    retryError: bool
    serverSideCopies: int
    serverSideCopyBytes: int
    serverSideMoveBytes: int
    serverSideMoves: int
    speed: float
    totalBytes: int
    totalChecks: int
    totalTransfers: int
    transferTime: float
    transferring: Optional[List[TransferringInfo]] = None
    transfers: int


class FileInfo(BaseModel):
    """
    数据示例：
        {
            "Path": "/home/acedroidx/share/rec/21452505-七海Nana7mi/录制-21452505-20250821-230039-344-怎么还在中忍考试.flv",
            "Name": "录制-21452505-20250821-230039-344-怎么还在中忍考试.flv",
            "Size": 3642769082,
            "MimeType": "video/x-flv",
            "ModTime": "2025-08-22T00:00:39.507829615+08:00",
            "IsDir": false
        }
    """

    Path: str
    Name: str
    Size: int
    MimeType: str
    ModTime: str
    IsDir: bool


def bytes_to_mb_str(b: int) -> str:
    """将字节值格式化为 MB 字符串（保留两位小数）。"""
    return f"{b / (1024 * 1024):.2f} MB"


async def list_files(session: aiohttp.ClientSession, base_dir: str) -> List[FileInfo]:
    url = f"{RCLONE_BASE_URL}/operations/list"
    payload = {"fs": "/", "remote": base_dir}
    async with session.post(url, json=payload) as resp:
        data = await resp.json()
        return [FileInfo(**f) for f in data.get("list", [])]


async def move_file(
    session: aiohttp.ClientSession, src_path: str, dst_path: str
) -> bool:
    url = f"{RCLONE_BASE_URL}/operations/movefile"
    payload = {
        "srcFs": "/",
        "srcRemote": src_path,
        "dstFs": "/",
        "dstRemote": dst_path,
        "_config": {"CheckFirst": True, "Metadata": True, "PartialSuffix": ".partial"},
    }
    async with session.post(url, json=payload) as resp:
        try:
            data = await resp.json()
        except Exception as e:
            raise RuntimeError(f"move_file: 响应解析失败: {e}")
        if resp.status == 200 and not data:
            return True
        if resp.status != 200 and "error" in data:
            raise RuntimeError(f"move_file error: {data.get('error')}")
        raise RuntimeError(f"move_file: 未知错误，状态码: {resp.status}, 响应: {data}")


async def create_dir(session: aiohttp.ClientSession, fs: str, dir_path: str) -> bool:
    url = f"{RCLONE_BASE_URL}/operations/mkdir"
    payload = {"fs": fs, "remote": dir_path}
    async with session.post(url, json=payload) as resp:
        try:
            data = await resp.json()
        except Exception as e:
            raise RuntimeError(f"create_dir: 响应解析失败: {e}")
        if resp.status == 200 and not data:
            return True
        if resp.status != 200 and "error" in data:
            raise RuntimeError(f"create_dir error: {data.get('error')}")
        raise RuntimeError(f"create_dir: 未知错误，状态码: {resp.status}, 响应: {data}")


def get_file_info(files: List[FileInfo], name: str) -> Optional[FileInfo]:
    for f in files:
        if f.Name == name:
            return f
    return None


def should_skip_recent_file(mod_time_str: str, now: datetime, hours: int) -> bool:
    """
    判断文件ModTime距离现在是否小于n小时
    """
    try:
        # 去除纳秒部分，兼容官方datetime.fromisoformat
        base = mod_time_str[:19]  # "2025-08-27T01:35:54"
        # 处理时区
        if "+" in mod_time_str:
            tz_str = mod_time_str[mod_time_str.find("+") :]
            mod_time = datetime.fromisoformat(base).replace(
                tzinfo=timezone(timedelta(hours=int(tz_str.split(":")[0][1:])))
            )
        elif "-" in mod_time_str[19:]:
            tz_str = mod_time_str[mod_time_str.find("-", 19) :]
            mod_time = datetime.fromisoformat(base).replace(
                tzinfo=timezone(timedelta(hours=-int(tz_str.split(":")[0][1:])))
            )
        else:
            mod_time = datetime.fromisoformat(mod_time_str)
    except Exception as e:
        raise ValueError(f"解析ModTime失败: {mod_time_str}, 错误: {e}")
    delta = now - mod_time
    return delta.total_seconds() < hours * 3600


async def move_small_files() -> None:
    """
    移动较小的flv和对应的xml到单独的目录
    """
    async with aiohttp.ClientSession() as session:
        files: List[FileInfo] = await list_files(session, REC_BASE_DIR)
        now = datetime.now(timezone.utc)
        for flv_info in files:
            # 跳过文件夹
            if flv_info.IsDir:
                continue
            filename = flv_info.Name
            if not filename.endswith(".flv"):
                continue
            xml_filename: str = filename[:-4] + ".xml"
            xml_info: Optional[FileInfo] = get_file_info(files, xml_filename)
            if not xml_info or xml_info.IsDir:
                continue
            # 跳过ModTime距离现在时间不到2小时的flv
            if should_skip_recent_file(flv_info.ModTime, now, 2):
                continue
            if (flv_info.Size or 0) >= FLV_SIZE_LIMIT:
                continue
            if (xml_info.Size or 0) >= XML_SIZE_LIMIT:
                continue
            # 目标子目录
            dest_dir: str = f"{REC_BASE_DIR}/{DEST_SUBDIR}"
            # 检查目标目录是否存在，不存在则创建
            dest_dir_exists: bool = any(f.Path == dest_dir and f.IsDir for f in files)
            if not dest_dir_exists:
                # 创建目录
                await create_dir(session, "/", dest_dir)
            # 移动文件
            await move_file(
                session, f"{REC_BASE_DIR}/{filename}", f"{dest_dir}/{filename}"
            )
            await move_file(
                session, f"{REC_BASE_DIR}/{xml_filename}", f"{dest_dir}/{xml_filename}"
            )
            print(f"Moved: {filename} and {xml_filename}")


async def stop_job(session: aiohttp.ClientSession, jobid: int) -> bool:
    """
    停止指定的任务
    """
    url = f"{RCLONE_BASE_URL}/job/stop"
    payload = {"jobid": jobid}
    async with session.post(url, json=payload) as resp:
        try:
            stop_resp = await resp.json()
        except Exception as e:
            raise RuntimeError(f"stop_job: 响应解析失败: {e}")
        if resp.status == 200 and not stop_resp:
            return True
        if resp.status != 200 and "error" in stop_resp:
            raise RuntimeError(f"stop_job error: {stop_resp.get('error')}")
        raise RuntimeError(
            f"stop_job: 未知错误，状态码: {resp.status}, 响应: {stop_resp}"
        )


async def copy_file(
    session: aiohttp.ClientSession,
    src_fs: str,
    src_path: str,
    dst_fs: str,
    dst_path: str,
    check_interval: int = 1,
) -> None:
    """
    异步复制文件，定期检查任务状态并输出进度。
    """
    # 发起异步复制请求
    url = f"{RCLONE_BASE_URL}/operations/copyfile"
    payload = {
        "srcFs": src_fs,
        "srcRemote": src_path,
        "dstFs": dst_fs,
        "dstRemote": dst_path,
        "_config": {"CheckFirst": True, "Metadata": True, "PartialSuffix": ".partial"},
        "_async": True,
    }
    logging.info(f"copy_file: 开始任务 src={src_path}, dst={dst_path}")
    async with session.post(url, json=payload) as resp:
        data = await resp.json()
        try:
            copy_resp = CopyFileResponse.model_validate(data)
        except Exception as e:
            raise RuntimeError(f"copy_file: copyfile响应解析失败: {e}, data: {data}")
        jobid = copy_resp.jobid

    # 进度条初始化
    finished = False

    with tqdm(
        total=1,
        desc=src_path.split("/")[-1],
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as pbar:
        while not finished:
            # 查询任务状态
            status_url = f"{RCLONE_BASE_URL}/job/status"
            status_payload = {"jobid": str(jobid)}
            async with session.post(status_url, json=status_payload) as status_resp:
                status_data = await status_resp.json()
                try:
                    job_status = JobStatusResponse.model_validate(status_data)
                except Exception as e:
                    raise RuntimeError(
                        f"copy_file: job/status响应解析失败: {e}, data: {status_data}"
                    )
                finished = job_status.finished
                success = job_status.success
                error = job_status.error
                # 查询进度
                stats_url = f"{RCLONE_BASE_URL}/core/stats"
                async with session.post(stats_url, json={}) as stats_resp:
                    stats_data = await stats_resp.json()
                    try:
                        stats_resp_obj = CoreStatsResponse.model_validate(stats_data)
                    except Exception as e:
                        raise RuntimeError(
                            f"copy_file: core/stats响应解析失败: {e}, data: {stats_data}"
                        )
                    transferring = stats_resp_obj.transferring or []
                    # 查找当前job的进度
                    job_progress = None
                    for t in transferring:
                        if t.group == f"job/{jobid}":
                            job_progress = t
                            break
                    if job_progress:
                        total = job_progress.size
                        current = job_progress.bytes
                        pbar.total = total
                        pbar.n = current
                        pbar.refresh()
                    else:
                        pbar.total = 1
                        pbar.n = 0
                        pbar.refresh()
            if finished:
                pbar.close()
                if not success:
                    raise RuntimeError(f"copy_file: 任务失败, error: {error}")
                logging.info(
                    f"copy_file: 任务完成, jobid={jobid}, src={src_path}, dst={dst_path}"
                )
                break
            try:
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError as e:
                logging.info("copy_file: 用户取消任务")
                await stop_job(session, jobid)
                raise e


async def parse_month_from_filename(filename: str) -> int:
    """
    从文件名中解析出月份：先用 '-' 分割，取第三段（日期），再解析月份
    """
    parts = filename.split("-")
    if len(parts) < 3:
        raise ValueError(f"文件名格式不正确: {filename}")
    date_str = parts[2]
    if len(date_str) != 8 or not date_str.isdigit():
        raise ValueError(f"日期段格式不正确: {date_str}")
    month = int(date_str[4:6])
    return month


async def upload_and_move() -> None:
    """
    上传文件到CLOUD_FS里f"{month}月"的文件夹，并移动本地文件到uploaded目录里f"{month}月"的文件夹
    允许上传的文件类型为flv、xml、txt
    会在上传前检查当天已上传字节数，不超过 DAILY_UPLOAD_LIMIT（50GB）。
    """
    global uploaded_today
    async with aiohttp.ClientSession() as session:
        logging.info(
            f"今日已上传: {bytes_to_mb_str(uploaded_today)} / 限额: {bytes_to_mb_str(DAILY_UPLOAD_LIMIT)}"
        )

        files: List[FileInfo] = await list_files(session, REC_BASE_DIR)
        for file in files:
            if not file.Name.endswith((".flv", ".xml", ".txt")):
                continue
            # 跳过ModTime距离现在时间不到2小时的文件
            now = datetime.now(timezone.utc)
            if should_skip_recent_file(file.ModTime, now, 2):
                continue

            # 如果已达到或超过当日限额，跳过剩余上传
            if uploaded_today + file.Size >= DAILY_UPLOAD_LIMIT:
                logging.info(
                    f"将要达到今日上传限额，跳过剩余上传任务: 当前 {bytes_to_mb_str(uploaded_today)} + 文件 {bytes_to_mb_str(file.Size)} >= 限额 {bytes_to_mb_str(DAILY_UPLOAD_LIMIT)}"
                )
                break

            month = await parse_month_from_filename(file.Name)
            month_dir = f"{month}月"
            cloud_dir = f"{CLOUD_BASE_DIR}/{month_dir}"
            uploaded_dir = f"{REC_BASE_DIR}/uploaded/{month_dir}"
            await create_dir(session, CLOUD_FS, cloud_dir)
            await create_dir(session, "/", uploaded_dir)
            cloud_path = f"{cloud_dir}/{file.Name}"
            uploaded_path = f"{uploaded_dir}/{file.Name}"

            # 执行上传并在成功后更新已上传字节计数
            await copy_file(session, "/", file.Path, CLOUD_FS, cloud_path)
            uploaded_today += file.Size
            logging.info(
                f"上传后今日已上传: {bytes_to_mb_str(uploaded_today)} (新增 {bytes_to_mb_str(file.Size)})"
            )

            # 移动本地文件到 uploaded 目录
            await move_file(session, file.Path, uploaded_path)


async def batch_process_files() -> None:
    await move_small_files()
    await upload_and_move()


async def main() -> None:
    """主入口：使用 APScheduler 的 AsyncIOScheduler 在本地时间每天 10:00 调度 `batch_process_files`。"""

    # 使用 AsyncIOScheduler 调度每天本地时间 10:00
    scheduler = AsyncIOScheduler()

    # 全局上传计数锁已在模块导入时初始化

    async def reset_daily_counter() -> None:
        global uploaded_today
        # 直接重置内存计数（无锁）
        uploaded_today = 0
        logging.info("已重置今日上传计数为 0")

    # 添加每日零点重置任务
    reset_trigger = CronTrigger(hour=0, minute=0)
    scheduler.add_job(
        reset_daily_counter,
        reset_trigger,
        id="reset_daily_counter",
        replace_existing=True,
    )

    # 用于追踪当前正在运行的 asyncio Tasks（job_wrapper 的实例）
    running_tasks: set[asyncio.Task] = set()

    async def job_wrapper() -> None:
        # 将当前协程包装为 Task 并注册，以便主线程可以取消它
        current_task = asyncio.current_task()
        if current_task is not None:
            running_tasks.add(current_task)
        try:
            logging.info("开始执行 batch_process_files")
            await batch_process_files()
            logging.info("batch_process_files 执行完成")
        except asyncio.CancelledError:
            logging.info(
                "job_wrapper: 收到取消信号，正在取消正在运行的子任务（如果有）"
            )
            # 传播 CancelledError 以便上层也能处理
            raise
        except Exception as e:
            logging.exception(f"batch_process_files 执行出错: {e}")
        finally:
            if current_task is not None and current_task in running_tasks:
                running_tasks.discard(current_task)

    # 首次运行：启动时立即执行一次 job_wrapper（不等待第一次 Cron 触发）
    logging.info("首次启动：立即触发一次 batch_process_files")
    await job_wrapper()

    # 添加每天本地时间 10:00 的 Cron 触发器
    trigger = CronTrigger(hour=10, minute=0)
    scheduler.add_job(
        job_wrapper,
        trigger,
        id="daily_batch_10am",
        replace_existing=True,
        max_instances=1,
    )

    # 启动调度器
    scheduler.start()
    logging.info(
        "APScheduler 调度器已启动，任务已注册：每天本地时间 10:00 执行 batch_process_files"
    )

    # 优雅退出处理
    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()

    def _handle_signal(_: int, __: Optional[object] = None) -> None:
        logging.info("收到终止信号，准备关闭调度器和事件循环")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            # Windows 上的某些事件循环实现不支持 add_signal_handler
            signal.signal(sig, lambda s, f: _handle_signal(s, f))

    # 等待停止事件
    await stop_event.wait()

    logging.info("收到关闭信号：取消所有正在运行的调度任务...")
    # 首先停止调度器，避免触发新的作业
    scheduler.shutdown(wait=False)

    # 取消并等待所有正在运行的 job tasks，这会触发 job 内部的 CancelledError 分支
    if running_tasks:
        logging.info(f"正在取消 {len(running_tasks)} 个正在运行的任务")
        for t in list(running_tasks):
            try:
                t.cancel()
            except Exception:
                logging.exception("取消任务时发生错误")

        # 等待任务完成，设置一个超时以避免无限等待
        try:
            await asyncio.wait_for(
                asyncio.gather(*running_tasks, return_exceptions=True), timeout=30
            )
        except asyncio.TimeoutError:
            logging.warning("等待正在运行的任务完成超时，继续退出")

    logging.info("所有运行中任务已处理，退出")


if __name__ == "__main__":
    asyncio.run(main())
