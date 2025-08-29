import aiohttp
import asyncio
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
from tqdm.asyncio import tqdm
from dotenv import load_dotenv
import os

load_dotenv()

REC_BASE_DIR: str = os.getenv("REC_BASE_DIR") or (_ for _ in ()).throw(ValueError("环境变量 REC_BASE_DIR 未设置"))
CLOUD_FS: str = os.getenv("CLOUD_FS") or (_ for _ in ()).throw(ValueError("环境变量 CLOUD_FS 未设置"))
CLOUD_BASE_DIR: str = os.getenv("CLOUD_BASE_DIR") or (_ for _ in ()).throw(ValueError("环境变量 CLOUD_BASE_DIR 未设置"))
RCLONE_BASE_URL: str = os.getenv("RCLONE_BASE_URL") or (_ for _ in ()).throw(ValueError("环境变量 RCLONE_BASE_URL 未设置"))
DEST_SUBDIR: str = "smallfile"
FLV_SIZE_LIMIT: int = 1 * 1024 * 1024  # 1MB
XML_SIZE_LIMIT: int = 20 * 1024  # 20KB


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


def should_skip_recent_file(mod_time_str: str, now: datetime, hours: int = 6) -> bool:
    """
    判断文件ModTime距离现在是否小于6小时
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
            # 跳过ModTime距离现在时间不到6小时的flv
            if should_skip_recent_file(flv_info.ModTime, now, 6):
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
        raise RuntimeError(f"stop_job: 未知错误，状态码: {resp.status}, 响应: {stop_resp}")

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
    print(f"copy_file: 开始任务 src={src_path}, dst={dst_path}")
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
                print(
                    f"copy_file: 任务完成, jobid={jobid}, src={src_path}, dst={dst_path}"
                )
                break
            try:
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError as e:
                print("copy_file: 用户取消任务")
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
    """
    async with aiohttp.ClientSession() as session:
        files: List[FileInfo] = await list_files(session, REC_BASE_DIR)
        for file in files:
            if file.Name.endswith((".flv", ".xml", ".txt")):
                month = await parse_month_from_filename(file.Name)
                month_dir = f"{month}月"
                cloud_dir = f"{CLOUD_BASE_DIR}/{month_dir}"
                uploaded_dir = f"{REC_BASE_DIR}/uploaded/{month_dir}"
                await create_dir(session, CLOUD_FS, cloud_dir)
                await create_dir(session, "/", uploaded_dir)
                cloud_path = f"{cloud_dir}/{file.Name}"
                uploaded_path = f"{uploaded_dir}/{file.Name}"
                await copy_file(session, "/", file.Path, CLOUD_FS, cloud_path)
                await move_file(session, file.Path, uploaded_path)

async def main() -> None:
    await move_small_files()
    await upload_and_move()


if __name__ == "__main__":
    asyncio.run(main())
