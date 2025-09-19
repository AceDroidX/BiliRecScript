FROM python:3.13-alpine

# 避免生成 .pyc 文件并使输出不被缓冲（对日志友好）
# ENV PYTHONDONTWRITEBYTECODE=1 \
#     PYTHONUNBUFFERED=1

WORKDIR /app

# 将依赖和源码复制到镜像中
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py /app

# 非 root 运行（可选），但保留为 root 以便挂载任意路径时权限一致
# 如果希望更安全，可以取消注释下面两行以创建并切换到非特权用户
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser /app
USER appuser

CMD ["python3", "main.py"]
