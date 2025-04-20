# 使用官方 Python 运行时作为父镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装依赖
# --no-cache-dir: 不缓存下载的包，减小镜像体积
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ 

# 复制项目代码到工作目录
COPY ../ /app

# 暴露 Streamlit 默认端口
EXPOSE 8501

# 运行 app.py 当容器启动时
# --server.port 8501: 指定端口
# --server.address 0.0.0.0: 允许从外部访问
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]