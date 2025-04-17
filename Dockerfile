# 使用官方 Python 运行时作为父镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装依赖
# --no-cache-dir: 不缓存下载的包，减小镜像体积
# --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org: 解决可能的网络问题
RUN pip install --no-cache-dir --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# 复制项目代码到工作目录
COPY app.py .

# 暴露 Streamlit 默认端口
EXPOSE 8501

# 运行 app.py 当容器启动时
# --server.port 8501: 指定端口
# --server.address 0.0.0.0: 允许从外部访问
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]