# 析言数据分析助手

这是一个基于 Streamlit 的 Web 应用，允许用户上传数据文件（表格、图片、PDF），通过 AI 模型进行数据提取和分析，并使用自然语言查询数据库。

## 功能特性

*   **多种文件上传:** 支持上传 CSV, Excel (.xls, .xlsx), 图片 (.jpg, .png) 和 PDF 文件。
*   **智能数据提取:** 
    *   直接解析表格文件 (CSV, Excel)。
    *   使用 Qwen-VL 模型对图片和 PDF 文件进行 OCR 识别，提取表格数据。
*   **自动数据库集成:** 上传或识别的数据会自动存入 PostgreSQL 数据库，并根据文件名自动创建数据表。
*   **自然语言查询:** 用户可以使用自然语言提问，应用将调用 XiYan-SQL 模型将问题转换为 SQL 查询语句。
*   **数据可视化:** 对查询结果自动生成 Plotly 图表进行可视化分析。
*   **结果展示与下载:** 在界面上展示查询结果（表格和图表），并提供 CSV 格式下载。
*   **灵活的数据库连接:** 用户可以通过界面配置数据库连接信息。

## 环境配置

在运行应用前，请将项目根目录下的 `.env.example` 文件复制一份并重命名为 `.env`。然后，根据你的实际情况修改 `.env` 文件中的配置项：

```env
# Qwen-VL 模型配置 (用于 OCR)
VL_MODEL_BASEURL=YOUR_VL_MODEL_API_BASE_URL
VL_MODEL_KEY=YOUR_VL_MODEL_API_KEY
VL_MODEL_NAME=qwen-vl-plus # 或其他兼容模型

# XiYan-SQL 模型配置 (用于 Text-to-SQL)
SQL_MODEL_BASEURL=YOUR_SQL_MODEL_API_BASE_URL
SQL_MODEL_KEY=YOUR_SQL_MODEL_API_KEY
SQL_MODEL_NAME=sql-agent # 或其他兼容模型

# PostgreSQL 数据库连接信息 (如果使用 Docker Compose, 这些会被覆盖)
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_DATABASE=your_db_name
```

**注意:** 请将 `YOUR_...` 替换为实际的 API 地址、密钥和数据库凭证。

## 部署方式

### 方式一：使用 Pip 和虚拟环境

1.  **前提条件:**
    *   Python 3.8 或更高版本
    *   Pip 包管理器
    *   一个正在运行的 PostgreSQL 数据库实例

2.  **步骤:**
    ```bash
    # 1. 克隆仓库
    git clone <repository_url>
    cd xiyan-web

    # 2. 创建并激活虚拟环境 (推荐)
    python -m venv venv
    source venv/bin/activate  # Linux/macOS
    # venv\Scripts\activate  # Windows

    # 3. 安装依赖
    pip install -r requirements.txt

    # 4. 复制并配置 .env 文件 (参考上面的 环境配置 部分)
    cp .env.example .env
    # 然后编辑 .env 文件，填入你的 API Keys 和数据库信息（如果未使用 Docker）
    #    确保 DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE 指向你的 PostgreSQL 实例

    # 5. 运行 Streamlit 应用
    streamlit run app.py
    ```

3.  在浏览器中打开 Streamlit 提供的本地 URL (通常是 `http://localhost:8501`)。

### 方式二：使用 Docker 和 Docker Compose

这种方式会自动创建一个 PostgreSQL 容器，简化了数据库配置。

1.  **前提条件:**
    *   Docker
    *   Docker Compose

2.  **步骤:**
    ```bash
    # 1. 克隆仓库
    git clone <repository_url>
    cd xiyan-web

    # 2. 复制并配置 .env 文件 (参考上面的 环境配置 部分)
    cp .env.example .env
    # 然后编辑 .env 文件，填入你的 VL_* 和 SQL_* API Keys。
    # DB_* 变量会被 docker-compose.yaml 覆盖，无需在此修改。
    #    只需要配置 VL_* 和 SQL_* 相关的环境变量。
    #    DB_* 变量会被 docker-compose.yaml 中的设置覆盖。

    # 3. 使用 Docker Compose 构建并启动服务
    docker-compose up --build -d
    ```
    *   `--build` 确保镜像被构建。
    *   `-d` 让容器在后台运行。

4.  应用将在 `http://localhost:8501` 上可用。PostgreSQL 数据库将在 Docker 网络内部运行，应用会自动连接。

5.  **停止服务:**
    ```bash
    docker-compose down
    ```

## 使用说明

1.  启动应用后，首先在侧边栏或展开区域配置数据库连接信息（如果未使用 Docker Compose）。
2.  点击“连接数据库”按钮。
3.  在“上传数据文件”区域上传你的文件。
4.  文件处理完成后，已加载的数据表会显示出来。
5.  在“提问与分析”区域的聊天输入框中输入你的自然语言问题，例如：“统计每个产品的销售总额”，“显示所有语文成绩大于90分的学生姓名”，“删除表 temp_data”。
6.  应用会生成 SQL 查询，执行并在下方显示结果表格和图表。
7.  你可以下载查询结果的 CSV 文件。