import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os
from dotenv import load_dotenv
import io
import time 
import base64 
import fitz 
from openai import OpenAI
import logging 

st.set_page_config(page_title="析言数据分析助手", layout="wide")

# 加载环境变量
load_dotenv()

# VL模型
VL_MODEL_BASEURL = os.getenv("VL_MODEL_BASEURL")
VL_MODEL_KEY = os.getenv("VL_MODEL_KEY")
VL_MODEL_NAME = os.getenv("VL_MODEL_NAME")
# SQL模型
SQL_MODEL_BASEURL = os.getenv("SQL_MODEL_BASEURL")
SQL_MODEL_KEY = os.getenv("SQL_MODEL_KEY")
SQL_MODEL_NAME = os.getenv("SQL_MODEL_NAME")

# --- API 客户端初始化 ---
def get_openai_client(base_url, api_key):
    """获取 OpenAI 客户端实例"""
    if not base_url or not api_key:
        st.error("缺少必要的API配置信息 (Base URL 或 API Key)")
        return None
    try:
        return OpenAI(api_key=api_key, base_url=base_url)
    except Exception as e:
        st.error(f"初始化 OpenAI 客户端时出错: {e}")
        return None

# 在应用加载时尝试初始化客户端
@st.cache_resource
def cached_get_vl_client():
    return get_openai_client(VL_MODEL_BASEURL, VL_MODEL_KEY)

@st.cache_resource
def cached_get_sql_client():
    return get_openai_client(SQL_MODEL_BASEURL, SQL_MODEL_KEY)

vl_client = cached_get_vl_client()
sql_client = cached_get_sql_client()

# --- 数据库连接 ---
def get_db_connection_form():
    """显示数据库连接表单"""
    with st.expander("数据库连接配置", expanded=True):
        with st.form("db_connection_form"):
            st.write("请填写数据库连接信息")
            # 从环境变量获取默认值
            db_host = st.text_input("数据库地址", value=os.getenv("DB_HOST", ""))
            db_port = st.text_input("端口", value=os.getenv("DB_PORT", "5432"))
            db_user = st.text_input("用户名", value=os.getenv("DB_USER", ""))
            db_password = st.text_input("密码", value=os.getenv("DB_PASSWORD", ""), type="password")
            db_name = st.text_input("数据库名", value=os.getenv("DB_DATABASE", ""))
            
            submitted = st.form_submit_button("连接数据库")
            if submitted:
                return {
                    "DB_HOST": db_host,
                    "DB_PORT": db_port,
                    "DB_USER": db_user,
                    "DB_PASSWORD": db_password,
                    "DB_DATABASE": db_name
                }
    return None

def get_db_connection(db_config):
    """根据配置建立数据库连接"""
    conn = None
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=db_config["DB_HOST"],
                port=db_config["DB_PORT"],
                user=db_config["DB_USER"],
                password=db_config["DB_PASSWORD"],
                database=db_config["DB_DATABASE"]
            )
            st.success("数据库连接成功!")
            return conn
        except psycopg2.OperationalError as e:
            st.warning(f"数据库连接失败，正在重试... ({retries}次剩余) 错误: {e}")
            retries -= 1
            time.sleep(5)
    st.error("无法连接到数据库，请检查配置或数据库服务状态。")
    return None

# --- 数据库辅助函数 ---
def insert_dataframe_to_db(df, table_name, conn):
    """将DataFrame插入到指定的数据库表中 (覆盖现有表)"""
    try:
        table_name = table_name.lower()  # 统一表名为小写
        with conn.cursor() as cur:
            # 检查表是否存在，如果存在则删除重建
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            conn.commit()
            
            # 类型推断函数
            def infer_sql_type(dtype):
                if pd.api.types.is_numeric_dtype(dtype):
                    return 'NUMERIC'
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    return 'TIMESTAMP'
                elif pd.api.types.is_bool_dtype(dtype):
                    return 'BOOLEAN'
                else:
                    return 'TEXT'
            
            # 创建表结构（根据数据类型推断列类型）
            columns = []
            for col in df.columns:
                col_type = infer_sql_type(df[col].dtype)
                columns.append(f'"{col}" {col_type}')
            create_table_sql = f'CREATE TABLE "{table_name}" (' + ', '.join(columns) + ');'
            cur.execute(create_table_sql)
            conn.commit()

            # 插入数据 (使用COPY FROM提高效率)
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep=',', quoting=1, quotechar='"', doublequote=True)
            buffer.seek(0)

            copy_sql = """COPY %s FROM stdin WITH (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"', ESCAPE '"')"""
            cur.copy_expert(sql=copy_sql % table_name, file=buffer)
            conn.commit()
        return True
    except psycopg2.Error as e:
        logging.error(f"数据库操作失败: {e}")
        st.error(f"将数据导入表 '{table_name}' 时出错: {e}")
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"未知错误: {e}", exc_info=True)
        st.error(f"将数据导入表 '{table_name}' 时出错: {e}")
        conn.rollback()
        return False

# --- 文件处理函数 ---
def process_tabular_file(uploaded_file, conn):
    """处理表格文件 (CSV, XLS, XLSX)"""
    try:
        file_name = os.path.splitext(uploaded_file.name)[0]
        table_name = ''.join(filter(str.isalnum, file_name)).lower()  # 统一表名为小写

        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file, escapechar='\\')
        else: # .xls or .xlsx
            df = pd.read_excel(uploaded_file)

        # 使用通用函数导入数据
        if insert_dataframe_to_db(df, table_name, conn):
            st.success(f"文件 '{uploaded_file.name}' 已成功导入到表 '{table_name}'")
            return table_name
        return None
    except Exception as e:
        st.error(f"处理表格文件 '{uploaded_file.name}' 时出错: {e}")
        return None


def call_vl_api(image_bytes=None, pdf_bytes=None):
    """调用Qwen-VL API进行OCR识别"""
    if not vl_client:
        st.error("VL 模型客户端未初始化，无法调用API。")
        return None

    messages = [
        {
            "role": "system",
            "content": [
                {"type": "text", "text": "你是一个有用的助手。请从图片或PDF中提取表格数据并以CSV格式返回。"}
            ]
        }
    ]

    user_content = []
    if image_bytes:
        img_base64 = base64.b64encode(image_bytes).decode('utf-8')
        user_content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{img_base64}"}
        })
    elif pdf_bytes:
        try:
            # 使用PyMuPDF将PDF转换为图片
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            for page_num in range(min(3, len(doc))):  # 最多处理前3页
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=300)  # 高DPI提高OCR精度
                img_bytes = pix.tobytes("jpeg")
                img_base64 = base64.b64encode(img_bytes).decode('utf-8')
                user_content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{img_base64}"}
                })
        except Exception as pdf_e:
            st.error(f"处理PDF时出错: {pdf_e}")
            return None

    user_content.append({
        "type": "text",
        "text": "请将上述内容中的表格数据提取出来，并以CSV格式返回。"
    })
    
    messages.append({
        "role": "user",
        "content": user_content
    })

    try:
        response = vl_client.chat.completions.create( # Use global vl_client
            model=VL_MODEL_NAME,
            messages=messages,
            temperature=0.1
        )
        
        # 解析响应
        if response.choices and response.choices[0].message.content:
            message_content = response.choices[0].message.content
            # 尝试从返回内容中找到CSV格式的数据块
            if '```csv' in message_content:
                csv_text = message_content.split('```csv')[1].split('```')[0].strip()
                df = pd.read_csv(io.StringIO(csv_text))
                return df
            else:
                st.warning(f"未能从API返回结果中提取有效的CSV数据。API原始返回: {message_content}")
                return None
        else:
            st.error(f"API调用成功，但返回结果格式不符合预期: {response}")
            return None
            
    except Exception as e:
        st.error(f"调用Qwen-VL API时出错: {e}")
        return None

def process_ocr(uploaded_file, conn):
    """处理图片或PDF文件"""
    try:
        file_name = os.path.splitext(uploaded_file.name)[0]
        table_name = ''.join(filter(str.isalnum, file_name)) # 清理文件名作为表名
        file_bytes = uploaded_file.getvalue()

        df = None
        if uploaded_file.type.startswith('image/'):
            df = call_vl_api(image_bytes=file_bytes)
        elif uploaded_file.type == 'application/pdf':
            df = call_vl_api(pdf_bytes=file_bytes)

        if df is not None and not df.empty:
            # 使用辅助函数将DataFrame导入数据库
            if insert_dataframe_to_db(df, table_name, conn):
                st.success(f"文件 '{uploaded_file.name}' 通过OCR处理后成功导入到表 '{table_name}'")
                return table_name
            else:
                return None
        else:
            st.error(f"未能从文件 '{uploaded_file.name}' 中提取或处理表格数据。")
            return None
    except Exception as e:
        st.error(f"处理文件 '{uploaded_file.name}' 时出错: {e}")
        return None

# --- 自然语言查询函数 ---
@st.cache_data(show_spinner=False) # Cache the schema based on connection and known tables
def get_db_schema(_conn, known_tables_tuple): # Use _conn to indicate it's for caching invalidation
    """获取数据库的表结构信息 (Cached)"""
    # Convert tuple back to list for processing if needed, though not strictly necessary here
    # known_tables = list(known_tables_tuple)
    schema = {}
    try:
        with _conn.cursor() as cur:
            # 获取所有用户表名 (或者可以只获取 known_tables? 保持获取所有表可能更安全)
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
            """)
            tables = [row[0] for row in cur.fetchall()]

            # 获取每个表的列信息
            for table_name in tables:
                # Optional: Filter to only show known (uploaded) tables in schema prompt?
                # if table_name in known_tables:
                cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = '{table_name}';
                """)
                columns = {row[0]: row[1] for row in cur.fetchall()}
                schema[table_name] = columns
        return schema
    except Exception as e:
        st.error(f"获取数据库结构时出错: {e}")
        return None

def call_xiyan_sql_api(user_query, db_schema):
    """调用XiYanSQL API将自然语言转换为SQL"""
    if not sql_client:
        st.error("SQL 模型客户端未初始化，无法调用API。")
        return None
    # from openai import OpenAI # Removed - client initialized globally

    try:
        # client = OpenAI( # Removed - use global sql_client
        #     api_key=SQL_MODEL_KEY,
        #     base_url=SQL_MODEL_BASEURL
        # )

        # 构建系统提示词 - 添加类型转换提示
        system_prompt = f"""你是一个强大的Text-to-SQL模型。你的角色是将用户的自然语言问题转换成PG-SQL查询语句，以便在以下的数据库模式上执行。
数据库模式如下：
{db_schema}

重要提示：
1. 如果需要对文本类型的列进行数值运算(如加减乘除)，必须使用CAST(列名 AS NUMERIC)进行显式类型转换
2. 在比较数值时(如=, <>, >, <等)，也必须使用CAST(列名 AS NUMERIC)进行显式类型转换
3. 例如：SELECT CAST(语文成绩 AS NUMERIC) + CAST(数学成绩 AS NUMERIC) AS 总分
4. 比较示例：WHERE CAST(t1.语文成绩 AS NUMERIC) <> CAST(t2.语文成绩 AS NUMERIC)
5. 确保所有数值运算和比较都进行了适当的类型转换
6. 特别注意：每个WHERE条件必须完整，不能以OR/AND等逻辑运算符结尾
7. 支持DROP TABLE等DDL语句"""

        response = sql_client.chat.completions.create( # Use global sql_client
            model=SQL_MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            temperature=0.1,
            max_tokens=500
        )

        # 解析API返回结果
        generated_text = response.choices[0].message.content
        
        # 提取SQL语句
        sql_query = None
        if '```sql' in generated_text:
            sql_query = generated_text.split('```sql')[1].split('```')[0].strip()
        elif any(keyword in generated_text.upper() for keyword in ['SELECT', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']):
            sql_query = generated_text.strip()

        if sql_query:
            sql_query = sql_query.rstrip(';')
            return sql_query
        else:
            st.warning(f"未能从API返回结果中提取有效的SQL语句。API原始返回: {generated_text}")
            st.info("提示：请明确指定要删除的表名，例如'删除表黄维申1月统一报销0205'")
            return None

    except Exception as e:
        st.error(f"调用XiYanSQL API时出错: {e}")
        return None

def execute_sql_query(conn, sql_query):
    """执行SQL查询并返回结果"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            # 检查是否是SELECT语句
            if sql_query.strip().upper().startswith("SELECT"):
                colnames = [desc[0] for desc in cur.description]
                results = cur.fetchall()
                df = pd.DataFrame(results, columns=colnames)
                return df, None # 返回DataFrame和无错误
            else:
                conn.commit() # 对于非SELECT语句（如UPDATE, INSERT, DELETE），提交事务
                return None, f"操作成功完成，影响行数: {cur.rowcount}" # 返回None和成功消息
    except Exception as e:
        conn.rollback() # 出错时回滚
        st.error(f"执行SQL查询时出错: {e}")
        st.error(f"尝试执行的SQL: {sql_query}")
        return None, f"执行SQL查询时出错: {e}" # 返回None和错误消息

# --- UI 辅助函数 ---
def display_results(dataframe, query_context="query_result"):
    """在Streamlit中显示DataFrame结果和下载按钮"""
    if dataframe is not None and not dataframe.empty:
        st.dataframe(dataframe.head(10).iloc[:, :10]) # 显示前10行10列
        csv = dataframe.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="下载完整表格 (CSV)",
            data=csv,
            file_name=f'{query_context}.csv',
            mime='text/csv',
        )
    elif dataframe is not None and dataframe.empty:
        st.info("查询成功，但结果为空。")
    # else: dataframe is None, error handled elsewhere

# --- Streamlit UI ---

st.title("析言数据分析助手")
st.caption("上传您的数据文件（CSV, Excel, 图片, PDF），然后用自然语言提问吧！")

# 初始化数据库连接和会话状态
if 'db_conn' not in st.session_state or st.session_state.db_conn is None or st.session_state.db_conn.closed:
    db_config = get_db_connection_form()
    if db_config:
        st.session_state.db_conn = get_db_connection(db_config)
    else:
        st.session_state.db_conn = None
else:
    # 如果已有连接，显示连接状态
    try:
        with st.session_state.db_conn.cursor() as cur:
            cur.execute('SELECT 1')
        st.success("数据库已连接")
    except:
        st.session_state.db_conn = None
        st.warning("数据库连接已断开，请重新连接")

if 'uploaded_tables' not in st.session_state:
    st.session_state.uploaded_tables = []
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'result_df' not in st.session_state: # 初始化 result_df
    st.session_state.result_df = None

# 获取数据库连接
conn = st.session_state.db_conn

# --- 文件上传区域 ---
st.header("1. 上传数据文件")
uploaded_files = st.file_uploader(
    "选择CSV, XLS, XLSX, JPG, PNG, 或 PDF 文件",
    accept_multiple_files=True,
    type=['csv', 'xls', 'xlsx', 'jpg', 'png', 'pdf']
)

if uploaded_files and conn:
    progress_bar = st.progress(0)
    status_text = st.empty()
    processed_count = 0
    newly_uploaded_tables = []

    for i, uploaded_file in enumerate(uploaded_files):
        status_text.text(f"正在处理文件 {i+1}/{len(uploaded_files)}: {uploaded_file.name}")
        table_name = None
        file_type = uploaded_file.type

        if file_type in ['text/csv', 'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']:
            table_name = process_tabular_file(uploaded_file, conn)
        elif file_type.startswith('image/') or file_type == 'application/pdf':
            table_name = process_ocr(uploaded_file, conn)
        else:
            st.warning(f"不支持的文件类型: {uploaded_file.name} ({file_type})")

        if table_name and table_name not in st.session_state.uploaded_tables:
            newly_uploaded_tables.append(table_name)
            st.session_state.uploaded_tables.append(table_name)

        processed_count += 1
        progress_bar.progress(processed_count / len(uploaded_files))

    status_text.text(f"所有文件处理完成！新增数据表: {', '.join(newly_uploaded_tables) if newly_uploaded_tables else '无'}")
    progress_bar.empty()
    # 清空上传列表避免重复处理
    # st.rerun() # 强制刷新可能导致用户体验不佳，暂时注释

# 显示当前数据库中的表（仅用户上传的）
if st.session_state.uploaded_tables:
    st.subheader("当前已加载的数据表:")
    st.write(", ".join(st.session_state.uploaded_tables))

# --- 自然语言查询区域 ---
st.header("2. 提问与分析")

# 显示聊天记录
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "dataframe" in message and message["dataframe"] is not None:
            display_results(message["dataframe"], message.get("query", "chat_result")) # Use helper function

# 获取用户输入
user_query = st.chat_input("请输入您的问题 (例如：'统计每个产品的销售总额')")

if user_query and conn:
    # 将用户问题添加到聊天记录
    st.session_state.chat_history.append({"role": "user", "content": user_query})
    with st.chat_message("user"):
        st.markdown(user_query)

    # 获取数据库结构 (使用缓存)
    with st.spinner("正在理解您的问题并查询数据库..."):
        # Pass tuple of known tables to ensure cache invalidation when tables change
        known_tables_tuple = tuple(sorted(st.session_state.uploaded_tables))
        db_schema = get_db_schema(conn, known_tables_tuple)
        if not db_schema:
            st.error("无法获取数据库结构，无法继续查询。")
        else:
            # 调用XiYanSQL API获取SQL语句
            sql_query = call_xiyan_sql_api(user_query, db_schema)

            if sql_query:
                st.write("生成的SQL查询:")
                st.code(sql_query, language='sql')

                # 执行SQL查询
                result_df, error_msg = execute_sql_query(conn, sql_query)

                # 将结果存储在 session_state 中
                st.session_state.result_df = result_df
                st.session_state.plotly_fig = None # 清除旧图表

                # 准备助手的回复
                assistant_response = {"role": "assistant"}

                if error_msg:
                    assistant_response["content"] = f"抱歉，执行查询时遇到问题：\n```{error_msg}```"
                elif result_df is not None:
                    assistant_response["content"] = f"根据您的问题 '{user_query}'，查询结果如下："
                    assistant_response["dataframe"] = result_df
                    assistant_response["query"] = user_query.replace(' ', '_')[:30] # 用于文件名

                else: # 非SELECT查询成功
                     assistant_response["content"] = f"操作已成功执行。"
                     st.session_state.result_df = None # 清除非SELECT查询的结果
                     # st.session_state.plotly_fig = None # Removed unused state

            else:
                assistant_response = {"role": "assistant", "content": "抱歉，我无法将您的问题转换为SQL查询。请尝试换一种问法。"}
                st.session_state.result_df = None # 清除失败查询的结果
                # st.session_state.plotly_fig = None # Removed unused state

    # 将助手回复添加到聊天记录并显示
    st.session_state.chat_history.append(assistant_response)
    with st.chat_message("assistant"):
        st.markdown(assistant_response["content"])
        if "dataframe" in assistant_response and assistant_response["dataframe"] is not None:
            display_results(assistant_response["dataframe"], assistant_response.get("query", "last_query_result")) # Use helper function

# --- 图表生成与显示区域 --- (移到聊天循环外部)
if st.session_state.result_df is not None and not st.session_state.result_df.empty and len(st.session_state.result_df.columns) >= 2:
    col1, col2 = st.columns([1, 2])
    
    with col1:
        with st.expander("图表设置", expanded=True):
            chart_type = st.selectbox(
                "选择图表类型",
                ["柱状图", "折线图", "饼图"],
                key='chart_type_select'
            )
            x_col = st.selectbox(
                "选择X轴数据",
                st.session_state.result_df.columns,
                key='x_col_select'
            )
            y_col = st.selectbox(
                "选择Y轴数据",
                st.session_state.result_df.columns,
                index=1 if len(st.session_state.result_df.columns) > 1 else 0,
                key='y_col_select'
            )
            if st.button("生成图表"):
                try:
                    fig = None
                    if chart_type == "柱状图":
                        fig = px.bar(st.session_state.result_df, x=x_col, y=y_col, title=f'{y_col} vs {x_col}')
                    elif chart_type == "折线图":
                        fig = px.line(st.session_state.result_df, x=x_col, y=y_col, title=f'{y_col} vs {x_col}')
                    elif chart_type == "饼图":
                        fig = px.pie(st.session_state.result_df, names=x_col, values=y_col, title=f'{y_col} 分布 by {x_col}')

                    if fig:
                        st.session_state.plotly_fig = fig
                    else:
                        st.warning("无法生成所选图表类型。")
                except Exception as e:
                    st.error(f"生成图表时出错: {e}")
                    st.session_state.plotly_fig = None
    
    with col2:
        if st.session_state.plotly_fig is not None:
            st.plotly_chart(st.session_state.plotly_fig, use_container_width=True, key=f"plotly_chart_{time.time()}")

elif user_query and not conn:
    st.error("数据库未连接，请检查配置并重启应用。")

# --- 页脚 ---
st.markdown("---")
st.caption("Powered by Streamlit")