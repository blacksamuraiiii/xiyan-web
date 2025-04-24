# -*- coding: utf-8 -*-
"""
析言数据分析助手主程序v0.3
功能：提供基于自然语言的数据分析界面，支持文件上传、数据库连接、SQL生成和可视化
"""

# 标准库导入
import io
import logging
import os
from datetime import datetime
# 第三方库导入
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

# 本地模块导入
from lib.db_utils import (
    execute_sql_query,
    get_db_connection,
    get_db_connection_form,
    get_db_schema,
)
from lib.llm_utils import call_xiyan_sql_api, cached_get_client
from lib.process_utils import process_ocr, process_tabular_file

# 加载环境变量
load_dotenv("./docker/.env")

# VL模型
VL_MODEL_BASEURL = os.getenv("VL_MODEL_BASEURL")
VL_MODEL_KEY = os.getenv("VL_MODEL_KEY")
VL_MODEL_NAME = os.getenv("VL_MODEL_NAME")
# SQL模型
SQL_MODEL_BASEURL = os.getenv("SQL_MODEL_BASEURL")
SQL_MODEL_KEY = os.getenv("SQL_MODEL_KEY")
SQL_MODEL_NAME = os.getenv("SQL_MODEL_NAME")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="析言数据分析助手", layout="wide")

# --- 主标题（必须放在主内容区最前面）---
st.markdown(
    """
    <h1>析言数据分析助手</h1>
    """,
    unsafe_allow_html=True
)
st.caption("上传您的数据文件（CSV, Excel, 图片, PDF），然后用自然语言提问吧！")

# --- LLM初始化（use llm_utils） ---
sql_client = cached_get_client(st, SQL_MODEL_BASEURL, SQL_MODEL_KEY, "SQL")
vl_client = cached_get_client(st, VL_MODEL_BASEURL, VL_MODEL_KEY, "VL")

# --- 数据库连接 (Using db_utils) ---
class SessionManager:
    """会话管理器类，负责管理多个用户会话状态"""
    def __init__(self):
        """初始化会话管理器
        - sessions: 存储所有会话的字典
        - current_session_id: 当前会话ID，使用时间戳和随机数生成
        """
        self.sessions = {}
        # 使用时间戳和随机数生成会话ID，确保唯一性
        self.current_session_id = f"session_{os.urandom(4).hex()}"
        logger.info(f"初始化会话管理器，当前会话ID: {self.current_session_id}")

    def get_session(self, session_id=None):
        """获取指定会话，如果不存在则创建新会话
        Args:
            session_id: 可选参数，指定要获取的会话ID
        Returns:
            返回指定或当前会话的状态字典
        """
        session_id = session_id or self.current_session_id
        if session_id not in self.sessions:
            logger.info(f"为新会话ID创建状态: {session_id}")
            # 初始化会话状态数据结构
            self.sessions[session_id] = {
                'db_connection': None,  # 数据库连接对象
                'db_config': None,      # 数据库连接配置
                'created_tables': [],   # 已创建的表
                'query_params': {},     # 查询参数
                'user_query': '',       # 用户查询内容
                'uploaded_files': [],   # 上传的文件列表
                'file_uploader_key': f"uploader_{os.urandom(4).hex()}",  # 文件上传器唯一键
                'uploaded_file_names': [],  # 上传文件名列表
                'uploaded_tables': [],  # 已上传的表名列表
                'sql_query_history': [],  # SQL查询历史
                'query_result_df': None,  # 查询结果DataFrame
                'query_result_colnames': None,  # 查询结果列名
                'last_error': None      # 最后错误信息
            }
        return self.sessions[session_id]

    def cleanup_old_sessions(self, max_age_seconds=1800):
        """清理过期会话
        Args:
            max_age_seconds: 会话最大存活时间(秒)，默认0.5小时
        """
        current_time = datetime.now().timestamp()
        expired_sids = []
        # 遍历所有会话ID，找出过期的会话
        for sid in list(self.sessions.keys()):
            session_data = self.sessions.get(sid)
            if session_data and session_data.get('created_time'):
                if current_time - session_data['created_time'] > max_age_seconds:
                    expired_sids.append(sid)

        # 清理过期会话
        if expired_sids:
            logger.info(f"清理 {len(expired_sids)} 个过期会话: {expired_sids}")
            for sid in expired_sids:
                session_data = self.sessions.get(sid)
                # 关闭会话中的数据库连接
                if session_data and session_data.get('db_connection'):
                    try:
                        conn_to_close = session_data['db_connection']
                        if conn_to_close and not conn_to_close.closed:
                             conn_to_close.close()
                             logger.info(f"已关闭过期会话 {sid} 的数据库连接")
                    except Exception as e:
                        logger.error(f"关闭会话 {sid} 的数据库连接时出错: {e}")
                # 从会话字典中移除过期会话
                self.sessions.pop(sid, None)

# 初始化会话管理器(如果不存在)
if 'session_manager' not in st.session_state:
    st.session_state.session_manager = SessionManager()

# --- 会话管理 ---
# 获取会话管理器实例
session_manager = st.session_state.session_manager
# 清理过期会话
session_manager.cleanup_old_sessions() 
# 获取当前会话状态
current_session = session_manager.get_session() 

# --- UI 辅助函数 ---
def display_results(dataframe, query_context="query_result"):
    """在Streamlit中显示DataFrame结果和下载按钮"""
    if dataframe is not None and not dataframe.empty:
        st.dataframe(dataframe.head(10)) # 默认只显示前10行
        csv = dataframe.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="下载完整表格 (CSV)",
            data=csv,
            file_name=f'{query_context}.csv',
            mime='text/csv',
            key=f'download_{query_context}_{datetime.now().timestamp()}'
        )

# --- 会话管理初始化 ---
if 'sessions' not in st.session_state:
    st.session_state.sessions = [{
        "name": "新查询",
        "history": [],
        "generated_sql": "",
        "edited_sql": "",
        "sql_result_df": None,
        "sql_result_message": None,
        "uploaded_tables": [],
        "db_conn": None,  # 独立数据库连接
        "db_config": None # 独立数据库配置
    }]
if 'active_session_idx' not in st.session_state:
    st.session_state.active_session_idx = 0

# --- 侧边栏会话管理 ---
with st.sidebar:
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            "<div style='font-size:1.5rem;font-weight:700;margin-bottom:0.5em;'>会话管理</div>",
            unsafe_allow_html=True
        )
    with col2:
        if st.button("➕", help="新建会话"):
            st.session_state.sessions.append({
                "name": "新查询",
                "history": [],
                "generated_sql": "",
                "edited_sql": "",
                "sql_result_df": None,
                "sql_result_message": None,
                "uploaded_tables": [],
                "db_conn": None,  
                "db_config": None 
            })
            st.session_state.active_session_idx = len(st.session_state.sessions) - 1
            st.rerun()


    session_names = [s["name"] for s in st.session_state.sessions]
    for idx, name in enumerate(session_names):
        is_active = idx == st.session_state.active_session_idx
        if st.button(
            name,
            key=f"session_btn_{idx}",
            use_container_width=True,
            type="primary" if is_active else "secondary"
        ):
            st.session_state.active_session_idx = idx
            st.rerun()
        

# --- 当前会话引用 ---
# 从会话列表中获取当前活跃会话
current_session = st.session_state.sessions[st.session_state.active_session_idx]

# --- 独立数据库连接和表格状态 ---
# 每个会话独立管理以下状态:
# db_conn: 数据库连接对象
# db_config: 数据库连接配置
# uploaded_tables: 已上传的表名列表
if "db_conn" not in current_session:
    current_session["db_conn"] = None
if "db_config" not in current_session:
    current_session["db_config"] = None
if "uploaded_tables" not in current_session:
    current_session["uploaded_tables"] = []

# 初始化数据库连接和会话状态（每个会话独立）
if current_session["db_conn"] is None or (hasattr(current_session["db_conn"], "closed") and current_session["db_conn"].closed):
    db_config = current_session["db_config"]
    if not db_config:
        db_config = get_db_connection_form(st)
        if db_config:
            current_session["db_config"] = db_config
    if db_config and current_session["db_conn"] is None:
        current_session["db_conn"] = get_db_connection(st, db_config)
else:
    try:
        with current_session["db_conn"].cursor() as cur:
            cur.execute('SELECT 1')
        st.success("数据库已连接")
    except:
        current_session["db_conn"] = None
        st.warning("数据库连接已断开，请重新连接")

# --- 文件上传区域 ---
st.header("1. 上传数据文件")
uploaded_files = st.file_uploader(
    "选择CSV, XLS, XLSX, JPG, PNG, 或 PDF 文件",
    accept_multiple_files=True,
    type=['csv', 'xls', 'xlsx', 'jpg', 'png', 'pdf']
)

conn = current_session["db_conn"]

if uploaded_files and conn:
    progress_bar = st.progress(0)
    status_text = st.empty()
    processed_count = 0
    newly_uploaded_tables = []

    for i, uploaded_file in enumerate(uploaded_files):
        status_text.text(f"正在处理文件 {i+1}/{len(uploaded_files)}: {uploaded_file.name}")
        table_name = None
        file_type = uploaded_file.type

        table_names = None
        if file_type in ['text/csv', 'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']:
            table_names = process_tabular_file(st, uploaded_file, conn)
        elif file_type.startswith('image/') or file_type == 'application/pdf':
            single_table_name = process_ocr(st, uploaded_file, conn, vl_client, VL_MODEL_NAME)
            if single_table_name:
                table_names = [single_table_name]
        else:
            st.warning(f"不支持的文件类型: {uploaded_file.name} ({file_type})")

        if table_names:
            for t_name in table_names:
                if t_name not in current_session["uploaded_tables"]:
                    newly_uploaded_tables.append(t_name)
                    current_session["uploaded_tables"].append(t_name)

        processed_count += 1
        progress_bar.progress(processed_count / len(uploaded_files))

    # tatus_text.text(f"所有文件处理完成！新增数据表: {', '.join(newly_uploaded_tables) if newly_uploaded_tables else '无'}")
    progress_bar.empty()

# 显示当前数据库中的表（仅当前会话上传的）
if current_session.get("uploaded_tables"):
    st.subheader("当前已加载的数据表:")
    st.write(", ".join(current_session["uploaded_tables"]))

# --- 自然语言查询与SQL执行区域 ---
st.header("2. 提问与分析")

# 显示聊天记录
for i, message in enumerate(current_session["history"]):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("show_sql_editor") and message.get("sql"):
            edited_sql_key = f"sql_edit_area_{i}"
            edited_sql = st.text_area(
                "编辑 SQL:",
                value=message["sql"],
                height=150,
                key=edited_sql_key
            )
            execute_button_key = f"execute_sql_button_{i}"
            if st.button("执行 SQL", key=execute_button_key):
                sql_to_execute = edited_sql
                if sql_to_execute and conn:
                    with st.spinner('正在执行SQL查询...'):
                        df_result, msg = execute_sql_query(st, conn, sql_to_execute)
                        current_session["sql_result_df"] = df_result
                        current_session["sql_result_message"] = msg
                        current_session["history"][i]["show_sql_editor"] = False
                        current_session["history"][i]["executed_sql"] = sql_to_execute
                        # 根据执行结果设置消息
                        if df_result is not None:
                            result_content = "SQL执行成功。"
                        else:
                            # 如果 df_result 为 None，说明执行失败或未返回数据，使用 msg 作为结果
                            result_content = f"SQL执行出错或未返回数据。"
                        if msg:
                            result_content += f" 信息: {msg}"
                        if df_result is not None and not df_result.empty:
                             result_content += "\n查询结果（部分）已在下方显示。"
                        elif df_result is not None and df_result.empty:
                             result_content += " 查询结果为空。"

                        current_session["history"].append({
                            "role": "assistant",
                            "content": result_content
                        })
                        current_session["generated_sql"] = ""
                        current_session["edited_sql"] = ""
                        st.session_state.plotly_fig = None
                        st.rerun()
                elif not sql_to_execute:
                    st.warning("SQL语句不能为空。")
                else:
                    st.warning("请先连接数据库。")

# 获取用户输入
user_query = st.chat_input("请输入您的问题 (例如：'统计每个产品的销售总额')")

if user_query and conn:
    # 如果当前会话名为“新查询”，用用户输入替换
    if current_session["name"] == "新查询":
        current_session["name"] = user_query.strip()[:20]  # 最多20字
    # 清空聊天历史
    current_session["history"] = []

    # 显示用户本次查询
    current_session["history"].append({"role": "user", "content": user_query})
    with st.chat_message("user"):
        st.markdown(user_query)

    with st.spinner("正在理解您的问题并生成SQL..."):
        known_tables_tuple = tuple(sorted(current_session["uploaded_tables"]))
        db_schema = get_db_schema(st, conn, known_tables_tuple)
        if not db_schema:
            st.error("无法获取数据库结构，请检查连接或稍后再试。")
            error_msg = "无法获取数据库结构，无法生成SQL。"
            current_session["history"].append({"role": "assistant", "content": error_msg})
            with st.chat_message("assistant"):
                st.error(error_msg)
        else:
            generated_sql = call_xiyan_sql_api(st, sql_client, SQL_MODEL_NAME, user_query, db_schema)
            if generated_sql:
                current_session["generated_sql"] = generated_sql
                current_session["edited_sql"] = generated_sql
                current_session["sql_result_df"] = pd.DataFrame()
                current_session["sql_result_message"] = None
                current_session["history"].append({
                    "role": "assistant",
                    "content": f"我为您生成了以下SQL，请检查或编辑后执行：",
                    "sql": generated_sql,
                    "show_sql_editor": True
                })
                st.rerun()
            else:
                current_session["generated_sql"] = ""
                current_session["edited_sql"] = ""
                error_message = "抱歉，无法将您的问题转换为SQL查询。请尝试换一种问法。"
                current_session["history"].append({"role": "assistant", "content": error_message})
                with st.chat_message("assistant"):
                    st.error(error_message)

# --- 图表生成与显示区域 ---
if current_session.get("sql_result_df") is not None and not current_session["sql_result_df"].empty:
    st.header("3. 查询结果与图表分析")
    st.dataframe(current_session["sql_result_df"].head(10).iloc[:, :10])
    csv = current_session["sql_result_df"].to_csv(index=False).encode('utf-8')
    st.download_button(
        label="下载完整结果 (CSV)",
        data=csv,
        file_name='query_result.csv',
        mime='text/csv',
        key=f'download_query_result_{datetime.now().timestamp()}'
    )

    if len(current_session["sql_result_df"].columns) >= 2:
        st.subheader("生成图表")
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
                    current_session["sql_result_df"].columns,
                    key='x_col_select'
                )
                y_col = st.selectbox(
                    "选择Y轴数据",
                    current_session["sql_result_df"].columns,
                    index=1 if len(current_session["sql_result_df"].columns) > 1 else 0,
                    key='y_col_select'
                )
                if st.button("生成图表", key="generate_chart_button"):
                    try:
                        fig = None
                        if chart_type == "柱状图":
                            fig = px.bar(current_session["sql_result_df"], x=x_col, y=y_col, title=f'{y_col} vs {x_col}')
                        elif chart_type == "折线图":
                            fig = px.line(current_session["sql_result_df"], x=x_col, y=y_col, title=f'{y_col} vs {x_col}')
                        elif chart_type == "饼图":
                            fig = px.pie(current_session["sql_result_df"], names=x_col, values=y_col, title=f'{y_col} 分布 by {x_col}')

                        if fig:
                            st.session_state.plotly_fig = fig
                        else:
                            st.warning("无法生成所选图表类型。")
                            st.session_state.plotly_fig = None
                    except Exception as e:
                        st.error(f"生成图表时出错: {e}")
                        st.session_state.plotly_fig = None
        with col2:
            if 'plotly_fig' in st.session_state and st.session_state.plotly_fig is not None:
                st.plotly_chart(st.session_state.plotly_fig, use_container_width=True)
            else:
                st.write("请在左侧选择数据并点击“生成图表”。")
    else:
        st.info("查询结果少于两列，无法生成图表。")

elif current_session.get("sql_result_message"):
    st.header("3. 操作结果")
    st.success(current_session["sql_result_message"])

elif user_query and not conn:
    st.error("数据库未连接，请检查配置并重启应用。")

# --- 页脚 ---
st.markdown("---")
st.caption("Powered by Streamlit")