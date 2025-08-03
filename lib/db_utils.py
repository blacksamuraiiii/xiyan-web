import os
import io
import time
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from contextlib import contextmanager

# log文件配置
logger = logging.getLogger(__name__)

# --- 数据库连接池和性能优化 ---
class DatabaseConnectionPool:
    """简单的数据库连接池"""
    def __init__(self, max_connections=5):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        
    @contextmanager
    def get_connection(self, db_config):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            # 尝试从池中获取可用连接
            for i, (pooled_conn, pooled_config) in enumerate(self.connections):
                if i not in self.in_use and pooled_config == db_config:
                    # 检查连接是否仍然有效
                    try:
                        with pooled_conn.cursor() as cur:
                            cur.execute('SELECT 1')
                        conn = pooled_conn
                        self.in_use.add(i)
                        logger.info("Reusing connection from pool")
                        break
                    except:
                        # 连接已失效，从池中移除
                        self.connections.pop(i)
                        break
            
            # 如果没有可用连接，创建新连接
            if conn is None and len(self.in_use) < self.max_connections:
                conn = self._create_connection(db_config)
                if conn:
                    self.connections.append((conn, db_config))
                    self.in_use.add(len(self.connections) - 1)
                    logger.info("Created new database connection")
            
            if conn:
                yield conn
            else:
                raise Exception("No available database connections")
                
        finally:
            # 释放连接回池中
            if conn:
                for i, (pooled_conn, _) in enumerate(self.connections):
                    if pooled_conn == conn and i in self.in_use:
                        self.in_use.remove(i)
                        break
    
    def _create_connection(self, db_config):
        """创建新的数据库连接"""
        try:
            return psycopg2.connect(
                host=db_config["DB_HOST"],
                port=db_config["DB_PORT"],
                user=db_config["DB_USER"],
                password=db_config["DB_PASSWORD"],
                database=db_config["DB_DATABASE"],
                connect_timeout=5
            )
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            return None
    
    def close_all(self):
        """关闭所有连接"""
        for conn, _ in self.connections:
            try:
                conn.close()
            except:
                pass
        self.connections.clear()
        self.in_use.clear()

# 全局连接池实例
db_pool = DatabaseConnectionPool()

@contextmanager
def get_db_connection_context(db_config):
    """获取数据库连接的上下文管理器（简化版本）"""
    conn = None
    try:
        conn = get_db_connection(None, db_config)
        yield conn
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# 数据库连接配置
def get_db_connection_form(st):
    """显示数据库连接表单并返回连接参数"""
    with st.expander("数据库连接配置", expanded=False):
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
                db_config = {
                    "DB_HOST": db_host,
                    "DB_PORT": db_port,
                    "DB_USER": db_user,
                    "DB_PASSWORD": db_password,
                    "DB_DATABASE": db_name
                }
                logger.info(f"Database connection form submitted with host: {db_host}, user: {db_user}, db: {db_name}")
                return db_config
    return None

# 数据库连接
def get_db_connection(st, db_config):
    """根据配置建立数据库连接"""
    conn = None
    retries = 3 
    wait_time = 3 
    while retries > 0:
        try:
            logger.info(f"Attempting to connect to database: {db_config['DB_HOST']}:{db_config['DB_PORT']} as {db_config['DB_USER']}")
            conn = psycopg2.connect(
                host=db_config["DB_HOST"],
                port=db_config["DB_PORT"],
                user=db_config["DB_USER"],
                password=db_config["DB_PASSWORD"],
                database=db_config["DB_DATABASE"],
                connect_timeout=5 
            )
            st.session_state.db_config_expanded = False
            st.success("数据库连接成功!")
            logger.info("Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Database connection failed (attempt {4-retries}/3): {e}")
            st.warning(f"数据库连接失败，正在重试... ({retries}次剩余) 错误: {e}")
            retries -= 1
            if retries > 0:
                time.sleep(wait_time)
        except Exception as e:
             logger.error(f"Unexpected error during database connection: {e}", exc_info=True)
             st.error(f"连接数据库时发生意外错误: {e}")
             return None # Stop retrying on unexpected errors

    st.error("无法连接到数据库，请检查配置或数据库服务状态。")
    logger.error("Failed to connect to the database after multiple retries.")
    return None

# 检查表是否存在
def _check_table_exists(cur, table_name):
    """检查指定的表是否存在于数据库中"""
    check_query = sql.SQL("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)")
    cur.execute(check_query, (table_name,))
    return cur.fetchone()[0]

def check_table_exists(conn, table_name):
    """公共函数：检查指定的表是否存在于数据库中。

    Args:
        conn: Active database connection.
        table_name (str): Name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
        None: If there's an error during the check.
    """
    if not conn:
        logger.error("check_table_exists called with no database connection.")
        return None
    # Sanitize table name (important!)
    sanitized_table_name = ''.join(filter(str.isalnum, table_name)).lower()
    if not sanitized_table_name:
        logger.error(f"Invalid table name provided for existence check: '{table_name}'")
        return None
    try:
        with conn.cursor() as cur:
            return _check_table_exists(cur, sanitized_table_name)
    except psycopg2.Error as e:
        logger.error(f"Database error checking existence of table '{sanitized_table_name}': {e}", exc_info=True)
        # Optionally, display an error to the user via st if needed in the calling context
        return None
    except Exception as e:
        logger.error(f"Unexpected error checking existence of table '{sanitized_table_name}': {e}", exc_info=True)
        return None

# 检查DataFrame模式与表模式是否兼容 (仅检查列名和大致数量)
def _check_schema_compatibility(cur, table_name, df_columns):
    """检查DataFrame的列是否与现有表的列兼容（名称和数量）"""
    info_query = sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position")
    cur.execute(info_query, (table_name,))
    table_columns = [row[0] for row in cur.fetchall()]
    
    # 比较列名（转换为小写和下划线以匹配sanitized名称）和数量
    sanitized_df_columns = set(''.join(filter(lambda x: x.isalnum() or x == '_', str(col))).lower() for col in df_columns)
    sanitized_table_columns = set(table_columns) # 假设表列名已经是sanitized的

    if len(sanitized_df_columns) != len(sanitized_table_columns):
        logger.warning(f"Schema mismatch for append: Column count differs. DF: {len(sanitized_df_columns)}, Table: {len(sanitized_table_columns)}")
        return False, f"列数量不匹配 (DataFrame: {len(sanitized_df_columns)}, 表: {len(sanitized_table_columns)})"
    
    if sanitized_df_columns != sanitized_table_columns:
        logger.warning(f"Schema mismatch for append: Column names differ. DF: {sanitized_df_columns}, Table: {sanitized_table_columns}")
        # 找出差异列以提供更详细的错误信息
        df_only = sanitized_df_columns - sanitized_table_columns
        table_only = sanitized_table_columns - sanitized_df_columns
        return False, f"列名不匹配 (DataFrame独有: {df_only}, 表独有: {table_only})"

    logger.info(f"Schema compatibility check passed for table '{table_name}'.")
    return True, ""

# 写入DataFrame到数据库
def insert_dataframe_to_db(st, df, table_name, conn, if_exists='replace'):
    """将DataFrame插入到指定的数据库表中。

    Args:
        st: Streamlit object for displaying messages.
        df: Pandas DataFrame to insert.
        table_name: Name of the target database table.
        conn: Active database connection.
        if_exists (str): Action to take if table already exists.
            'replace': Drop the existing table and create a new one. (Default)
            'append': Append data to the existing table. Fails if schema doesn't match.
            'fail': Raise an error and do nothing if the table exists.
    Returns:
        bool: True if insertion was successful, False otherwise.
    """
    if df is None or df.empty:
        st.warning(f"提供的 DataFrame 为空，无法操作表 '{table_name}'。")
        logger.warning(f"Attempted operation on an empty DataFrame for table '{table_name}'.")
        return False
    try:
        original_table_name = table_name # 保留原始名称用于消息
        # Sanitize table name (important!)
        sanitized_table_name = ''.join(filter(str.isalnum, table_name)).lower()
        if not sanitized_table_name:
             st.error(f"无法为 '{original_table_name}' 生成有效的表名进行操作。")
             logger.error(f"Invalid table name generated for DataFrame operation from '{original_table_name}'.")
             return False

        logger.info(f"Starting data operation on table '{sanitized_table_name}' (requested: '{original_table_name}'). Mode: {if_exists}. DataFrame shape: {df.shape}")
        
        # Note: Table existence check is now primarily handled *before* calling this function
        # in the application logic (process_utils.py) when user interaction is needed.
        # This function still needs internal checks for 'fail' and 'append' modes.

        with conn.cursor() as cur:
            # Check existence *within* the transaction for safety, especially for 'fail' and 'append'
            table_exists = _check_table_exists(cur, sanitized_table_name)

            if table_exists:
                logger.info(f"Table '{sanitized_table_name}' already exists.")
                if if_exists == 'fail':
                    st.error(f"表 '{sanitized_table_name}' 已存在，操作已中止 (策略: fail)。")
                    logger.error(f"Operation aborted: Table '{sanitized_table_name}' exists and if_exists='fail'.")
                    return False
                elif if_exists == 'replace':
                    # Warning moved to calling function (process_utils.py) where user confirms
                    # st.warning(f"表 '{sanitized_table_name}' 已存在，将执行删除并重建 (策略: replace)。")
                    logger.warning(f"Table '{sanitized_table_name}' exists. Dropping and recreating as per 'replace' strategy.")
                    try:
                        drop_query = sql.SQL("DROP TABLE IF EXISTS {table_name}").format(table_name=sql.Identifier(sanitized_table_name))
                        logger.debug(f"Executing: {drop_query.as_string(cur)}")
                        cur.execute(drop_query)
                        # 不需要立即提交，将在创建和复制后提交
                        table_exists = False # 标记为不存在，以便后续创建
                    except psycopg2.Error as e:
                        logger.error(f"Error dropping table '{sanitized_table_name}' for replacement: {e}", exc_info=True)
                        st.error(f"替换表 '{sanitized_table_name}' 时删除失败: {e}")
                        conn.rollback()
                        return False
                elif if_exists == 'append':
                    # Info moved to calling function (process_utils.py) where user confirms
                    # st.info(f"表 '{sanitized_table_name}' 已存在，将尝试追加数据 (策略: append)。")
                    logger.info(f"Table '{sanitized_table_name}' exists. Attempting to append data.")
                    # 检查模式兼容性
                    compatible, reason = _check_schema_compatibility(cur, sanitized_table_name, df.columns)
                    if not compatible:
                        st.error(f"无法追加数据到表 '{sanitized_table_name}'：模式不兼容。原因: {reason}")
                        logger.error(f"Append failed for table '{sanitized_table_name}': Schema incompatibility. Reason: {reason}")
                        return False
                    # 如果兼容，则直接进入数据插入阶段，跳过表创建
                    pass # 继续执行插入逻辑
                else:
                    st.error(f"无效的 'if_exists' 策略: '{if_exists}'。请使用 'replace', 'append', 或 'fail'。")
                    logger.error(f"Invalid if_exists strategy: '{if_exists}'")
                    return False
            
            # --- 列名清理和类型推断 --- (对replace和append都需要)
            sanitized_columns = {}
            df_renamed = df.copy() # 创建副本以重命名列
            for i, col in enumerate(df_renamed.columns):
                # 更健壮的清理：保留下划线，确保以字母或下划线开头
                sanitized_col = ''.join(filter(lambda x: x.isalnum() or x == '_', str(col))).lower()
                if not sanitized_col or not (sanitized_col[0].isalpha() or sanitized_col[0] == '_'):
                    sanitized_col = f'_col_{i}' # 如果清理后为空或以数字开头，则强制重命名
                
                # 处理潜在的重复列名
                original_sanitized = sanitized_col
                count = 1
                while sanitized_col in sanitized_columns.values():
                    sanitized_col = f"{original_sanitized}_{count}"
                    count += 1
                sanitized_columns[col] = sanitized_col
            df_renamed = df_renamed.rename(columns=sanitized_columns)
            logger.debug(f"DataFrame columns sanitized: {sanitized_columns}")

            # --- 表创建逻辑 (仅当表不存在或 if_exists == 'replace') ---
            if not table_exists:
                logger.info(f"Creating new table '{sanitized_table_name}'.")
                # 类型推断函数
                def infer_sql_type(dtype):
                    if pd.api.types.is_integer_dtype(dtype):
                        return 'BIGINT'
                    elif pd.api.types.is_float_dtype(dtype):
                        return 'DOUBLE PRECISION'
                    elif pd.api.types.is_numeric_dtype(dtype):
                        return 'NUMERIC'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        # 检查时区信息
                        if getattr(dtype, 'tz', None) is not None:
                            return 'TIMESTAMP WITH TIME ZONE'
                        else:
                            return 'TIMESTAMP WITHOUT TIME ZONE'
                    elif pd.api.types.is_bool_dtype(dtype):
                        return 'BOOLEAN'
                    else:
                        return 'TEXT' # 默认使用TEXT

                columns_sql = []
                for col_original in df.columns: # 使用原始df的列顺序和类型
                    sanitized_col_name = sanitized_columns[col_original]
                    col_type = infer_sql_type(df[col_original].dtype)
                    columns_sql.append(sql.SQL("{col} {type}").format(
                        col=sql.Identifier(sanitized_col_name),
                        type=sql.SQL(col_type)
                    ))
                    logger.debug(f"Column '{col_original}' (sanitized: '{sanitized_col_name}') inferred as SQL type: {col_type}")
                
                create_query = sql.SQL("CREATE TABLE {table_name} ({columns})").format(
                    table_name=sql.Identifier(sanitized_table_name),
                    columns=sql.SQL(", ").join(columns_sql)
                )
                try:
                    logger.debug(f"Executing: {create_query.as_string(cur)}")
                    cur.execute(create_query)
                except psycopg2.Error as e:
                    logger.error(f"Error creating table '{sanitized_table_name}': {e}", exc_info=True)
                    st.error(f"创建新表 '{sanitized_table_name}' 失败: {e}")
                    conn.rollback()
                    return False

            # --- 数据清洗和预处理 (对所有插入/追加操作) ---
            df_copy = df_renamed.copy() # 使用重命名后的列进行清洗
            for col in df_copy.columns:
                # 仅对 object 或 string 类型尝试清洗
                if pd.api.types.is_string_dtype(df_copy[col].dtype) or df_copy[col].dtype == 'object':
                    try:
                        # 确保列是字符串类型以应用 .str 访问器
                        df_copy[col] = df_copy[col].astype(str).str.strip()
                        # 替换多种表示空值的方式为 NaN
                        df_copy[col] = df_copy[col].replace(['', 'NULL', 'null', 'NA', 'N/A', '#N/A', 'nan', 'NaN'], np.nan)
                    except AttributeError:
                         # 这通常不应发生，因为我们已经检查了类型
                         logger.warning(f"Could not apply string strip/replace to column '{col}' in table '{sanitized_table_name}'. It might contain non-string data despite initial check.")
            
            # --- 数据插入 (使用COPY FROM) ---
            buffer = io.StringIO()
            # 使用更安全的CSV写入，确保正确处理引号和分隔符
            df_copy.to_csv(buffer, index=False, header=False, sep=',', na_rep='', quoting=1) # quoting=1 means csv.QUOTE_ALL
            buffer.seek(0)

            # 构建COPY命令，指定列名以确保顺序正确
            copy_columns = sql.SQL(',').join(map(sql.Identifier, df_copy.columns))
            copy_query = sql.SQL("COPY {table_name} ({columns}) FROM stdin WITH (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')").format(
                 table_name=sql.Identifier(sanitized_table_name),
                 columns=copy_columns
            )
            try:
                logger.debug(f"Executing COPY command for table '{sanitized_table_name}'")
                cur.copy_expert(sql=copy_query, file=buffer)
                conn.commit() # 仅在成功时提交
                action_verb = "追加" if (table_exists and if_exists == 'append') else "导入"
                # st.success(f"成功将数据{action_verb}到表 '{sanitized_table_name}'。")
                logger.info(f"Successfully {action_verb} data into table '{sanitized_table_name}'.")
                return True
            except psycopg2.Error as e:
                logger.error(f"Database error during COPY into '{sanitized_table_name}': {e}", exc_info=True)
                st.error(f"将数据导入/追加到表 '{sanitized_table_name}' 时数据库出错: {e}")
                conn.rollback()
                return False
            except Exception as e: # 捕获其他潜在错误，例如内存问题
                logger.error(f"Unexpected error during COPY into '{sanitized_table_name}': {e}", exc_info=True)
                st.error(f"将数据导入/追加到表 '{sanitized_table_name}' 时发生意外错误: {e}")
                conn.rollback()
                return False

    except psycopg2.Error as e:
        # 处理在 try 块开始处或 with conn.cursor() 之前的 psycopg2 错误
        logger.error(f"Database error during operation on '{table_name}': {e}", exc_info=True)
        st.error(f"处理表 '{table_name}' 时数据库出错: {e}")
        if conn:
            try:
                conn.rollback() # 尝试回滚
            except psycopg2.Error as rb_e:
                logger.error(f"Error during rollback attempt: {rb_e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during operation on '{table_name}': {e}", exc_info=True)
        st.error(f"处理表 '{table_name}' 时发生意外错误: {e}")
        if conn:
            try:
                conn.rollback()
            except psycopg2.Error as rb_e:
                logger.error(f"Error during rollback attempt: {rb_e}")
        return False

# 获取所有表名
def get_all_table_names(st, conn):
    """获取数据库中所有用户表的名称"""
    if not conn:
        st.error("数据库未连接，无法获取表列表。")
        logger.error("get_all_table_names called with no database connection.")
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
            """)
            tables = [row[0] for row in cur.fetchall()]
            logger.info(f"Retrieved {len(tables)} table names from public schema.")
            return tables
    except psycopg2.Error as db_err:
        st.error(f"获取表列表时出错: {db_err}")
        logger.error(f"Database error getting table names: {db_err}", exc_info=True)
        return None
    except Exception as e:
        st.error(f"获取表列表时发生意外错误: {e}")
        logger.error(f"Unexpected error getting table names: {e}", exc_info=True)
        return None

# 获取表结构
def get_db_schema(st, conn, known_tables):
    """获取数据库中指定表的结构信息"""
    if not known_tables:
        logger.warning("get_db_schema called with no known tables.")
        return {}
    if not conn:
        st.error("数据库未连接，无法获取表结构。")
        logger.error("get_db_schema called with no database connection.")
        return None

    schema = {}
    try:
        with conn.cursor() as cur:
            # Ensure known_tables is a list/tuple of strings
            if not isinstance(known_tables, (list, tuple)):
                 logger.error(f"known_tables must be a list or tuple, got {type(known_tables)}")
                 return None
            if not all(isinstance(t, str) for t in known_tables):
                 logger.error(f"All elements in known_tables must be strings.")
                 return None

            # Use parameterized query for safety, although table names aren't typically user input here
            # Constructing IN clause safely
            if not known_tables: return {} # Return empty if list is empty after validation
            placeholders = sql.SQL(',').join(sql.Placeholder() * len(known_tables))
            query = sql.SQL("""
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name IN ({})
                ORDER BY table_name, ordinal_position;
            """).format(placeholders)

            logger.debug(f"Fetching schema for tables: {known_tables}")
            cur.execute(query, known_tables)

            rows = cur.fetchall()
            for table_name, column_name, data_type in rows:
                if table_name not in schema:
                    schema[table_name] = {}
                schema[table_name][column_name] = data_type
            logger.info(f"Successfully retrieved schema for tables: {list(schema.keys())}")
        return schema
    except psycopg2.Error as db_err:
        st.error(f"获取数据库结构时出错: {db_err}")
        logger.error(f"Database error getting schema: {db_err}", exc_info=True)
        return None
    except Exception as e:
        st.error(f"获取数据库结构时发生意外错误: {e}")
        logger.error(f"Unexpected error getting schema: {e}", exc_info=True)
        return None

# 获取表数据
def get_table_data(st, conn, table_name, limit=100):
    """获取指定表的数据（带有限制）"""
    if not conn:
        st.error("数据库未连接，无法获取表数据。")
        logger.error(f"get_table_data called for '{table_name}' with no database connection.")
        return None
    
    # Sanitize table name
    sanitized_table_name = ''.join(filter(str.isalnum, table_name)).lower()
    if not sanitized_table_name:
        st.error(f"无法为 '{table_name}' 生成有效的表名以获取数据。")
        logger.error(f"Invalid table name generated for data retrieval from '{table_name}'.")
        return None

    try:
        with conn.cursor() as cur:
            # Check if table exists first
            if not _check_table_exists(cur, sanitized_table_name):
                st.error(f"表 '{sanitized_table_name}' 不存在。")
                logger.error(f"Attempted to get data from non-existent table '{sanitized_table_name}'.")
                return None

            query = sql.SQL("SELECT * FROM {table} LIMIT %s").format(table=sql.Identifier(sanitized_table_name))
            logger.debug(f"Fetching data from '{sanitized_table_name}' with limit {limit}")
            cur.execute(query, (limit,))
            colnames = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            df = pd.DataFrame(data, columns=colnames)
            logger.info(f"Successfully retrieved {len(df)} rows from '{sanitized_table_name}'.")
            return df
    except psycopg2.Error as db_err:
        st.error(f"获取表 '{sanitized_table_name}' 数据时出错: {db_err}")
        logger.error(f"Database error getting data from '{sanitized_table_name}': {db_err}", exc_info=True)
        return None
    except Exception as e:
        st.error(f"获取表 '{sanitized_table_name}' 数据时发生意外错误: {e}")
        logger.error(f"Unexpected error getting data from '{sanitized_table_name}': {e}", exc_info=True)
        return None

# 删除表
def delete_table(st, conn, table_name):
    """删除指定的数据库表"""
    if not conn:
        st.error("数据库未连接，无法删除表。")
        logger.error(f"delete_table called for '{table_name}' with no database connection.")
        return False

    # Sanitize table name
    sanitized_table_name = ''.join(filter(str.isalnum, table_name)).lower()
    if not sanitized_table_name:
        st.error(f"无法为 '{table_name}' 生成有效的表名以进行删除。")
        logger.error(f"Invalid table name generated for deletion from '{table_name}'.")
        return False

    try:
        with conn.cursor() as cur:
            # Check if table exists first
            if not _check_table_exists(cur, sanitized_table_name):
                st.warning(f"表 '{sanitized_table_name}' 不存在，无需删除。")
                logger.warning(f"Attempted to delete non-existent table '{sanitized_table_name}'.")
                return True # Consider it successful as the table is gone

            drop_query = sql.SQL("DROP TABLE IF EXISTS {table}").format(table=sql.Identifier(sanitized_table_name))
            logger.info(f"Attempting to drop table '{sanitized_table_name}'.")
            cur.execute(drop_query)
            conn.commit()
            st.success(f"表 '{sanitized_table_name}' 已成功删除。")
            logger.info(f"Successfully dropped table '{sanitized_table_name}'.")
            return True
    except psycopg2.Error as db_err:
        st.error(f"删除表 '{sanitized_table_name}' 时出错: {db_err}")
        logger.error(f"Database error dropping table '{sanitized_table_name}': {db_err}", exc_info=True)
        conn.rollback()
        return False
    except Exception as e:
        st.error(f"删除表 '{sanitized_table_name}' 时发生意外错误: {e}")
        logger.error(f"Unexpected error dropping table '{sanitized_table_name}': {e}", exc_info=True)
        conn.rollback()
        return False

# SQL代码审计
def validate_sql(sql_query):
    """SQL验证函数，防止危险操作 (basic check)"""
    # Basic check for keywords that modify data or structure outside of SELECT
    # This is NOT foolproof security, but a basic safeguard.
    forbidden_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'UPDATE', 'INSERT', 'GRANT', 'REVOKE', 'ALTER']
    # Check for whole words to avoid matching substrings like 'UPDATEd'
    sql_upper = sql_query.upper()
    for keyword in forbidden_keywords:
        # Use regex to find whole words
        import re
        if re.search(r'\b' + keyword + r'\b', sql_upper):
            logger.warning(f"Potentially dangerous SQL keyword '{keyword}' detected in query: {sql_query}")
            raise ValueError(f"检测到可能修改数据的操作 ({keyword})，已阻止执行。仅允许执行 SELECT 查询。")
    logger.info("SQL query passed basic validation.")
    return True

# 执行SQL查询
def execute_sql_query(st, conn, sql_query, params=None):
    """执行SQL查询并返回结果DataFrame和列名"""
    if not conn:
        st.error("数据库未连接，无法执行查询。")
        logger.error("execute_sql_query called with no database connection.")
        return None, None
    if not sql_query:
        st.warning("没有提供 SQL 查询语句。")
        logger.warning("execute_sql_query called with empty query.")
        return None, None

    try:
        # Validate the SQL query first (basic check)
        validate_sql(sql_query)

        logger.info(f"Executing SQL query: {sql_query}")
        if params:
             logger.info(f"With parameters: {params}")

        with conn.cursor() as cur:
            start_time = time.time()
            cur.execute(sql_query, params if params else None)
            execution_time = time.time() - start_time
            logger.info(f"SQL query executed successfully in {execution_time:.3f} seconds.")

            # Check if the query was a SELECT statement that returns rows
            if cur.description:
                colnames = [desc[0] for desc in cur.description]
                results = cur.fetchall()
                logger.info(f"Query returned {len(results)} rows.")
                if results:
                    df = pd.DataFrame(results, columns=colnames)
                    return df, colnames
                else:
                    # Return empty DataFrame with correct columns if no rows found
                    df = pd.DataFrame([], columns=colnames)
                    return df, colnames
            else:
                # Handle non-SELECT queries or queries with no return (e.g., SET commands if allowed)
                conn.commit() # Commit if it was a non-returning, valid query
                logger.info("Query executed but did not return rows (e.g., SET command).")
                st.info("查询已执行，但没有返回数据。")
                return None, None

    except ValueError as ve:
        # Catch validation errors specifically
        # 捕获验证错误
        error_msg = f"SQL 查询验证失败: {ve}"
        st.error(error_msg)
        logger.error(f"SQL validation failed: {ve}. Query: {sql_query}")
        return None, error_msg # 返回错误消息
    except psycopg2.Error as db_err:
        st.error(f"执行 SQL 查询时出错: {db_err}")
        logger.error(f"Database error executing query: {db_err}. Query: {sql_query}", exc_info=True)
        conn.rollback() # Rollback on error
        return None, None
    except Exception as e:
        st.error(f"执行 SQL 查询时发生意外错误: {e}")
        logger.error(f"Unexpected error executing query: {e}. Query: {sql_query}", exc_info=True)
        conn.rollback()
        return None, None