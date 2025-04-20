import os
import io
import time
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql

# log文件配置
logger = logging.getLogger(__name__)

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

# 写入DataFrame到数据库
def insert_dataframe_to_db(st, df, table_name, conn):
    """将DataFrame插入到指定的数据库表中(覆盖现有表)"""
    if df is None or df.empty:
        st.warning(f"提供的 DataFrame 为空，无法导入表 '{table_name}'。")
        logger.warning(f"Attempted to insert an empty DataFrame into table '{table_name}'.")
        return False
    try:
        table_name = ''.join(filter(str.isalnum, table_name)).lower() # Ensure table name is sanitized
        if not table_name:
             st.error("无法生成有效的表名进行导入。")
             logger.error("Invalid table name generated for DataFrame insertion.")
             return False

        logger.info(f"Starting data insertion into table '{table_name}'. DataFrame shape: {df.shape}")
        with conn.cursor() as cur:
            # 检查表是否存在，如果存在则删除重建
            drop_query = sql.SQL("DROP TABLE IF EXISTS {table_name}").format(
                table_name=sql.Identifier(table_name)
            )
            logger.debug(f"Executing: {drop_query.as_string(cur)}")
            cur.execute(drop_query)
            # No commit needed immediately, will commit after create and copy

            # 类型推断函数
            def infer_sql_type(dtype):
                if pd.api.types.is_integer_dtype(dtype):
                    return 'BIGINT' # Use BIGINT for integers
                elif pd.api.types.is_float_dtype(dtype):
                    return 'DOUBLE PRECISION' # Use DOUBLE PRECISION for floats
                elif pd.api.types.is_numeric_dtype(dtype):
                    return 'NUMERIC' # Fallback for other numeric types
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    return 'TIMESTAMP' # Keep TIMESTAMP
                elif pd.api.types.is_bool_dtype(dtype):
                    return 'BOOLEAN' # Keep BOOLEAN
                else:
                    return 'TEXT' # Default to TEXT

            # 创建表结构（根据数据类型推断列类型）
            columns = []
            sanitized_columns = {}
            for i, col in enumerate(df.columns):
                # Sanitize column names (lowercase, alphanumeric + underscore, start with letter or underscore)
                sanitized_col = ''.join(filter(lambda x: x.isalnum() or x == '_', str(col))).lower()
                if not sanitized_col or not (sanitized_col[0].isalpha() or sanitized_col[0] == '_'):
                    sanitized_col = f'_col_{i}' # Fallback if sanitization fails or starts with number
                # Handle potential duplicate sanitized names
                original_sanitized = sanitized_col
                count = 1
                while sanitized_col in sanitized_columns.values():
                    sanitized_col = f"{original_sanitized}_{count}"
                    count += 1
                sanitized_columns[col] = sanitized_col

                col_type = infer_sql_type(df[col].dtype)
                columns.append(sql.SQL("{col} {type}").format(
                    col=sql.Identifier(sanitized_col),
                    type=sql.SQL(col_type)
                ))
                logger.debug(f"Column '{col}' (sanitized: '{sanitized_col}') inferred as SQL type: {col_type}")

            # Rename DataFrame columns to sanitized versions before insertion
            df = df.rename(columns=sanitized_columns)

            create_query = sql.SQL("CREATE TABLE {table_name} ({columns})").format(
                table_name=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(columns)
            )
            logger.debug(f"Executing: {create_query.as_string(cur)}")
            cur.execute(create_query)

            # 数据清洗和预处理 (Applied on the renamed df)
            df_copy = df.copy() # Work on a copy to avoid SettingWithCopyWarning
            for col in df_copy.columns:
                # Handle string types
                if pd.api.types.is_string_dtype(df_copy[col].dtype) or df_copy[col].dtype == 'object': # Include object type
                    try:
                        df_copy[col] = df_copy[col].str.strip()
                        # Replace empty strings and common null representations with actual NaN
                        df_copy[col] = df_copy[col].replace(['', 'NULL', 'null', 'NA', 'N/A', '#N/A'], np.nan)
                    except AttributeError:
                         # Handle cases where a column might have mixed types and .str fails
                         logger.warning(f"Could not apply string strip/replace to column '{col}' in table '{table_name}'. It might contain non-string data.")
                # Convert potential numeric strings safely before insertion if column is TEXT
                # Note: COPY handles type conversion generally, but this pre-cleans common issues
                # This part is complex and might be better handled by letting COPY fail and report errors
                # For now, we rely on COPY's robustness and the inferred types.

            # 插入数据 (使用COPY FROM提高效率)
            buffer = io.StringIO()
            # Use na_rep='' which COPY interprets as NULL for TEXT columns.
            # For numeric types, pandas correctly writes NaN as empty string.
            # Ensure quoting handles commas and quotes within fields correctly.
            df_copy.to_csv(buffer, index=False, header=False, sep=',', na_rep='', quoting=1) # quoting=1 (csv.QUOTE_ALL)
            buffer.seek(0)

            copy_query = sql.SQL("COPY {table_name} FROM stdin WITH (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')").format(
                 table_name=sql.Identifier(table_name)
            )
            logger.debug(f"Executing COPY command for table '{table_name}'")
            cur.copy_expert(sql=copy_query, file=buffer)
            conn.commit() # Commit after successful COPY
            logger.info(f"Successfully inserted data into table '{table_name}'.")
        return True
    except psycopg2.Error as e:
        logger.error(f"Database error during insertion into '{table_name}': {e}", exc_info=True)
        st.error(f"将数据导入表 '{table_name}' 时数据库出错: {e}")
        if conn:
            conn.rollback() # Rollback on error
        return False
    except Exception as e:
        logger.error(f"Unexpected error during insertion into '{table_name}': {e}", exc_info=True)
        st.error(f"将数据导入表 '{table_name}' 时发生意外错误: {e}")
        if conn:
            conn.rollback()
        return False

# 查询数据库表结构
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
        st.error(f"SQL 查询验证失败: {ve}")
        logger.error(f"SQL validation failed: {ve}. Query: {sql_query}")
        return None, None
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