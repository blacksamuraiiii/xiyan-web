import os
import io
import base64
import logging
import pandas as pd
import numpy as np
import chardet
import fitz  # PyMuPDF
import time # 导入 time 模块
from .llm_utils import call_vl_api
from .db_utils import insert_dataframe_to_db, check_table_exists

# log文件配置
logger = logging.getLogger(__name__)

# --- 统一错误处理函数 ---
def handle_error(st, error_message, exception=None, error_code=None, user_suggestion=None):
    """统一的错误处理函数，提供标准化的错误消息格式
    
    Args:
        st: Streamlit对象
        error_message: 错误消息
        exception: 异常对象（可选）
        error_code: 错误代码（可选）
        user_suggestion: 用户建议（可选）
    """
    # 构建完整的错误消息
    full_message = error_message
    
    if error_code:
        full_message = f"错误代码: {error_code} - {full_message}"
    
    if user_suggestion:
        full_message += f"\n\n💡 建议: {user_suggestion}"
    
    # 显示给用户
    st.error(full_message)
    
    # 记录日志
    if exception:
        logger.error(f"{error_message} - Exception: {str(exception)}", exc_info=True)
    else:
        logger.error(error_message)

def show_success(st, success_message, details=None):
    """统一的成功消息显示函数
    
    Args:
        st: Streamlit对象
        success_message: 成功消息
        details: 详细信息（可选）
    """
    if details:
        full_message = f"✅ {success_message}\n\n{details}"
    else:
        full_message = f"✅ {success_message}"
    
    st.success(full_message)
    logger.info(success_message)

def show_warning(st, warning_message, details=None):
    """统一的警告消息显示函数
    
    Args:
        st: Streamlit对象
        warning_message: 警告消息
        details: 详细信息（可选）
    """
    if details:
        full_message = f"⚠️ {warning_message}\n\n{details}"
    else:
        full_message = f"⚠️ {warning_message}"
    
    st.warning(full_message)
    logger.warning(warning_message)

def show_info(st, info_message, details=None):
    """统一的信息消息显示函数
    
    Args:
        st: Streamlit对象
        info_message: 信息消息
        details: 详细信息（可选）
    """
    if details:
        full_message = f"ℹ️ {info_message}\n\n{details}"
    else:
        full_message = f"ℹ️ {info_message}"
    
    st.info(full_message)
    logger.info(info_message)


# --- 主处理函数 ---
def process_uploaded_files(st, uploaded_files, conn, vl_client, vl_model_name):
    """处理上传的文件列表，根据类型分发处理，并处理表存在逻辑。"""
    if not conn:
        st.error("数据库未连接，无法处理文件。")
        return
    if not uploaded_files:
        st.info("请先上传文件。")
        return

    processed_tables = []
    files_pending_confirmation = []

    # 第一遍：处理文件，识别需要用户确认的表
    for uploaded_file in uploaded_files:
        file_type = uploaded_file.type
        file_name = uploaded_file.name
        logger.info(f"Processing file: {file_name}, type: {file_type}")

        if file_type in ['text/csv', 'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']:
            # 表格文件可能产生多个表（Excel sheets）
            base_file_name = os.path.splitext(file_name)[0]
            original_base_table_name = base_file_name

            if file_name.endswith('.csv'):
                # 检查CSV对应的表是否存在
                sanitized_name = ''.join(filter(str.isalnum, original_base_table_name)).lower()
                if sanitized_name and check_table_exists(conn, sanitized_name):
                    files_pending_confirmation.append({'file': uploaded_file, 'type': 'csv', 'original_name': original_base_table_name})
                else:
                    # 表不存在，直接处理
                    result = process_tabular_file(st, uploaded_file, conn)
                    if result: processed_tables.extend(result)
            elif file_name.endswith(('.xls', '.xlsx')):
                 # 增强的Excel预检查：多引擎支持
                 engines = ['calamine', 'openpyxl', 'xlrd']
                 excel_data = None
                 successful_engine = None
                 
                 for engine in engines:
                     try:
                         uploaded_file.seek(0)
                         excel_data = pd.read_excel(uploaded_file, sheet_name=None, engine=engine)
                         if excel_data:
                             successful_engine = engine
                             logger.info(f"Successfully read Excel {file_name} using engine: {engine} during pre-check")
                             break
                     except ImportError:
                         continue
                     except Exception as e:
                         logger.warning(f"Engine {engine} failed for {file_name} during pre-check: {e}")
                         continue
                 
                 if excel_data is None:
                     st.error(f"无法读取Excel文件 '{file_name}'，已尝试所有可用的解析引擎。请检查文件格式是否正确。")
                     logger.error(f"All Excel engines failed for {file_name} during pre-check")
                     continue
                     
                 if excel_data:
                     non_empty_sheets = [(name, df) for name, df in excel_data.items() if not df.empty]
                     for sheet_name, df_sheet in non_empty_sheets:
                         # 直接使用sheet名称生成表名
                         cleaned_sheet_name = ''.join(filter(str.isalnum, str(sheet_name))).lower()
                         original_table_name = cleaned_sheet_name if cleaned_sheet_name else f"sheet_{len(processed_tables) + len(files_pending_confirmation) + 1}"
                         
                         sanitized_name = ''.join(filter(str.isalnum, original_table_name)).lower()
                         if sanitized_name and check_table_exists(conn, sanitized_name):
                             files_pending_confirmation.append({'file': uploaded_file, 'type': 'excel_sheet', 'original_name': original_table_name, 'sheet_name': sheet_name, 'df': df_sheet})
                         else:
                             # 表不存在，直接处理该sheet
                             if insert_dataframe_to_db(st, df_sheet, sanitized_name, conn, if_exists='replace'):
                                  st.success(f"EXCEL表 '{sheet_name}' 已成功创建表 '{sanitized_name}'。")  # 移除了文件名显示
                                  processed_tables.append(sanitized_name)
                             else:
                                  st.error(f"创建表 '{sanitized_name}' 从工作表 '{sheet_name}' 失败。")

        elif file_type.startswith('image/') or file_type == 'application/pdf':
            # OCR 文件
            original_table_name = os.path.splitext(file_name)[0]
            sanitized_name = ''.join(filter(str.isalnum, original_table_name)).lower()
            if sanitized_name and check_table_exists(conn, sanitized_name):
                 files_pending_confirmation.append({'file': uploaded_file, 'type': 'ocr', 'original_name': original_table_name})
            else:
                 # 表不存在，直接处理
                 result = process_ocr(st, uploaded_file, conn, vl_client, vl_model_name)
                 if result: processed_tables.append(result)
        else:
            st.warning(f"跳过不支持的文件类型: {file_name}")

    # 第二遍：处理需要用户确认的文件
    if files_pending_confirmation:
        st.divider()
        st.subheader("以下文件对应的数据库表已存在，请确认操作：")
        
        all_confirmed = True # 跟踪是否所有待确认项都已处理
        results_from_confirmation = []

        for item in files_pending_confirmation:
            uploaded_file = item['file']
            original_name = item['original_name']
            file_type = item['type']
            
            # 调用重构后的 _handle_table_existence
            proceed, final_table_name, if_exists_strategy = _handle_table_existence(st, conn, original_name)

            if if_exists_strategy == 'pending':
                all_confirmed = False # 只要有一个待定，就不是全部确认
                # 不需要立即处理，等待下一次streamlit运行循环
            elif proceed:
                # 用户已确认，执行操作
                if file_type == 'csv':
                    # 需要重新读取文件内容，使用增强的CSV解析逻辑
                    try:
                        uploaded_file.seek(0) # 重置文件指针
                        raw_data = uploaded_file.read()
                        result = chardet.detect(raw_data)
                        detected_encoding = result['encoding']
                        
                        # 使用相同的多种编码尝试机制
                        encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'latin1', detected_encoding]
                        encodings_to_try = [enc for enc in encodings_to_try if enc is not None]
                        
                        df = None
                        successful_encoding = None
                        
                        for encoding in encodings_to_try:
                            try:
                                uploaded_file.seek(0)
                                df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding, escapechar='\\')
                                
                                if all(isinstance(col, int) for col in df.columns) or len(df) == 0:
                                    uploaded_file.seek(0)
                                    df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding, escapechar='\\', header=None)
                                    df.columns = [f'col_{i}' for i in range(len(df.columns))]
                                
                                if not df.empty and len(df.columns) > 0:
                                    successful_encoding = encoding
                                    break
                                    
                            except UnicodeDecodeError:
                                continue
                            except Exception:
                                continue
                        
                        if df is None:
                            st.error(f"无法重新读取确认后的CSV文件 '{uploaded_file.name}'")
                            logger.error(f"Failed to re-read confirmed CSV {uploaded_file.name}")
                            continue
                        
                        if insert_dataframe_to_db(st, df, final_table_name, conn, if_exists=if_exists_strategy):
                            st.success(f"CSV 文件 '{uploaded_file.name}' 已成功操作表 '{final_table_name}' (策略: {if_exists_strategy})。")
                            results_from_confirmation.append(final_table_name)
                        else:
                            st.error(f"操作 CSV 文件 '{uploaded_file.name}' 到表 '{final_table_name}' 失败。")
                    except Exception as e:
                        st.error(f"处理确认后的 CSV 文件 '{uploaded_file.name}' 时出错: {e}")
                        logger.error(f"Error processing confirmed CSV {uploaded_file.name}: {e}", exc_info=True)
                
                elif file_type == 'excel_sheet':
                    df_sheet = item['df']
                    sheet_name = item['sheet_name']
                    base_file_name = os.path.splitext(uploaded_file.name)[0]
                    if insert_dataframe_to_db(st, df_sheet, final_table_name, conn, if_exists=if_exists_strategy):
                        st.success(f"EXCEL表 '{base_file_name}'-'{sheet_name}' 已成功操作表 '{final_table_name}' (策略: {if_exists_strategy})。")
                        results_from_confirmation.append(final_table_name)
                    else:
                        st.error(f"操作工作表 '{sheet_name}' 到表 '{final_table_name}' 失败。")

                elif file_type == 'ocr':
                     # 对于OCR，需要重新执行OCR流程，因为结果未存储
                     result = process_ocr(st, uploaded_file, conn, vl_client, vl_model_name, force_process=True, target_table_name=final_table_name, ocr_if_exists=if_exists_strategy)
                     if result: results_from_confirmation.append(result)
            
            elif if_exists_strategy == 'skip':
                 # 用户确认跳过
                 st.info(f"已跳过文件 '{uploaded_file.name}' (类型: {file_type}) 的数据库操作。")
                 # 标记为已处理（跳过也是一种处理）
            
            # 如果是 'fail' 或其他未处理情况，也算处理完成（失败处理）

            # 添加分隔线，使每个文件的确认块更清晰
            st.divider()
        
        processed_tables.extend(results_from_confirmation)

        # if not all_confirmed:
        #     st.info("部分文件需要您确认操作后才能继续处理。")

    st.divider()
    if processed_tables:
        st.success(f"文件处理完成。成功操作的表: {', '.join(processed_tables)}")
    else:
        st.info("所有文件处理完毕，没有新的数据库表被操作或创建（可能被跳过或处理失败）。")

# --- 辅助函数 --- 
def _handle_table_existence(st, conn, original_table_name):
    """检查表是否存在，如果存在则向用户显示选项并返回处理方式。

    Args:
        st: Streamlit object.
        conn: Database connection.
        original_table_name (str): 用户提供的原始表名（未清理）。

    Returns:
        tuple: (proceed: bool, final_table_name: str, if_exists_strategy: str)
               proceed: 是否继续数据库操作。
               final_table_name: 最终使用的表名（可能被用户修改）。
               if_exists_strategy: 用户选择的处理策略 ('replace', 'append', 'fail', 'rename', 'skip', 'pending')。
               如果用户选择跳过或取消，返回 (False, final_table_name, 'skip')
               如果等待用户输入或确认，返回 (False, final_table_name, 'pending')
    """
    # 清理原始表名以进行检查和默认使用
    sanitized_base_name = ''.join(filter(str.isalnum, original_table_name)).lower()
    if not sanitized_base_name:
        st.error(f"无法从 '{original_table_name}' 生成有效的默认表名。请在下方手动指定。")
        sanitized_base_name = f"table_{int(time.time())}" # 提供一个备用基础

    # 使用 session state 来存储用户选择和确认状态，避免控件状态重置问题
    # 为每个表生成唯一的 session key
    session_key_base = f"handle_table_{original_table_name.replace('.', '_').replace(' ', '_')}" # 确保key的唯一性
    action_key = f"{session_key_base}_action"
    rename_key = f"{session_key_base}_rename_input"
    confirm_key = f"{session_key_base}_confirm_button"
    confirmed_key = f"{session_key_base}_confirmed" # 新增：跟踪确认状态的key

    # 初始化 session state (如果不存在)
    if action_key not in st.session_state:
        st.session_state[action_key] = '替换现有表' # 默认选项
    if rename_key not in st.session_state:
        st.session_state[rename_key] = f"{sanitized_base_name}_new"
    if confirmed_key not in st.session_state:
        st.session_state[confirmed_key] = False # 初始化确认状态为 False

    table_exists = check_table_exists(conn, sanitized_base_name)

    if table_exists is None: # Error during check
        st.error(f"检查表 '{sanitized_base_name}' 是否存在时出错。")
        return False, sanitized_base_name, 'fail' # Fail safe

    if not table_exists:
        # 表不存在，直接进行创建（使用replace策略）
        logger.info(f"Table '{sanitized_base_name}' does not exist. Proceeding with creation.")
        return True, sanitized_base_name, 'replace'

    # --- 表存在，检查是否已确认 --- 
    if st.session_state[confirmed_key]:
        final_name = st.session_state.get(f"{session_key_base}_final_name", sanitized_base_name)
        final_strategy = st.session_state.get(f"{session_key_base}_final_strategy", 'skip') # 默认为 skip，如果没存
        final_proceed = st.session_state.get(f"{session_key_base}_final_proceed", False)
        # logger.debug(f"Returning stored state for confirmed table '{sanitized_base_name}': {final_proceed}, {final_name}, {final_strategy}")
        return final_proceed, final_name, final_strategy

    # --- 表存在且未确认，显示用户选项 --- 
    st.warning(f"数据库中已存在名为 '{sanitized_base_name}' 的表 (来自文件 '{original_table_name}')。请选择操作：")

    # 使用列布局优化显示
    col1, col2 = st.columns([3, 1]) # 调整比例给单选按钮更多空间

    with col1:
        action = st.radio(
            "选择操作：",
            ('替换现有表', '追加到现有表', '重命名新表', '跳过此文件'),
            key=action_key, # 使用 session state key
            horizontal=True,
            label_visibility="collapsed"
        )

        # 如果选择重命名，显示输入框
        rename_input_visible = (action == '重命名新表')
        if rename_input_visible:
            new_table_name_input = st.text_input(
                "输入新表名：",
                value=st.session_state[rename_key],
                key=rename_key # 使用 session state key
            )
        else:
            # 隐藏时仍然需要一个占位符或逻辑处理，确保key存在
            new_table_name_input = st.session_state[rename_key] # 保留状态但不显示

    with col2:
        # 确认按钮，每个表实例一个独立的按钮
        confirm_pressed = st.button("确认操作", key=confirm_key)

    # --- 处理确认按钮点击 --- 
    if confirm_pressed:
        logger.info(f"Confirmation received for table '{sanitized_base_name}'. Action: {action}")
        final_table_name = sanitized_base_name
        if_exists_strategy = 'fail' # Default to fail/skip if logic below doesn't set
        proceed = False

        if action == '替换现有表':
            if_exists_strategy = 'replace'
            proceed = True
        elif action == '追加到现有表':
            if_exists_strategy = 'append'
            proceed = True
        elif action == '重命名新表':
            # 清理并验证新表名
            proposed_name = ''.join(filter(str.isalnum, new_table_name_input)).lower()
            if not proposed_name:
                st.error("新表名无效，不能为空或只包含特殊字符。请重新输入并确认。")
                return False, sanitized_base_name, 'pending' # 特殊状态表示等待用户修正
            elif proposed_name == sanitized_base_name:
                st.error(f"新表名 '{proposed_name}' 与现有表名相同。请选择其他操作或输入不同的新表名。")
                return False, sanitized_base_name, 'pending'
            else:
                # 检查重命名的目标表是否也存在
                rename_target_exists = check_table_exists(conn, proposed_name)
                if rename_target_exists is None:
                    st.error(f"检查目标新表名 '{proposed_name}' 是否存在时出错。")
                    return False, sanitized_base_name, 'fail'
                elif rename_target_exists:
                    st.error(f"目标新表名 '{proposed_name}' 也已存在。请选择不同的名称或操作。")
                    return False, sanitized_base_name, 'pending'
                else:
                    final_table_name = proposed_name
                    if_exists_strategy = 'replace' # 对新命名的表总是创建/替换
                    proceed = True
        elif action == '跳过此文件':
            if_exists_strategy = 'skip'
            proceed = False
            st.info(f"已选择跳过文件 '{original_table_name}' 的数据库操作。")
        
        # 只有在确认后且操作不是 pending 时，才标记为已确认并存储状态
        if if_exists_strategy != 'pending':
            st.session_state[confirmed_key] = True
            # 存储最终决定，以便下次调用时直接返回
            st.session_state[f"{session_key_base}_final_proceed"] = proceed
            st.session_state[f"{session_key_base}_final_name"] = final_table_name
            st.session_state[f"{session_key_base}_final_strategy"] = if_exists_strategy
            logger.info(f"State confirmed and stored for '{sanitized_base_name}': proceed={proceed}, name={final_table_name}, strategy={if_exists_strategy}")

        logger.info(f"_handle_table_existence returning: proceed={proceed}, name={final_table_name}, strategy={if_exists_strategy}")
        return proceed, final_table_name, if_exists_strategy

    else:
        # 如果按钮未被按下，表示用户尚未确认，不进行任何数据库操作
        # 返回一个表示“待定”的状态
        # logger.debug(f"No confirmation yet for table '{sanitized_base_name}'. Returning 'pending'.")
        return False, sanitized_base_name, 'pending'

# --- 处理表格--- 
def process_tabular_file(st, uploaded_file, conn):
    """处理表格文件(CSV, XLS, XLSX)，支持Excel多工作表，并在表存在时询问用户操作。"""
    created_tables = []
    try:
        base_file_name = os.path.splitext(uploaded_file.name)[0]
        # 使用原始文件名生成基础表名，稍后清理
        original_base_table_name = base_file_name 

        if uploaded_file.name.endswith('.csv'):
            # 增强的CSV解析：多种编码尝试和改进错误处理
            raw_data = uploaded_file.read()
            result = chardet.detect(raw_data)
            detected_encoding = result['encoding']
            logger.info(f"Detected encoding for {uploaded_file.name}: {detected_encoding}")

            # 多种编码尝试列表
            encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'latin1', detected_encoding]
            encodings_to_try = [enc for enc in encodings_to_try if enc is not None]
            
            df = None
            successful_encoding = None
            
            for encoding in encodings_to_try:
                try:
                    # 重置文件指针位置
                    uploaded_file.seek(0)
                    df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding, escapechar='\\')
                    
                    # 检查列名是否为整数类型，如果是则重新读取为无标题行
                    if all(isinstance(col, int) for col in df.columns) or len(df) == 0:
                        uploaded_file.seek(0)
                        df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding, escapechar='\\', header=None)
                        df.columns = [f'col_{i}' for i in range(len(df.columns))]
                    
                    # 验证数据有效性
                    if not df.empty and len(df.columns) > 0:
                        successful_encoding = encoding
                        logger.info(f"Successfully read CSV {uploaded_file.name} with encoding: {encoding}")
                        break
                        
                except UnicodeDecodeError:
                    logger.warning(f"Encoding {encoding} failed for {uploaded_file.name}, trying next...")
                    continue
                except Exception as e:
                    logger.warning(f"Error reading CSV {uploaded_file.name} with encoding {encoding}: {e}")
                    continue

            if df is None:
                handle_error(
                    st, 
                    f"无法解码CSV文件 '{uploaded_file.name}'，已尝试多种编码格式。",
                    error_code="CSV_DECODE_ERROR",
                    user_suggestion="请检查文件内容是否为有效的CSV格式，或尝试将文件保存为UTF-8编码后重新上传。"
                )
                return None
            
            if df.empty or len(df.columns) == 0:
                handle_error(
                    st,
                    f"CSV文件 '{uploaded_file.name}' 为空或没有有效数据列",
                    error_code="CSV_EMPTY_ERROR",
                    user_suggestion="请检查文件内容，确保包含有效的数据。"
                )
                return None

            # 记录成功使用的编码
            logger.info(f"CSV {uploaded_file.name} successfully parsed using encoding: {successful_encoding}")
            
            # 在插入前检查表是否存在并获取用户选择
            proceed, final_table_name, if_exists_strategy = _handle_table_existence(st, conn, original_base_table_name)

            if proceed:
                if insert_dataframe_to_db(st, df, final_table_name, conn, if_exists=if_exists_strategy):
                    st.success(f"CSV 文件 '{uploaded_file.name}' 已成功操作表 '{final_table_name}' (策略: {if_exists_strategy})。")
                    created_tables.append(final_table_name)
                else:
                    st.error(f"操作 CSV 文件 '{uploaded_file.name}' 到表 '{final_table_name}' 失败。")
            else:
                # st.info(f"跳过文件 '{uploaded_file.name}' 的数据库操作。")
                pass

        elif uploaded_file.name.endswith(('.xls', '.xlsx')):
            # 增强的Excel解析：多引擎支持
            engines = ['calamine', 'openpyxl', 'xlrd']
            excel_data = None
            successful_engine = None
            
            for engine in engines:
                try:
                    uploaded_file.seek(0)  # 重置文件指针
                    excel_data = pd.read_excel(uploaded_file, sheet_name=None, engine=engine)
                    if excel_data:
                        successful_engine = engine
                        logger.info(f"Successfully read Excel {uploaded_file.name} using engine: {engine}")
                        break
                except ImportError:
                    logger.warning(f"Engine {engine} not available for {uploaded_file.name}")
                    continue
                except Exception as e:
                    logger.warning(f"Engine {engine} failed for {uploaded_file.name}: {e}")
                    continue
            
            if excel_data is None:
                handle_error(
                    st,
                    f"无法读取Excel文件 '{uploaded_file.name}'，已尝试所有可用的解析引擎。",
                    error_code="EXCEL_READ_ERROR",
                    user_suggestion="请检查文件格式是否正确，或尝试将文件保存为较新的Excel格式后重新上传。"
                )
                return None

            if not excel_data:
                st.warning(f"Excel 文件 '{uploaded_file.name}' 为空或无法读取。")
                return None

            sheet_items = list(excel_data.items())

            # Filter out empty sheets
            non_empty_sheets = [(name, df) for name, df in sheet_items if not df.empty]

            if not non_empty_sheets:
                st.warning(f"Excel 文件 '{uploaded_file.name}' 所有工作表均为空。")
                return None

            for sheet_name, df in non_empty_sheets:
                # Determine original table name based on sheet
                if len(non_empty_sheets) == 1:
                    original_table_name = original_base_table_name
                else:
                    # Sanitize sheet name for table name part
                    cleaned_sheet_name = ''.join(filter(str.isalnum, str(sheet_name))).lower()
                    # 如果有多个非空sheet，直接使用清理后的sheet名，如果清理后为空，则使用通用名称
                    original_table_name = cleaned_sheet_name if cleaned_sheet_name else f"sheet_{len(created_tables) + 1}"

                # 在插入前检查表是否存在并获取用户选择
                proceed, final_table_name, if_exists_strategy = _handle_table_existence(st, conn, original_table_name)

                if proceed:
                    if insert_dataframe_to_db(st, df, final_table_name, conn, if_exists=if_exists_strategy):
                        st.success(f"EXCEL表 '{base_file_name}'-'{sheet_name}' 已成功操作表 '{final_table_name}' (策略: {if_exists_strategy})。")
                        created_tables.append(final_table_name)
                    else:
                        st.error(f"操作工作表 '{sheet_name}' 到表 '{final_table_name}' 失败。")
                else:
                     # st.info(f"跳过工作表 '{sheet_name}' 的数据库操作。")
                     pass
        else:
            st.warning(f"不支持的文件类型: {uploaded_file.name}")
            return None

        return created_tables if created_tables else None

    except Exception as e:
        st.error(f"处理表格文件 '{uploaded_file.name}' 时出错: {e}")
        logger.error(f"Error processing tabular file {uploaded_file.name}: {e}", exc_info=True)
        return None

# --- 处理OCR--- 
def process_ocr(st, uploaded_file, conn, vl_client, vl_model_name, force_process=False, target_table_name=None, ocr_if_exists='replace'):
    """处理图片或PDF文件进行OCR，并在表存在时询问用户操作（除非强制执行），然后存入数据库。
    
    Args:
        force_process (bool): 如果为True，则跳过存在性检查和用户交互，直接使用提供的策略。
        target_table_name (str): 当 force_process 为 True 时，指定要操作的目标表名。
        ocr_if_exists (str): 当 force_process 为 True 时，指定表存在时的操作策略。
    """
    try:
        file_name_base = os.path.splitext(uploaded_file.name)[0]
        original_table_name = file_name_base # 使用原始文件名作为基础

        proceed = False
        final_table_name = None
        if_exists_strategy = 'fail'

        if force_process:
            # 强制处理，使用传入的参数
            proceed = True
            final_table_name = target_table_name
            if_exists_strategy = ocr_if_exists
            logger.info(f"Force processing OCR for {uploaded_file.name}. Target: {final_table_name}, Strategy: {if_exists_strategy}")
        else:
            # 正常流程，检查表是否存在并获取用户确认
            proceed, final_table_name, if_exists_strategy = _handle_table_existence(st, conn, original_table_name)
            if if_exists_strategy == 'pending':
                return None # 等待用户确认

        if not proceed:
            if if_exists_strategy != 'skip': # 避免重复显示跳过信息
                 # st.info(f"跳过文件 '{uploaded_file.name}' (OCR) 的数据库操作。")
                 pass
            return None

        # --- 执行 OCR 和数据库操作 --- 
        file_bytes = uploaded_file.getvalue()
        df = None
        image_base64_list = []

        if uploaded_file.type.startswith('image/'):
            logger.info(f"Processing image file {uploaded_file.name} for OCR.")
            # 增强的图片格式支持：统一转换为RGB格式
            try:
                from PIL import Image
                img = Image.open(io.BytesIO(file_bytes))
                img = img.convert('RGB')  # 统一转换为RGB格式
                buffer = io.BytesIO()
                img.save(buffer, format='JPEG')
                img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                image_base64_list.append(img_base64)
                img.close()  # 释放内存
                buffer.close()
            except ImportError:
                logger.warning("PIL not available, using raw image data")
                img_base64 = base64.b64encode(file_bytes).decode('utf-8')
                image_base64_list.append(img_base64)
            except Exception as e:
                logger.error(f"Image conversion failed for {uploaded_file.name}: {e}")
                img_base64 = base64.b64encode(file_bytes).decode('utf-8')
                image_base64_list.append(img_base64)
                
        elif uploaded_file.type == 'application/pdf':
            logger.info(f"Processing PDF file {uploaded_file.name} for OCR.")
            try:
                doc = fitz.Document(stream=file_bytes, filetype="pdf")
                num_pages_to_process = min(3, len(doc))  # 限制处理页数以节省内存
                
                for page_num in range(num_pages_to_process):
                    try:
                        page = doc.load_page(page_num)
                        # 优化DPI设置：平衡质量和内存使用
                        pix = page.get_pixmap(dpi=200)  # 降低DPI从300到200
                        img_bytes_page = pix.tobytes("jpeg")
                        img_base64 = base64.b64encode(img_bytes_page).decode('utf-8')
                        image_base64_list.append(img_base64)
                        # 显式释放内存
                        del pix
                    except Exception as e:
                        logger.error(f"Error processing page {page_num} in {uploaded_file.name}: {e}")
                        continue
                
                doc.close()
                logger.info(f"Processed {num_pages_to_process} pages from {uploaded_file.name}")
            except Exception as e:
                logger.error(f"Error opening PDF {uploaded_file.name}: {e}")
                st.error(f"无法打开PDF文件 '{uploaded_file.name}'，文件可能已损坏。")
                return None
        else:
            st.warning(f"不支持的OCR文件类型: {uploaded_file.name} ({uploaded_file.type})")
            return None
            
        df_str = call_vl_api(st, vl_client, vl_model_name, image_base64_list=image_base64_list)

        if df_str is not None and isinstance(df_str, str):
            try:
                # 将CSV字符串转换为DataFrame
                df = pd.read_csv(io.StringIO(df_str))
                if not df.empty:
                    logger.info(f"OCR successful for {uploaded_file.name}. Extracted DataFrame shape: {df.shape}")
                    
                    # 使用确认后的表名和策略进行数据库操作
                    if insert_dataframe_to_db(st, df, final_table_name, conn, if_exists=if_exists_strategy):
                        st.success(f"文件 '{uploaded_file.name}' 通过OCR处理后成功操作表 '{final_table_name}' (策略: {if_exists_strategy})。")
                        return final_table_name
                    else:
                        st.error(f"OCR处理后，操作数据到表 '{final_table_name}' 失败。")
                        return None
                else:
                    st.warning(f"未能从文件 '{uploaded_file.name}' 中提取到表格数据 (OCR结果为空)。")
                    logger.warning(f"OCR for {uploaded_file.name} resulted in an empty DataFrame.")
                    return None
            except Exception as e:
                st.error(f"处理OCR结果时出错: {e}")
                logger.error(f"Error processing OCR result for {uploaded_file.name}: {e}", exc_info=True)
                return None
        elif df_str is None:
             # Error/warning already shown by call_vl_api
             logger.warning(f"OCR call for {uploaded_file.name} returned None.")
             return None
        else: # 如果 call_vl_api 返回了非字符串（例如 DataFrame），这不符合预期
            st.error(f"OCR API 返回了意外的类型: {type(df_str)}。期望是 CSV 字符串。")
            logger.error(f"Unexpected return type from call_vl_api for {uploaded_file.name}: {type(df_str)}")
            return None

    except Exception as e:
        st.error(f"处理OCR文件 '{uploaded_file.name}' 时出错: {e}")
        logger.error(f"Error processing OCR file {uploaded_file.name}: {e}", exc_info=True)