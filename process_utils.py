import os
import io
import base64
import logging
import pandas as pd
import numpy as np
import chardet
import fitz  # PyMuPDF
from llm_utils import call_vl_api
from db_utils import insert_dataframe_to_db

logger = logging.getLogger(__name__)

def process_tabular_file(st, uploaded_file, conn):
    """处理表格文件(CSV, XLS, XLSX)，支持Excel多工作表"""
    created_tables = []
    try:
        base_file_name = os.path.splitext(uploaded_file.name)[0]
        base_table_name = ''.join(filter(str.isalnum, base_file_name)).lower()
        if not base_table_name:
            base_table_name = f"file_{abs(hash(uploaded_file.name))}" 

        if uploaded_file.name.endswith('.csv'):
            # 使用 chardet 自动检测编码
            raw_data = uploaded_file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            logger.info(f"Detected encoding for {uploaded_file.name}: {encoding}")

            try:
                df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding, escapechar='\\')
                if all(isinstance(col, int) for col in df.columns) or len(df) == 0:
                    # Reread without header
                    df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding, escapechar='\\', header=None)
                    df.columns = [f'col_{i}' for i in range(len(df.columns))]

                if df.empty or len(df.columns) == 0:
                    st.error(f"CSV文件 '{uploaded_file.name}' 为空或没有有效数据列")
                    return None

            except Exception as e:
                st.error(f"无法解码或读取CSV文件 '{uploaded_file.name}'，请检查文件编码格式和内容。错误: {e}")
                logger.error(f"Error reading CSV {uploaded_file.name}: {e}", exc_info=True)
                return None

            if insert_dataframe_to_db(st, df, base_table_name, conn):
                st.success(f"CSV 文件 '{uploaded_file.name}' 已成功导入到表 '{base_table_name}'")
                created_tables.append(base_table_name)
            else:
                 st.error(f"导入 CSV 文件 '{uploaded_file.name}' 到表 '{base_table_name}' 失败。")

        elif uploaded_file.name.endswith(('.xls', '.xlsx')):
            try:
                # 使用 calamine 引擎读取Excel文件
                excel_data = pd.read_excel(uploaded_file, sheet_name=None, engine='calamine')
            except Exception as e:
                st.error(f"读取 Excel 文件 '{uploaded_file.name}' 时出错: {e}")
                logger.error(f"Error reading Excel {uploaded_file.name}: {e}", exc_info=True)
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
                # Sanitize sheet name for table name
                cleaned_sheet_name = ''.join(filter(str.isalnum, str(sheet_name))).lower()

                # Determine table name
                if len(non_empty_sheets) == 1:
                    table_name = base_table_name
                else:
                    table_name = f"{base_table_name}_{cleaned_sheet_name}" if cleaned_sheet_name else f"{base_table_name}_sheet_{len(created_tables) + 1}"

                if insert_dataframe_to_db(st, df, table_name, conn):
                    st.success(f"EXCEL表 '{base_file_name}'-'{sheet_name}' 已成功导入到表 '{table_name}'")
                    created_tables.append(table_name)
                else:
                    st.error(f"导入工作表 '{sheet_name}' 到表 '{table_name}' 失败。")
        else:
            st.warning(f"不支持的文件类型: {uploaded_file.name}")
            return None

        return created_tables if created_tables else None

    except Exception as e:
        st.error(f"处理表格文件 '{uploaded_file.name}' 时出错: {e}")
        logger.error(f"Error processing tabular file {uploaded_file.name}: {e}", exc_info=True)
        return None


def process_ocr(st, uploaded_file, conn, vl_client, vl_model_name):
    """处理图片或PDF文件进行OCR并存入数据库"""
    try:
        file_name_base = os.path.splitext(uploaded_file.name)[0]
        table_name = ''.join(filter(str.isalnum, file_name_base)).lower()
        if not table_name:
             table_name = f"ocr_{abs(hash(uploaded_file.name))}"

        file_bytes = uploaded_file.getvalue()
        df = None

        if uploaded_file.type.startswith('image/'):
            logger.info(f"Processing image file {uploaded_file.name} for OCR.")
            df = call_vl_api(st, vl_client, vl_model_name, image_bytes=file_bytes)
        elif uploaded_file.type == 'application/pdf':
            logger.info(f"Processing PDF file {uploaded_file.name} for OCR.")
            df = call_vl_api(st, vl_client, vl_model_name, pdf_bytes=file_bytes)
        else:
            st.warning(f"不支持的OCR文件类型: {uploaded_file.name} ({uploaded_file.type})")
            return None

        if df is not None and not df.empty:
            logger.info(f"OCR successful for {uploaded_file.name}. Extracted DataFrame shape: {df.shape}")
            # 使用辅助函数将DataFrame导入数据库
            if insert_dataframe_to_db(st, df, table_name, conn):
                st.success(f"文件 '{uploaded_file.name}' 通过OCR处理后成功导入到表 '{table_name}'")
                return table_name
            else:
                st.error(f"OCR处理后，导入数据到表 '{table_name}' 失败。")
                return None
        elif df is None:
             # Error/warning already shown by call_vl_api
             logger.warning(f"OCR call for {uploaded_file.name} returned None.")
             return None
        else: # df is empty
            st.warning(f"未能从文件 '{uploaded_file.name}' 中提取到表格数据 (OCR结果为空)。")
            logger.warning(f"OCR for {uploaded_file.name} resulted in an empty DataFrame.")
            return None

    except Exception as e:
        st.error(f"处理OCR文件 '{uploaded_file.name}' 时出错: {e}")
        logger.error(f"Error processing OCR file {uploaded_file.name}: {e}", exc_info=True)
        return None