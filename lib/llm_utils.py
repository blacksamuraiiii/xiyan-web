import os
import logging
from openai import OpenAI
import streamlit as st 

# log文件配置
logger = logging.getLogger(__name__)

# LLM 初始化
# 通用客户端初始化函数
def cached_get_client(st, base_url, api_key, client_name):
    """获取并缓存OpenAI客户端实例"""
    logger.info(f"Attempting to initialize {client_name} client...")
    
    if not base_url or not api_key:
        st.error(f"缺少 {client_name} 模型的API配置信息 (Base URL 或 API Key)")
        logger.error(f"Missing API config for {client_name}: Base URL or API Key.")
        return None
        
    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        logger.info(f"{client_name} client initialized successfully for base URL: {base_url}")
        logger.info(f"{client_name} client initialized and cached.")
        return client
    except Exception as e:
        st.error(f"初始化 {client_name} 客户端时出错: {e}")
        logger.error(f"Error initializing {client_name} client: {e}", exc_info=True)
        logger.error(f"Failed to initialize {client_name} client.")
        return None

# 调用XiYan SQL API
def call_xiyan_sql_api(st, sql_client: OpenAI, sql_model_name: str, user_query: str, db_schema: dict):
    """调用XiYanSQL API将自然语言转换为SQL，仅返回SQL字符串"""
    if not sql_client:
        st.error("SQL 模型客户端未初始化，无法调用API。")
        logger.error("call_xiyan_sql_api called without an initialized SQL client.")
        return None
    if not user_query:
        st.warning("请输入您的问题。")
        logger.warning("call_xiyan_sql_api called with empty user query.")
        return None
    if not db_schema:
        st.warning("数据库结构信息不可用，无法生成SQL。")
        logger.warning("call_xiyan_sql_api called without DB schema.")
        return None

    try:
        # 格式化数据库 Schema 信息
        schema_string = ""
        for table, columns in db_schema.items():
            schema_string += f"表 '{table}':\n"
            for col_name, col_type in columns.items():
                schema_string += f"  - {col_name} ({col_type})\n"
            schema_string += "\n"

        # 按照官方格式构建系统提示词
        system_prompt = f"""你是一名PostgreSQL专家，现在需要阅读并理解下面的【数据库schema】描述，运用PostgreSQL知识生成sql语句回答【用户问题】。
        【用户问题】
        {user_query}

        【数据库schema】
        {schema_string}

        重要提示:
        1. 在对列进行聚合（如 SUM, AVG）之前，如果需要将文本类型（TEXT, VARCHAR）转换为数值类型（INTEGER, NUMERIC, FLOAT），请务必先过滤掉无法成功转换的值，以避免 'invalid input syntax' 错误。例如，可以使用 `WHERE column ~ '^[0-9]+(\\.[0-9]+)?$'` 来筛选纯数字字符串，或者使用 `CASE` 语句或 `NULLIF` 结合 `CAST` 进行安全转换。
        2. 优先使用 `WHERE` 子句过滤掉非数值数据，而不是在 `SUM` 或 `AVG` 内部尝试转换。
        3. 生成的SQL语句必须以分号结尾。
        4. 只返回SQL语句，不要包含任何解释性文字或markdown标记。"""

        logger.info(f"Calling SQL API ({sql_model_name}) for query: '{user_query}'")
        response = sql_client.chat.completions.create(
            model=sql_model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            temperature=0.1,
            max_tokens=2048 # Reduced max_tokens slightly
        )
        logger.info("SQL API response received.")

        # 解析API返回结果
        if response.choices and response.choices[0].message.content:
            generated_text = response.choices[0].message.content.strip()
            logger.debug(f"SQL API raw response: {generated_text}")

            # 提取SQL语句 (more robust extraction)
            sql_query = None
            if '```sql' in generated_text:
                sql_query = generated_text.split('```sql')[1].split('```')[0].strip()
                logger.info("Extracted SQL from ```sql block.")
            elif any(keyword in generated_text.upper().split() for keyword in ['SELECT', 'WITH']): 
                 lines = generated_text.split('\n')
                 sql_lines = []
                 found_sql = False
                 for line in lines:
                     if any(line.strip().upper().startswith(kw) for kw in ['SELECT', 'WITH']):
                         found_sql = True
                     if found_sql:
                         sql_lines.append(line)
                 if sql_lines:
                     sql_query = "\n".join(sql_lines).strip()
                     logger.info("Extracted SQL based on starting keywords.")
                 else:
                     sql_query = generated_text
                     logger.warning("SQL keyword detected, but couldn't isolate query cleanly. Using full response.")
            else:
                 if ';' in generated_text or 'FROM' in generated_text.upper():
                      sql_query = generated_text
                      logger.warning("No clear SQL block/keyword, assuming response is SQL based on ';' or 'FROM'.")

            if sql_query:
                sql_query = sql_query.rstrip(';').strip() + ';'
                logger.info(f"Successfully extracted SQL query: {sql_query}")
                return sql_query
            else:
                st.warning(f"未能从API返回结果中提取有效的SQL语句。请检查模型输出或调整提示。")
                logger.warning(f"Could not extract SQL from API response: {generated_text}")
                # st.info("提示：请明确指定要删除的表名，例如'删除测试表'") # This hint seems out of place here
                return None
        else:
            st.error(f"SQL API 调用成功，但返回结果为空或格式不符合预期: {response}")
            logger.error(f"SQL API call successful but response format unexpected: {response}")
            return None

    except Exception as e:
        st.error(f"调用 XiYan SQL API 时出错: {e}")
        logger.error(f"Error calling SQL API: {e}", exc_info=True)
        return None

# 调用Qwen-VL API
def call_vl_api(st, vl_client: OpenAI, vl_model_name: str, image_base64_list=None):
    """调用Qwen-VL API进行OCR识别"""
    if not vl_client:
        st.error("VL 模型客户端未初始化，无法调用API。")
        return None

    messages = [
        {
            "role": "system",
            "content": [
                {"type": "text", "text": "你是一个OCR表格识别助手,请从图片或PDF页面中提取表格数据并以CSV格式返回。如果内容不是表格，请说明。"}
            ]
        }
    ]

    if not image_base64_list:
        st.error("没有提供图片或有效的PDF内容进行OCR处理。")
        return None

    user_content = []
    for img_base64 in image_base64_list:
        user_content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{img_base64}"}
        })

    user_content.append({
        "type": "text",
        "text": "请将上述内容中的表格数据提取出来，并以CSV格式返回。确保CSV格式正确，包含表头。"
    })

    messages.append({
        "role": "user",
        "content": user_content
    })

    try:
        logger.info(f"Calling VL API ({vl_model_name}) for OCR...")
        response = vl_client.chat.completions.create(
            model=vl_model_name,
            messages=messages,
            temperature=0.1
        )
        logger.info(f"VL API response received.")

        # 解析响应
        if response.choices and response.choices[0].message.content:
            message_content = response.choices[0].message.content
            logger.debug(f"VL API raw response content: {message_content}")
            # 尝试从返回内容中找到CSV格式的数据块
            csv_text = None
            if '```csv' in message_content:
                csv_text = message_content.split('```csv')[1].split('```')[0].strip()
                logger.info("Found CSV block in VL API response.")
            elif '```' in message_content: # Handle potential ```text block
                 potential_csv = message_content.split('```')[1].split('```')[0].strip()
                 if ',' in potential_csv and '\n' in potential_csv: # Basic check for CSV structure
                      csv_text = potential_csv
                      logger.info("Found potential CSV in generic code block.")
            elif ',' in message_content and '\n' in message_content: # Try parsing directly if separators exist
                 csv_text = message_content.strip()
                 logger.info("Attempting direct parse of VL API response as CSV.")

            if csv_text:
                logger.info(f"Successfully received CSV text from VL API")
                try:
                    return csv_text
                except Exception as parse_e:
                    st.warning(f"无法解析从API返回的CSV数据: {parse_e}")
                    logger.warning(f"Failed to parse CSV from VL API response: {parse_e}. CSV text was: \n{csv_text}")
                    return None
            else:
                st.warning(f"未能从API返回结果中提取有效的CSV数据。模型可能未识别到表格或返回格式不符。")
                logger.warning(f"Could not extract CSV data from VL API response. Content: {message_content}")
                return None
        else:
            st.error(f"API调用成功，但返回结果格式不符合预期或为空: {response}")
            logger.error(f"VL API call successful but response format unexpected: {response}")
            return None

    except Exception as e:
        st.error(f"调用Qwen-VL API时出错: {e}")
        logger.error(f"Error calling VL API: {e}", exc_info=True)
        return None
