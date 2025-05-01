# agent/sql/process_sql.py
import re, json, requests
from config import *
from agent.sql.exec import exec_sql_s

def replace_date_with_day(sql):
    """
    This function replaces instances of exact date conditions in a SQL 
    statement from a format like:
        TradingDate = 'YYYY-MM-DD'
    to:
        date(TradingDate) = 'YYYY-MM-DD'
    
    Parameters:
        sql (str): The original SQL statement.
        
    Returns:
        str: The modified SQL statement, or the original if no match is found.
    """
    # Regex pattern to match patterns like: ColumnName = 'YYYY-MM-DD'
    pattern = r"([.\w]+)\s*=\s*'(\d{4}-\d{2}-\d{2})'"

    def replace_func(match):
        column_name = match.group(1)
        date_value = match.group(2)
        return f"date({column_name}) = '{date_value}'"

    new_sql = re.sub(pattern, replace_func, sql)

    # If no change was made, return the original SQL
    return new_sql if new_sql != sql else sql


def extract_sql(text):
    """
    从输入文本中提取被包裹在三重反引号（sql ... ）中的 SQL 语句
    Extracts an SQL statement from a block of text enclosed in triple backticks:
        ```sql
        SELECT ...
        ```
    
    Parameters:
        text (str): The full text containing an SQL statement.
        
    Returns:
        str: The extracted SQL statement, or a message if not found.
    """
    sql_pattern = re.compile(r'```sql(.*?)```', re.DOTALL)
    match = sql_pattern.search(text)
    if match:
        # Strip leading and trailing whitespace from the matched SQL
        return match.group(1).strip()
    else:
        # print(f"--------------------extract_sql else:{text}")
        return f"No SQL statement found :{text}."


def select_data(sql_text):
    """
    将指定的 SQL 查询 发送到某个 API 端点（通过 POST 请求），并返回该 API 的响应结果
    Sends the given SQL query to a specified endpoint and returns the JSON response.
    
    Parameters:
        sql_text (str): The SQL query to be executed.
        
    Returns:
        str: The JSON response from the API, formatted with indentation.
    """
    url = "https://comm.chatglm.cn/finglm2/api/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f'Bearer {Access_Token}'
    }
    data = {
        "sql": sql_text,  # e.g. SELECT * FROM constantdb.secumain LIMIT 10
        "limit": 50
    }
    response = requests.post(url, headers=headers, json=data)
    try:
        return json.dumps(response.json(), indent=2, ensure_ascii=False)
    except:
        return str(response.json())


def clean_sql_statement(sql):
    """
    清理SQL
    
    Parameters:
        sql (str): 输入的 SQL 语句。
    
    Returns:
        str: 清理后的 SQL 语句。
    """
    #     清理 SQL 语句中第一个 SELECT 前面的内容。
    match = re.search(r'\bSELECT\b.*', sql, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(0)
    else:
        return sql


def wrap_date_in_sql_with_conditions(sql_statement):
    # 正则匹配符合条件的时间字段
    pattern = r"(?<!\(\()\b(\w+)\b\s*=\s*'(2\d{3}-\d{2}-\d{2})'"
    # 替换，将字段用 date() 包裹
    modified_sql = re.sub(pattern, r"date(\1) = '\2'", sql_statement)
    return modified_sql


def ensure_date_in_between(sql_query):
    # 匹配 BETWEEN 左侧的字段，确保字段被 DATE() 包裹
    pattern = r"(\bAND\s+)(\w+)(\s+BETWEEN\s+)"
    replacement = r"\1date(\2)\3"
    updated_query = re.sub(pattern, replacement, sql_query)
    return updated_query


def clean_sql_query(sql):
    # 去除首尾空白字符
    cleaned = sql.strip()
    
    # 如果包含分号，去除分号后面的内容
    if ';' in cleaned:
        cleaned = cleaned.split(';')[0]
    
    # 检查并去除末尾的点
    if cleaned.endswith('.'):
        cleaned = cleaned[:-1]
    
    return cleaned
    
    
def validate_and_fix_sql_tables(sql_statement):
    """
    检查并修复 SQL 语句中的表名和库名是否匹配。
    
    参数：
        sql_statement (str): SQL 查询语句
        
    返回：
        str: 修复后的 SQL 查询语句
    """
    database_L_zh = database_L_zh_all
    if len(QUESTION_TYPE_LIST) == 0:
        database_L_zh = database_L_zh_all
    elif len(QUESTION_TYPE_LIST) >= 2:
        database_L_zh = database_L_zh_all
    elif "港股" in QUESTION_TYPE_LIST:
        database_L_zh = database_L_zh_hk
    elif "美股" in QUESTION_TYPE_LIST:
        database_L_zh = database_L_zh_us
    elif "A股" in QUESTION_TYPE_LIST:
        database_L_zh = database_L_zh_cn
    else:
        database_L_zh = database_L_zh_all

    # 构造表名到完整库表名的映射
    table_to_full_name = {item['数据表名'].split('.')[1]: item['数据表名'] for item in database_L_zh}

    # 提取 SQL 中的表名（包括库名）
    matches = re.findall(r"FROM\s+([a-zA-Z0-9_\.]+)", sql_statement, re.IGNORECASE)

    if not matches:
        if DEBUG_VER == 3:
            print("未在 SQL 语句中找到表名")
        return sql_statement

    fixed_sql = sql_statement

    for full_table_name in matches:
        if '.' in full_table_name:
            db_name, table_name = full_table_name.split('.')
        else:
            db_name = None
            table_name = full_table_name

        # 检查表名是否在 database_L_zh 中
        if table_name in table_to_full_name:
            correct_full_name = table_to_full_name[table_name]
            correct_db_name = correct_full_name.split('.')[0]

            # 如果库名不匹配或缺失，替换为正确的库表名
            if not db_name or db_name != correct_db_name:
                fixed_sql = re.sub(
                    rf"\b{re.escape(full_table_name)}\b",  # 精确匹配表名
                    correct_full_name, 
                    fixed_sql
                )
                if DEBUG_VER == 3:
                    print(f"修正库表名: {full_table_name} -> {correct_full_name}")
        else:
            if DEBUG_VER == 3:
                print(f"表名未找到: {table_name}")
            pass
    return fixed_sql
    

def to_select(text):
    """
    High-level function that:
      1. Extracts SQL from the given text.
      2. Optimizes the extracted SQL by converting date columns to 'date(...)'.
      3. Executes the optimized SQL through select_data and returns the result.
    
    Parameters:
        text (str): The input text containing an SQL statement.
        
    Returns:
        str: The JSON response from the SQL query.
    """
    global prev_tables_name_list
    global QUESTION_TYPE_LIST
    sql_statement = extract_sql(text)
    if DEBUG_VER == 3:
        print('***********Extracted SQL****************')
    sql_statement = clean_sql_statement(sql_statement)
    sql_statement = wrap_date_in_sql_with_conditions(sql_statement)
    sql_statement = validate_and_fix_sql_tables(sql_statement)
    sql_statement = ensure_date_in_between(sql_statement)
    sql_statement = clean_sql_query(sql_statement)
    if DEBUG_VER == 3:
        print(f"---------------sql_statement:{sql_statement}")
        print('***********Extracted SQL****************')
        
    optimized_sql = replace_date_with_day(sql_statement)
        
    result = select_data(optimized_sql)
    if 'count' in result:
        if '"count": 0' not in result:
            prev_tables_name_list += [i.get('数据表名') for i in table_maps if i.get('数据表名') in sql_statement]
        if ('"count": 0' in result) and ('AS' in sql_statement or 'as' in sql_statement):
            result = f"查询异常。SQL语句：{sql_statement}没有找到数据，请判断使用的字段是否正确，可尝试其它字段或库表查询，并把当次0也作为结果返回（也可能是真实结果），表的结构如下：{table_maps_LL}"
    prev_tables_name_list = list(set(prev_tables_name_list))
    if DEBUG_VER == 3:
        print(f"----------prev_tables_name_list:{prev_tables_name_list}")
    for table_name in prev_tables_name_list:
        if table_name in content_CN and table_name not in content_US and table_name not in content_HK:
            QUESTION_TYPE_LIST.append("A股")
            QUESTION_TYPE_LIST = list(set(QUESTION_TYPE_LIST))

    if "查询执行失败" in result:
        LL = [i for i in table_maps if i.get('数据表名') in sql_statement]
        result = result + f"表的结构如下：{LL}"
    if "Unknown column" in result:
        LL = [i for i in table_maps if i.get('数据表名') in sql_statement]
        result = result + f"表的结构如下：{LL}"
    if '"data": []' in result:
        LL = [i for i in table_maps if i.get('数据表名') in sql_statement]
        result = f"查询异常。SQL语句：{sql_statement}没有找到数据，结果如下：{result}。请用其它相关字段或库表查询，表的结构如下：{LL}"
    if 'No database selected' in result:
        LL = [i for i in table_maps if i.get('数据表名') in sql_statement]
        result = f"查询异常。SQL语句：{sql_statement}没有找到数据，结果如下：{result}。请用其它相关字段或库表查询，表的结构如下：{LL}"
    try:
        data_dict = json.loads(result)
        data = data_dict.get("data", [])
        if len(data) >= 50:
            result = result + " 数据库最多返回50条数据，如果需要的数据超过50条，请用多sql嵌套组合或者聚合函数count、sum、avg、max、min等来查询。如果回复的内容数量跟预期不符，思考SQL语句是否存在问题。"
        for item in data:
            if all(value is None for value in item.values()):
                result = f"查询异常。SQL语句：{sql_statement}可能没有找到数据，结果如下：{result}。请判断使用的字段是否正确，可尝试其它类似字段或库表查询（如时间字段更换成EndDate或InfoPublDate），并把当次null也作为结果返回（也可能是真实结果），表的结构如下：{table_maps_LL}"
    except Exception as e:
        pass
    # print(f"--------------sql_result:{result}")
    # print(f"--------------type sql_result:{type(result)}")
    return result

def extract_table_names(sql):
    # 正则匹配 FROM 关键字后的库表名，库表名后会有空格
    matches = re.findall(r'FROM\s+([\w\.]+)\s', sql, re.IGNORECASE)
    return matches

def all_tables_in_prompt(tables_name_list, main_sql_prompts):
    lower_prompts = main_sql_prompts.lower()
    return all(table.lower() in lower_prompts for table in tables_name_list)
