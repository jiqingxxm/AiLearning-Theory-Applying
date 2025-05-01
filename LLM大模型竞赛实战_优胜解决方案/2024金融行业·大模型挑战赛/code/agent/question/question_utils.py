# agent/question/question_utils.py
import json
from config import *
from agent.llm_client import create_chat_completion
from agent.sql.exec import exec_sql_s
from agent.utils import find_json, dict_to_sentence

def preprocessing_question(question):
    """
    SQL问题预处理
    """
    if "年末" in question:
        question = question.replace("年末","年12月31日")
    if "年底" in question:
        question = question.replace("年底","年12月31日")
    return question

def add_question_mark(text):
    # 检查文本最后是否缺少问号
    if not text.endswith('?') and not text.endswith('？'):
        text += '？'
    
    # 检查文本中右括号“）”的对应左括号“（”的处理
    if len(text) >= 2 and text[-2] == '）':
        left_bracket_index = text.rfind('（')
        if left_bracket_index != -1:
            if left_bracket_index == 0 or text[left_bracket_index - 1] not in ['?', '？']:
                text = text[:left_bracket_index] + '？' + text[left_bracket_index:]
    
    return text

def process_company_name(value):
    """
    Given a company name (or related keyword), search in three tables:
    ConstantDB.SecuMain, ConstantDB.HK_SecuMain, ConstantDB.US_SecuMain.

    Attempts to match various company-related fields (e.g., ChiName, EngName, etc.)
    and returns all matching results along with the table where they were found.

    Parameters:
        value (str): The company name or related string to match.

    Returns:
        list: A list of tuples (result, table) where result is the matched data and table is the table name.
              If no matches found, prints a message and returns an empty list.
    """
    res_lst = []
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    if len(QUESTION_TYPE_LIST) == 0:
        tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    elif len(QUESTION_TYPE_LIST) >= 2:
        tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    elif "港股" in QUESTION_TYPE_LIST:
        tables = ['ConstantDB.HK_SecuMain']
    elif "美股" in QUESTION_TYPE_LIST:
        tables = ['ConstantDB.US_SecuMain']
    elif "A股" in QUESTION_TYPE_LIST:
        tables = ['ConstantDB.SecuMain']
    else:
        tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    # print(f"--------------tables:{tables}")
    columns_to_match = ['CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                        'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']
    columns_to_select = ['InnerCode', 'CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                         'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']

    # Escape single quotes to prevent SQL injection
    value = value.replace("'", "''")

    for table in tables:
        # For the US table, remove columns that may not be available
        local_match_cols = columns_to_match.copy()
        local_select_cols = columns_to_select.copy()
        if 'US' in table:
            if 'ChiNameAbbr' in local_match_cols:
                local_match_cols.remove('ChiNameAbbr')
            if 'ChiNameAbbr' in local_select_cols:
                local_select_cols.remove('ChiNameAbbr')
            if 'EngNameAbbr' in local_match_cols:
                local_match_cols.remove('EngNameAbbr')
            if 'EngNameAbbr' in local_select_cols:
                local_select_cols.remove('EngNameAbbr')

        # Build the WHERE clause with OR conditions for each column
        match_conditions = [f"{col} = '{value}'" for col in local_match_cols]
        where_clause = ' OR '.join(match_conditions)

        sql = f"""
        SELECT {', '.join(local_select_cols)}
        FROM {table}
        WHERE {where_clause}
        """
        result = exec_sql_s(sql)
        if result:
            res_lst.append((result, table))
    # else:
    #     # The 'else' clause in a for loop runs only if no 'break' was encountered.
    #     # Here it just prints if no results were found.
    #     if not res_lst:
    #         if DEBUG_VER == 3:
    #             print(f"未在任何表中找到上市公司名称为 {value} 的信息。")
        else:
            # If no result, modify query for fuzzy matching
            fuzzy_match_conditions = [f"{col} LIKE '%{value}%' " for col in local_match_cols]
            fuzzy_where_clause = ' OR '.join(fuzzy_match_conditions)

            # Query with fuzzy match
            sql_fuzzy = f"""
            SELECT {', '.join(local_select_cols)}
            FROM {table}
            WHERE {fuzzy_where_clause}
            """
            fuzzy_result = exec_sql_s(sql_fuzzy)
            if fuzzy_result:
                res_lst.append((fuzzy_result, table))
                
    # If no results found after both exact and fuzzy matches
    if not res_lst:
        if DEBUG_VER == 3:
            print(f"未在任何表中找到上市公司名称为 {value} 的信息。")
            
    return res_lst

def process_code(value):
    """
    Given a code (e.g., a stock code), search the three tables and return matches.

    Parameters:
        value (str): The code to search for.

    Returns:
        list: A list of tuples (result, table) if found, else empty.
    """
    res_lst = []
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    if len(QUESTION_TYPE_LIST) == 0:
        tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    elif len(QUESTION_TYPE_LIST) >= 2:
        tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    elif "港股" in QUESTION_TYPE_LIST:
        tables = ['ConstantDB.HK_SecuMain']
    elif "美股" in QUESTION_TYPE_LIST:
        tables = ['ConstantDB.US_SecuMain']
    elif "A股" in QUESTION_TYPE_LIST:
        tables = ['ConstantDB.SecuMain']
    else:
        tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    # print(f"--------------tables:{tables}")
    columns_to_select = ['InnerCode', 'CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                         'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']

    value = value.replace("'", "''")  # Escape single quotes

    for table in tables:
        local_select_cols = columns_to_select.copy()
        if 'US' in table:
            if 'ChiNameAbbr' in local_select_cols:
                local_select_cols.remove('ChiNameAbbr')
            if 'EngNameAbbr' in local_select_cols:
                local_select_cols.remove('EngNameAbbr')

        sql = f"""
        SELECT {', '.join(local_select_cols)}
        FROM {table}
        WHERE SecuCode = '{value}'
        """
        result = exec_sql_s(sql)
        if result:
            res_lst.append((result, table))
    else:
        if not res_lst:
            if DEBUG_VER == 3:
                print(f"未在任何表中找到代码为 {value} 的信息。")
    return res_lst


def process_jj(value):
    sql = f"SELECT ChiName, CompanyCode FROM InstitutionDB.LC_InstiArchive WHERE ChiName LIKE '%{value}%'"
    res = exec_sql_s(sql)
    if res:
        return [(res,'InstitutionDB.LC_InstiArchive')]
    else:
        res_lst = process_company_name(value)
        if res_lst:
            return res_lst
        return []


def process_items(item_list):
    """
    Given a list of items (dictionaries) from JSON extraction, attempt to process each based on its key:
    - If key is '基金名称' or '上市公司名称', use process_company_name.
    - If key is '代码', use process_code.
    - Otherwise, print an unrecognized key message.

    Parameters:
        item_list (list): A list of dictionaries like [{"上市公司名称": "XX公司"}, {"代码":"600872"}].

    Returns:
        tuple: (res, tables)
               res (str): A formatted string showing what was found.
               tables (list): A list of table names where matches were found.
    """
    res_list = []
    try:
        for item in item_list:
            key, value = list(item.items())[0]
            if key in ["基金名称", "上市公司名称"]:
                res_list.extend(process_company_name(value))
            elif key == "代码":
                res_list.extend(process_code(value))
            elif key == "基金公司简称":
                res_list.extend(process_jj(value))
            else:
                if DEBUG_VER == 3:
                    print(f"无法识别的键：{key}")
                pass
    except Exception as e:
        if DEBUG_VER == 3:
            print(f"process_items 发生错误: {e}")
        pass
    # Filter out empty results
    res_list = [i for i in res_list if i]
    res = ''
    tables = []
    for result_data, table_name in res_list:
        tables.append(table_name)
        res += f"预处理程序通过表格：{table_name} 查询到以下内容：\n {json.dumps(result_data, ensure_ascii=False, indent=1)} \n"

    return res, tables


def process_question(question):
    """
    Given a question, run it through a prompt to perform Named Entity Recognition (NER),
    extract entities (上市公司名称, 代码, 基金名称), parse the assistant's JSON response,
    and process the items to retrieve relevant information from the database.

    Parameters:
        question (str): The user question.

    Returns:
        tuple: (res, tables) where
               res (str) - Processed result details as a string.
               tables (list) - List of tables involved in the final result.
    """
    prompt = '''
    你将会进行命名实体识别任务，并输出实体json。你只需要识别以下4种实体：
    
    -上市公司名称
    -代码
    -基金名称
    -基金公司简称

    其中，上市公司名称可以是全称，简称，拼音缩写，代码包含股票代码和基金代码，基金名称包含债券型基金，
    以下是几个示例：
    user:唐山港集团股份有限公司是什么时间上市的（回答XXXX-XX-XX）
    当年一共上市了多少家企业？
    这些企业有多少是在北京注册的？
    assistant:```json
    [{"上市公司名称":"唐山港集团股份有限公司"}]
    ```
    user:JD的职工总数有多少人？
    该公司披露的硕士或研究生学历（及以上）的有多少人？
    20201月1日至年底退休了多少人？
    assistant:```json
    [{"上市公司名称":"JD"}]
    ```
    user:600872的全称、A股简称、法人、法律顾问、会计师事务所及董秘是？
    该公司实控人是否发生改变？如果发生变化，什么时候变成了谁？是哪国人？是否有永久境外居留权？（回答时间用XXXX-XX-XX）
    assistant:```json
    [{"代码":"600872"}]
    ```
    user:华夏鼎康债券A在2019年的分红次数是多少？每次分红的派现比例是多少？
    基于上述分红数据，在2019年最后一次分红时，如果一位投资者持有1000份该基金，税后可以获得多少分红收益？
    assistant:```json
    [{"基金名称":"华夏鼎康债券A"}]
    ```
    user:实体识别任务：```易方达基金管理有限公司在19年成立了多少支基金？
    哪支基金的规模最大？
    这支基金20年最后一次分红派现比例多少钱？```
    assistant:```json
    [{"基金公司简称":"易方达"}]
    ```
    user:化工纳入过多少个子类概念？
    assistant:```json
    []
    ```
    '''
    messages = [{'role': 'system', 'content': prompt}, {'role': 'user', 'content': question}]
    aa = create_chat_completion(messages)
    bb = find_json(aa.choices[0].message.content)
    return process_items(bb)

def question_rew(context_text, original_question):
    """
    Rewrite the given question to be clearer and more specific based on the provided context,
    without altering the original meaning or omitting any information.
    
    Parameters:
        context_text (str): The context text that the question is based on.
        original_question (str): The question to be rewritten.
        
    Returns:
        str: The rewritten question.
    """
    prompt = (
        f"根据这些内容：'{context_text}',帮我重写当前问题：'{original_question}' ,让问题清晰明确，"
        "不改变原意，代词转成具体人事物，不要遗漏信息，只返回问题。"
        "如果当前问题中有时间代词（如“当年、当天”等）或指物代词（如“该公司”、“它”等），检查前面问题和回答（一般是上一个，就近原则）中是否明确了时间或主体等，并将这些信息补充到当前问题中。"
        "如果当前问题无法从前面问题和回答中找到代词（如时间）所指的具体信息，则表示当前代词，如时间指全部时间。"
        "问题可能需要时间，也可能不需要时间，如果不需要则在后面追加一个不带时间的小问题（不需要换行等，只需要接在原问题后面）。"
        "让我们一步一步思考"
        "以下是几个示例：\n"
        "user:根据这些内容：'第1轮问题：最新更新的2021年度报告中，机构持有无限售流通A股数量合计最多的公司简称是？ 第1轮回答：公司简称 帝尔激光',帮我重写当前问题：'在这份报告中，该公司机构持有无限售流通A股比例合计是多少，保留2位小数？'"
        "assistant:最新更新的2021年度报告中,公司简称 帝尔激光 持有无限售流通A股比例合计是多少，保留2位小数？"
        "user:根据这些内容：'第1轮问题：TK他是否已经成立了？（是或者否） 第1轮回答：是',帮我重写当前问题：'这家公司17年最高收盘价是多少？'"
        "assistant: 2017年，TK这家公司的最高收盘价是多少？"
        "user:根据这些内容：'第1轮问题：TK他是否已经成立了？（是或者否） 第1轮回答：是 \n第2轮问题：2017年，TK这家公司的最高收盘价是多少？  第2轮回答：2017年TK最高收盘价 5.79',帮我重写当前问题：'当天有多少家公司成立了？'"
        "assistant: 2017年TK最高收盘价 10.79 是什么时候？当天有多少家公司成立了？"
        "user:根据这些内容：'第1轮问题：航锦科技股份有限公司是否变更过公司名称？ 第1轮回答：否（没有）\n第2轮问题：？航锦科技股份有限公司涉及回购的最大的一笔金额是多少？第2轮回答：43951008.0',帮我重写当前问题：该年度前十大股东的持股比例变成了多少？"
        "assistant:？航锦科技股份有限公司涉及回购的最大的一笔金额 43951008.0是哪一年？该年度前十大股东的持股比例变成了多少？"
    )

    messages = [{"role": "user", "content": prompt}]
    response = create_chat_completion(messages)
    return response.choices[0].message.content

def process_dict(d):
    """
    Recursively process a nested dictionary to produce a comma-separated description.
    For nested dictionaries, it processes them recursively and returns a descriptive string.
    
    For example:
        {
            "company": {
                "name": "ABC Corp",
                "location": "New York"
            },
            "year": 2021
        }
    might be processed into a string like:
        "company company 是 name 是 ABC Corp, location 是 New York, year 2021"
    
    Parameters:
        d (dict): A dictionary or another object to describe.
        
    Returns:
        str: A descriptive string.
    """
    def recursive_process(sub_dict):
        sentences = []
        for key, value in sub_dict.items():
            if isinstance(value, dict):
                # Process nested dictionary and wrap result in dict_to_sentence for formatting
                nested_result = recursive_process(value)
                sentences.append(dict_to_sentence({key: nested_result}))
            else:
                # Non-dict values are directly appended
                sentences.append(f"{key} {value}")
        return ", ".join(sentences)

    if not isinstance(d, dict):
        # If it's not a dictionary, just return its string representation
        return str(d)

    return recursive_process(d)