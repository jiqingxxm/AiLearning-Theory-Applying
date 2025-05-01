# agent/schema/schema_utils.py
import jieba
import numpy as np
from collections import Counter
from config import *
from agent.utils import clean_text, find_dict_by_element

def parse_table_structures(input_text):
    """
    用于解析输入文本并提取表结构信息，返回一个字典，其中表名作为键，表结构作为值
    Parse the input text to extract table structures. 
    
    The format is expected as pairs: "table_name === table_structure".
    
    Parameters:
        input_text (str): The raw text containing table structures.
        
    Returns:
        tables_dict (dict): A dictionary where keys are table names and 
                            values are the associated table structures.
    """
    tables_text = input_text.split('===')[1:]
    tables_dict = {tables_text[i]: tables_text[i + 1] for i in range(0, len(tables_text), 2)}
    return tables_dict

def map_chinese_to_english_tables(chinese_names, english_names):
    """
    将中文的表名映射到对应的英文表名
    Map Chinese table names to their corresponding English table names.
    For each Chinese name, there is a matching English name 
    (case-insensitive comparison).
    
    Parameters:
        chinese_names (list): A list of Chinese table names.
        english_names (list): A list of English table names.
        
    Returns:
        name_map (dict): A dictionary mapping Chinese table names to English table names.
    """
    name_map = {}
    for cname in chinese_names:
        # Find the corresponding English name (case-insensitive match)
        english_match = [en for en in english_names if str(en).lower() == cname.lower()][0]
        name_map[cname] = english_match
    return name_map

def find_value_in_list_of_dicts(dict_list, key_to_match, value_to_match, key_to_return):
    """
    在字典列表中查找满足条件的字典，并返回其中指定键的值。
    Search through a list of dictionaries and find the first dictionary where 
    the value of key_to_match equals value_to_match, then return the value 
    associated with key_to_return.
    
    Parameters:
        dict_list (list): A list of dictionaries to search through.
        key_to_match (str): The key whose value we want to match.
        value_to_match (str): The value we are looking for.
        key_to_return (str): The key whose value we want to return.
        
    Returns:
        (str): The value associated with key_to_return in the matching dictionary, 
               or an empty string if no match is found.
    """
    for dictionary in dict_list:
        if dictionary.get(key_to_match) == value_to_match:
            return dictionary.get(key_to_return)
    return ''

def filter_table_comments(question, table_comments):
    """
    根据输入问题从表注释列表中筛选出与问题相关的注释。
    Filter a list of table comments based on the given question. 
    Uses jieba for segmentation and removes stopwords, returning only comments 
    that contain at least one of the segmented keywords.
    
    Parameters:
        question (str): The question text.
        table_comments (list): A list of comment strings to filter.
    
    Returns:
        filtered_comments (list): Filtered list of comments.
    """
    stopwords = ['？', '有', '的', '多少', '人', '（', '）']
    seg_list = list(jieba.cut(question, cut_all=False))
    filtered_seg_list = [word for word in seg_list if word not in stopwords]

    filtered_comments = []
    for comment in table_comments:
        if any(keyword in comment for keyword in filtered_seg_list):
            filtered_comments.append(comment)
    return filtered_comments

def get_table_schema(question=''):
    """
    获取表格的结构信息以及字段的注释
    Retrieve table schemas along with optional filtered field comments.
    If a question is provided, the comments will be filtered based on 
    question keywords.
    
    The function:
      1. Maps Chinese table names to English table names.
      2. For each table, retrieves its structure and finds associated comments.
      3. If a question is provided, filter the comments based on keywords extracted from the question.
    
    Parameters:
        question (str): The question text. If empty, no filtering is performed.
        
    Returns:
        table_maps (list): A list of dictionaries, each containing table schema information.
        {
            '数据表名': EnglishTableName,
            '数据表结构': TableStructure,
            '字段注释': FilteredComments (optional if question is provided)
        }
    """
    if QUESTION_TYPE == "全股":
        parsed_tables = parse_table_structures(input_text_all)
        database_L = database_L_all
        database_table_en = database_table_en_all
    elif QUESTION_TYPE == "港股":
        parsed_tables = parse_table_structures(input_text_hk)
        database_L = database_L_hk
        database_table_en = database_table_en_hk
    elif QUESTION_TYPE == "美股":
        parsed_tables = parse_table_structures(input_text_us)
        database_L = database_L_us
        database_table_en = database_table_en_us
    elif QUESTION_TYPE == "A股":
        parsed_tables = parse_table_structures(input_text_cn)
        database_L = database_L_cn
        database_table_en = database_table_en_cn
    else:
        parsed_tables = parse_table_structures(input_text_all)
        database_L = database_L_all
        database_table_en = database_table_en_all

    # Clean up keys and values
    cleaned_tables = {
        k.replace(' ', '').replace('表结构', ''): v.replace('--', '')
        for k, v in parsed_tables.items()
    }

    # List of Chinese table names (keys)
    chinese_table_names = list(cleaned_tables.keys())

    name_map = map_chinese_to_english_tables(chinese_table_names, database_table_en)

    table_maps = []
    for cname, structure in cleaned_tables.items():
        english_name = name_map.get(cname)
        comments = find_value_in_list_of_dicts(database_L, '数据表名', english_name, '注释')

        if question == '':
            # No filtering, just return table name and structure
            table_map = {
                '数据表名': english_name,
                '数据表结构': structure
            }
        else:
            # Filter comments based on question
            filtered_comments = filter_table_comments(question, comments)
            table_map = {
                '数据表名': english_name,
                '数据表结构': structure,
                '字段注释': filtered_comments
            }

        table_maps.append(table_map)
    return table_maps

def to_get_question_columns(question):
    """
    Given a question (string) and a global variable database_L_zh (list of dicts),
    find 列名 that correspond to 列名中文描述 mentioned in the question. 
    
    If any matching columns are found, return a message instructing the user to 
    use these column names directly for data querying. If none are found, return an empty string.
    
    Parameters:
        question (str): The input question text.
        
    Returns:
        str: A message with identified column names or an empty string if none found.
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

    L_num = []
    for items in database_L_zh:
        L_num += items['列名中文描述']

    # Get unique column descriptions
    L_num_new = [item for item, count in Counter(L_num).items() if count == 1]

    # Drop NaN if any
    series_num = pd.Series(L_num_new)
    L_num_new = list(series_num.dropna())

    # Remove known irrelevant items
    irrelevant_items = ['年度', '占比']
    for irr in irrelevant_items:
        if irr in L_num_new:
            L_num_new.remove(irr)

    matched_columns = []
    for col_descs in L_num_new:
        col_desc_another = items_another[col_descs]
        for col_desc in col_desc_another:
            # Check if the column description or its cleaned version appears in the question
            if col_desc in question or clean_text(col_desc) in question:
                L_dict = find_dict_by_element(database_L_zh, col_descs)
                if not L_dict:
                    break
                # Create a mapping from Chinese description to English column name
                dict_zip = dict(zip(L_dict[0]['列名中文描述'], L_dict[0]['列名']))
                column_name = dict_zip[col_descs]
                data_table = L_dict[0]['数据表名']

                matched_columns.append({
                    '数据库表': data_table,
                    '列名': column_name,
                    '列名中文含义': col_descs
                })
                break

    if matched_columns:
        return f"已获得一部分数据库列名{matched_columns}，请充分利用获得的列名直接查询数据。"
    else:
        return ''
    
def get_table_schema_with_emb(tables_name=[], question='', threshold=0.35, print_all=True):
    # tables_name是英文名的list
    if len(QUESTION_TYPE_LIST) == 0:
        parsed_tables = parse_table_structures(input_text_all)
        database_L = database_L_all
        database_table_en = database_table_en_all
    elif len(QUESTION_TYPE_LIST) >= 2:
        parsed_tables = parse_table_structures(input_text_all)
        database_L = database_L_all
        database_table_en = database_table_en_all
    elif "港股" in QUESTION_TYPE_LIST:
        parsed_tables = parse_table_structures(input_text_hk)
        database_L = database_L_hk
        database_table_en = database_table_en_hk
    elif "美股" in QUESTION_TYPE_LIST:
        parsed_tables = parse_table_structures(input_text_us)
        database_L = database_L_us
        database_table_en = database_table_en_us
    elif "A股" in QUESTION_TYPE_LIST:
        parsed_tables = parse_table_structures(input_text_cn)
        database_L = database_L_cn
        database_table_en = database_table_en_cn
    else:
        parsed_tables = parse_table_structures(input_text_all)
        database_L = database_L_all
        database_table_en = database_table_en_all
        
    # Clean up keys and values
    cleaned_tables = {
        k.replace(' ', '').replace('表结构', ''): v.replace('--', '')
        for k, v in parsed_tables.items()
    }

    columns_to_clean = ["column_description"]
    time_columns = ["TradingDay", "EndDate", "InfoPublDate"]
    remove_columns = ['InitialInfoPublDate', 'XGRQ','InsertTime','UpdateTime']
    table_maps = []
    highest_score_list = []
    if tables_name == [] or question == '':
        return None
    for table_name in tables_name:
        if DEBUG_VER == 3:
            print(f"---------->table_name:{table_name}")
        for english_name, structure in cleaned_tables.items():
            if english_name.lower() == table_name.lower():
                # print(f"------------threshold:{threshold}")
                filtered_comments = []
                df_tmp = df_all2[df_all2["table_name"]==table_name.split(".")[1]]
                imp_matched_columns, matched_columns_with_scores = extract_matching_columns_with_similarity(
                    df_tmp, question, threshold, MAX_TOP_COLUMNS
                )
                if matched_columns_with_scores == [] and imp_matched_columns == []:
                    # print(f"------------pass")
                    pass
                else:
                    # print(f"------------no pass")
                    # 过滤掉不需要的列
                    remove_columns = ["InitialInfoPublDate", "XGRQ", "InsertTime", "UpdateTime"]
                    matched_columns = [
                        item["column"] for item in matched_columns_with_scores if item["column"] not in remove_columns
                    ]
                    matched_columns = [column for column in matched_columns if column not in remove_columns]
                    highest_score_item = max(matched_columns_with_scores, key=lambda x: x["similarity_score"])
                    highest_score = highest_score_item["similarity_score"]
                    if print_all:
                        pass
                        # print(f"------------table_name:{table_name}")
                        # print(f"------------imp_matched_columns:{imp_matched_columns}, matched_columns_with_scores:{matched_columns_with_scores}")
                    else:
                        if (highest_score > threshold) or imp_matched_columns != []:
                            pass
                            # print(f"------------table_name:{table_name}")
                            # print(f"------------imp_matched_columns:{imp_matched_columns}, matched_columns_with_scores:{matched_columns_with_scores}")
                    filtered_structure = filter_structure(structure, matched_columns, time_columns)
                    filtered_comments = extract_column_descriptions(filtered_structure, df_tmp)
                    table_map = {
                        '数据表名': table_name,
                        '数据表结构': filtered_structure,
                        '部分字段注释补充': filtered_comments
                    }
                    table_maps.append(table_map)
                    highest_score_list.append(highest_score)
    if not table_maps or not highest_score_list:
        return [], []
    
    # 将 table_maps 和 highest_score_list 按分数排序
    combined_list = list(zip(table_maps, highest_score_list))
    sorted_combined_list = sorted(combined_list, key=lambda x: x[1], reverse=True)
    table_maps, highest_score_list = zip(*sorted_combined_list)
    table_maps = list(table_maps)
    highest_score_list = list(highest_score_list)
    return table_maps, highest_score_list

def deep_find_tables(all_tables_name_list=[], question='', threshold=0.6, print_all=False, top_table_n=5):
    # tables_name是英文名的list
    if len(QUESTION_TYPE_LIST) == 0:
        df_tmp = df_all
    elif len(QUESTION_TYPE_LIST) >= 2:
        df_tmp = df_all
    elif "港股" in QUESTION_TYPE_LIST:
        df_tmp = df_hk
    elif "美股" in QUESTION_TYPE_LIST:
        df_tmp = df_us
    elif "A股" in QUESTION_TYPE_LIST:
        df_tmp = df_cn
    else:
        df_tmp = df_all
    if all_tables_name_list==[] or question=='':
        return []
    all_table_list = df_tmp['库表名英文'].values.tolist()
    all_table_list = [table for table in all_table_list if table not in all_tables_name_list]
    all_table_list = list(set(all_table_list))
    table_maps, highest_score_list = get_table_schema_with_emb(all_table_list, question, threshold, print_all)
    return table_maps[:top_table_n]

# %% [code] {"execution":{"iopub.status.busy":"2025-03-07T09:14:36.671853Z","iopub.execute_input":"2025-03-07T09:14:36.672272Z","iopub.status.idle":"2025-03-07T09:14:36.693550Z","shell.execute_reply.started":"2025-03-07T09:14:36.672191Z","shell.execute_reply":"2025-03-07T09:14:36.692326Z"},"jupyter":{"source_hidden":true}}
def find_table_with_emb(all_tables_name_list=[], question='', use_local_vectors=USE_LOCAL_VECTORS):
    """
    判断后返回最高分的表（始终返回1个）：
    1. 则对 df_tmp 中所有唯一的库表进行批量比较，并记录每个表的相似度。
    2. 最后合并候选结果，从中选取相似度最高的表返回（返回列表中只含1个表）。
    """
    # 根据 QUESTION_TYPE_LIST 选择对应的数据集
    if len(QUESTION_TYPE_LIST) == 0:
        df_tmp = df_all
        embeddings_path = df_all_embeddings_path
    elif len(QUESTION_TYPE_LIST) >= 2:
        df_tmp = df_all
        embeddings_path = df_all_embeddings_path
    elif "港股" in QUESTION_TYPE_LIST:
        df_tmp = df_hk
        embeddings_path = df_hk_embeddings_path
    elif "美股" in QUESTION_TYPE_LIST:
        df_tmp = df_us
        embeddings_path = df_us_embeddings_path
    elif "A股" in QUESTION_TYPE_LIST:
        df_tmp = df_cn
        embeddings_path = df_cn_embeddings_path
    else:
        df_tmp = df_all
        embeddings_path = df_all_embeddings_path

    if not all_tables_name_list or question == '':
        return []

    candidate_scores = []
    df_unique = df_tmp[['库表名英文', 'table_describe']].drop_duplicates()
    candidate_ids_deep = df_unique['库表名英文'].tolist()
    candidate_texts_deep = df_unique['table_describe'].tolist()
    
    if use_local_vectors:
        texts_for_batch_deep = [question]
        similarity_scores_deep = calculate_similarity(texts_for_batch_deep, local_vectors=embeddings_path)
    else:
        texts_for_batch_deep = [question] + candidate_texts_deep
        similarity_scores_deep = calculate_similarity(texts_for_batch_deep)
        
    for table, sim in zip(candidate_ids_deep, similarity_scores_deep):
        candidate_scores.append((table, sim))
    # 如果没有候选表，则返回空列表
    if not candidate_scores:
        return []

    # 从所有候选中选取相似度最高的表（不论是否达到 threshold）
    best_table, best_sim = max(candidate_scores, key=lambda x: x[1])
    if DEBUG_VER == 3:
        print(f"Best table: {best_table} with similarity: {best_sim}")
    return [best_table]

def extract_matching_columns_with_similarity(df, question, threshold, top_n=25):
    """
    根据question提取匹配列：
      1. 对于每行的字段（如 column_description 与 注释），先做全量匹配，
         如果文本完全包含在question中，则直接赋分1.0。
      2. 同时收集所有候选文本，最后批量调用 calculate_similarity，
         将 question 与所有候选文本比较，得到相似度分数（只保留达到阈值的）。
      3. 同一列可能对应多个候选文本，最终保留相似度最高的得分。
    返回：
      - imp_matched_columns：全量匹配（得分1.0）的列名列表
      - topn_matched_columns：满足阈值的列及其相似度分数（按分数降序排列，最多 top_n 个）
    """
    imp_matched_columns_with_scores = {}  # 全量匹配得分为1.0的列
    matched_columns_with_scores = {}      # 语义匹配得分（可能多次出现，取最高分）

    candidate_texts = []  # 收集待计算相似度的候选文本
    candidate_ids = []    # 对应候选文本所属的列名

    # 遍历DataFrame中的每一行
    for _, row in df.iterrows():
        col_name = row["column_name"]

        # 处理 column_description 字段
        if isinstance(row["column_description"], str):
            col_desc = row["column_description"]
            # 全量匹配检查：如果候选文本出现在question中，则直接记1.0分
            if col_desc in question:
                imp_matched_columns_with_scores[col_name] = 1.0
                matched_columns_with_scores[col_name] = 1.0
            # 无论全量匹配与否，都加入候选列表进行语义匹配
            else:
                candidate_texts.append(col_desc)
                candidate_ids.append(col_name)

        # 处理 注释 字段（假设多个注释以“，”分隔）
        if isinstance(row["注释"], str):
            words = row["注释"].split("，")
            for word in words:
                if word in question:
                    imp_matched_columns_with_scores[col_name] = 1.0
                    matched_columns_with_scores[col_name] = 1.0
                # candidate_texts.append(word)
                # candidate_ids.append(col_name)

    # 批量计算语义相似度：将 question 与所有候选文本一次性比较
    if candidate_texts:
        # 构造输入列表：第一个文本为 question，其余为所有候选文本
        texts_for_batch = [question] + candidate_texts
        similarity_scores = calculate_similarity(texts_for_batch)
        # 遍历每个候选文本的相似度
        for idx, sim in enumerate(similarity_scores):
            if sim >= threshold:
                col = candidate_ids[idx]
                # 同一列可能有多个候选文本，保留相似度最高的得分
                matched_columns_with_scores[col] = max(matched_columns_with_scores.get(col, 0), sim)

    # 合并全量匹配和语义匹配的结果
    all_matched_columns = {**matched_columns_with_scores, **imp_matched_columns_with_scores}
    # 按相似度分数降序排列（注意：由于全量匹配的分数为1.0，通常会排在最前面）
    unique_matched_columns = {col: score for col, score in sorted(all_matched_columns.items(), key=lambda x: -x[1])}

    # 提取全量匹配列
    imp_matched_columns = [col for col, score in unique_matched_columns.items() if score == 1.0]

    # 筛选出满足阈值的列，并构造输出格式（字典列表），按分数降序排序
    matched_columns = sorted(
        [{"column": col, "similarity_score": score} for col, score in unique_matched_columns.items() if score >= threshold],
        key=lambda x: x["similarity_score"],
        reverse=True
    )
    topn_matched_columns = matched_columns[:top_n]
    return imp_matched_columns, topn_matched_columns

def filter_structure(structure, matched_columns, time_columns):
    # 分割表头和数据部分
    sections = structure.split("\n\n", 1)
    header = sections[0]  # 表头部分
    data_rows = sections[1] if len(sections) > 1 else ""  # 数据部分
    
    # 条件检查函数
    def satisfies_conditions(row):
        row_fields = row.split()  # 假设字段之间是用空格分隔的
        # 完全匹配检查
        if any(col == field for field in row_fields for col in matched_columns):
            return True
        if any(col == field for field in row_fields for col in time_columns):
            return True
        if any(keyword in row for keyword in ["Code", "Abbr", "Name"]):
            return True
        return False

    # 逐行过滤数据部分
    filtered_rows = []
    for row in data_rows.strip().split("\n"):
        if satisfies_conditions(row):
            filtered_rows.append(row)

    # 将过滤后的内容与表头合并
    filtered_structure = header + "\n\n" + "\n".join(filtered_rows) if filtered_rows else header
    return filtered_structure

def extract_column_descriptions(filtered_structure, df_tmp):
    # 从"\n\n"开始提取内容部分
    content = filtered_structure.split("\n\n", 1)[1].strip()

    # 提取列名（每行第一个空格前的部分）
    column_names = []
    for line in content.split("\n"):
        column_name = line.split()[0]  # 获取第一个空格前的内容
        column_names.append(column_name)

    # 转换 df_tmp 为字典形式，方便查找
    column_dict = dict(zip(df_tmp["column_name"], df_tmp["注释"]))

    # 构造结果列表
    result = []
    for column_name in column_names:
        if column_name in column_dict and len(str(column_dict[column_name]))>3:
            result.append({column_name: column_dict[column_name]})

    return result

def calculate_similarity(text_list, local_vectors=False):
    """
    批量计算相似度：
      - 输入一个文本列表，其中第一个文本作为基准，其余文本与基准比较
      - 当候选文本超过 64 条时，会分批请求，最后返回所有候选文本与基准文本的相似度（保留4位小数）
    """

    base_text = text_list[0]
    # 先单独请求基准文本的 embedding
    base_response = client.embeddings.create(
        model="embedding-3",
        input=[base_text]
    )
    base_embedding = base_response.data[0].embedding
    base_embedding = np.array(base_embedding)

    all_similarities = []
    if local_vectors:
        if DEBUG_VER == 3:
            print(f'------>local_vectors:{local_vectors}')
        # 使用本地保存的向量
        with open(local_vectors, 'r', encoding='utf-8') as f:
            local_embeddings = json.load(f)
        
        # 确保读取到的向量与候选文本对应
        candidate_embeddings = np.array(local_embeddings)
    else:
        # 批量处理候选文本，每次最多请求64条
        candidate_texts = text_list[1:]
        candidate_embeddings = []
        chunk_size = 64  # 每次最多请求64条
        for i in range(0, len(candidate_texts), chunk_size):
            chunk = candidate_texts[i:i+chunk_size]
            response = client.embeddings.create(
                model="embedding-3",
                input=chunk
            )
            # 提取候选文本的 embedding 并转换为 NumPy 数组
            embeddings = [item.embedding for item in response.data]
            candidate_embeddings.extend(embeddings)
        
        candidate_embeddings = np.array(candidate_embeddings)

    # 计算余弦相似度： dot / (||base|| * ||candidate||)
    dot_products = candidate_embeddings.dot(base_embedding)
    norm_base = np.linalg.norm(base_embedding)
    norm_candidates = np.linalg.norm(candidate_embeddings, axis=1)
    similarities = dot_products / (norm_base * norm_candidates)

    # 保留4位小数，并加入结果列表
    sims = [round(float(sim), 4) for sim in similarities]
    all_similarities.extend(sims)

    return all_similarities
