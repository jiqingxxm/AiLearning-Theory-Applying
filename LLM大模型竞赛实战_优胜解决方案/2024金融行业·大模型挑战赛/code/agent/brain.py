import time, gc
from config import *
from agent.question.question_utils import preprocessing_question, process_question, process_dict, add_question_mark, question_rew
from agent.prompt import create_chat_completion, create_chat_completion_change_zhushi
from agent.schema.schema_utils import get_table_schema, find_table_with_emb, get_table_schema_with_emb, deep_find_tables 
from agent.sql.exec import exec_sql_s
from agent.sql.process_sql import to_select, extract_table_names, all_tables_in_prompt
from agent.sql.knowledge import way_string_2
from agent.optimization import optimize_answer, find_kl_answer
from agent.utils.async_util import async_llm_chain_call
from agent.utils.io_utils import load_external_answers
from agent.utils.json_utils import find_json

def run_conversation_xietong(question, org_question):
    global QUESTION_TYPE
    global QUESTION_TYPE_LIST
    global LL
    global table_maps
    global table_maps_LL
    global all_tables_name_list
    str_process_question = process_question(question)
    if DEBUG_VER == 3:
        print(f"-------------已查询获得事实: {str_process_question}")

    if "ConstantDB.HK_SecuMain" in str(str_process_question) and "港股" not in QUESTION_TYPE_LIST:
        QUESTION_TYPE_LIST.append("港股")
    if "ConstantDB.US_SecuMain" in str(str_process_question) and "美股" not in QUESTION_TYPE_LIST:
        QUESTION_TYPE_LIST.append("美股")
    if "ConstantDB.SecuMain" in str(str_process_question) and "A股" not in QUESTION_TYPE_LIST:
        QUESTION_TYPE_LIST.append("A股")

    content_p_1 = content_ALL
    if len(QUESTION_TYPE_LIST) == 0:
        content_p_1 = content_ALL
    elif len(QUESTION_TYPE_LIST) >= 2:
        content_p_1 = content_ALL
    elif "港股" in QUESTION_TYPE_LIST:
        content_p_1 = content_HK
    elif "美股" in QUESTION_TYPE_LIST:
        content_p_1 = content_US
    elif "A股" in QUESTION_TYPE_LIST:
        content_p_1 = content_CN
    else:
        content_p_1 = content_ALL

    if DEBUG_VER == 3:
        print(f"---------QUESTION_TYPE_LIST:{QUESTION_TYPE_LIST}")
    content_p = content_p_1.replace('<<question>>', str(question)).replace('<<fact_1>>',
                                                                           str(str_process_question))
    main_sql_prompts,highest_prompt_similar = way_string_2(org_question)
    content_p = content_p + main_sql_prompts
    # 执行函数部分
    messages = []
    messages.append({"role": "user", "content": "您好阿"})
    messages.append({"role": "user", "content": content_p})
    response = create_chat_completion(messages)
    
    if "全部完成" in response.choices[0].message.content:
        if "XX" in response.choices[0].message.content:
            response.choices[0].message.content += "ConstantDB.SecuMain,ConstantDB.HK_SecuMain,ConstantDB.US_SecuMain"
        else:
            if DEBUG_VER == 3:
                print("全部完成：response：",response.choices[0].message.content)
            if "需要查询的数据库表" in response.choices[0].message.content or "需要查询数据库表" in response.choices[0].message.content:
                pass
            else:
                return response.choices[0].message.content

    messages.append({"role": "assistant", "content": response.choices[0].message.content})
    table_maps = get_table_schema(question)
    LL = [i for i in table_maps if i.get('数据表名') in response.choices[0].message.content]
    tables_name_list = [i.get('数据表名') for i in table_maps if i.get('数据表名') in response.choices[0].message.content]
    try:
       sql_prompt_db_tables = extract_table_names(main_sql_prompts)
    except Exception as e:
        if DEBUG_VER == 3:
            print(f"------->e:{e}")
    if sql_prompt_db_tables:
        all_tables_name_list += sql_prompt_db_tables
    all_tables_name_list += tables_name_list
    all_tables_name_list = list(set(all_tables_name_list))
    if DEBUG_VER == 3:
        print(f"-----------------all_tables_name_list:{all_tables_name_list}")
    all_tables_name_list += find_table_with_emb(all_tables_name_list, question)
    all_tables_name_list = list(set(all_tables_name_list))
    if DEBUG_VER == 3:
        print(f"-----------------find_table_with_emb all_tables_name_list:{all_tables_name_list}")
    if all_tables_name_list == []:
        highest_similarity_score_list = [0]
    else:
        table_maps_LL, highest_similarity_score_list = get_table_schema_with_emb(all_tables_name_list, question)
        # print(f"--------------table_maps_LL:{table_maps_LL}")
    if all_tables_in_prompt(tables_name_list, main_sql_prompts) and highest_prompt_similar > START_DEEP_THRESHOLD:
        content_p_2 = """获取的表结构如下<list>,表结构中列名可以引用使用,表结构中数据示例只是参考不能引用。
    我们现在开始查询当前问题，可参考示例sql写出查询sql语句，如果示例sql是一句的（一步），你也尽可能用一句sql得到结果。
    如果需要多步，我把查询结果告诉你，你再告诉我下一步，注意如果我返回的结果为空或者错误影响下一步调用，请重新告诉我sql语句。
    等你全部回答完成，不需要进行下一步调用时，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我，只需要总结当前问题。
    查询技巧: 涉及计算的使用sql进行加减乘除数学计算。查询尽量使用InnerCode。查年用year()函数、月用month()、某日则用date()，如：date(TradingDay) = 'YYYY-MM-DD'。sql查询语句不需要注释，不然会报错。尽量利用表格中已有的字段。\n"""
        
    else:
        if DEEP_COLUMNS_RESEARCH:
            if max(highest_similarity_score_list) < START_DEEP_THRESHOLD: # 没有大于0.5的说明这个表可能不对，新增表
                print(f"-----------Start Deeping table search")
                deep_table_maps_LL = deep_find_tables(all_tables_name_list, org_question)  # 直接用原始的扫
                print(f"--------------len(deep_table_maps_LL):{len(deep_table_maps_LL)}")
                deep_tables_name_list = [i.get('数据表名') for i in deep_table_maps_LL]
                all_tables_name_list += deep_tables_name_list
                all_tables_name_list = list(set(all_tables_name_list))
                
            # org_table_maps_LL, org_question_similarity_score_list = get_table_schema_with_emb(all_tables_name_list, org_question)
            # # print(f"--------------org_table_maps_LL:{org_table_maps_LL}")
            # if max(org_question_similarity_score_list) < START_DEEP_THRESHOLD: # 没有大于0.5的说明这个表可能不对，新增表
            #     print(f"-----------Start org Deeping table search")
            #     org_deep_table_maps_LL = deep_find_tables(all_tables_name_list, org_question)
            #     print(f"--------------len(org_deep_table_maps_LL):{len(org_deep_table_maps_LL)}")
            #     org_deep_tables_name_list = [i.get('数据表名') for i in org_deep_table_maps_LL]
            #     all_tables_name_list += org_deep_tables_name_list
            #     all_tables_name_list = list(set(all_tables_name_list))
    
            if max(highest_similarity_score_list) < START_DEEP_THRESHOLD:
                table_maps_LL, highest_similarity_score_list = get_table_schema_with_emb(all_tables_name_list, question)
                if DEBUG_VER == 3:
                    print(f"--------------End Deeping table_maps_LL:{table_maps_LL}")
        
        content_p_2 = """获取的表结构如下<list>,表结构中列名可以引用使用,表结构中数据示例只是参考不能引用。
    我们现在开始查询当前问题，请你分步写出查询sql语句，我把查询结果告诉你，你再告诉我下一步，
    注意如果我返回的结果为空或者错误影响下一步调用，请重新告诉我sql语句。
    等你全部回答完成，不需要进行下一步调用时，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我，只需要总结当前问题。
    查询技巧: 涉及计算的使用sql进行加减乘除数学计算。查询尽量使用InnerCode。查年用year()、查月用month()、某日用date()，如：date(TradingDay) = 'YYYY-MM-DD'。sql查询语句不需要注释，不然会报错。尽量利用表格中已有的字段。\n"""
    if len(str_process_question[1]) == 0:
        if DEBUG_VER == 3:
            print("--------------查询获得事实为空，跳过处理")
    else:
        if FRASH_DATA_DESCRIBE:
            table_maps_LL = create_chat_completion_change_zhushi(str_process_question, table_maps_LL)

    content_p_2 = main_sql_prompts + content_p_2.replace('<list>', str(table_maps_LL))
    if DEBUG_VER == 3:
        print(f"-------------content_p_2: {content_p_2}")
    messages.append({"role": "user", "content": content_p_2})  ###开始对话 
    last_answer = run_conversation_until_complete(messages, max_rounds=MAX_ROUNDS)
    return last_answer

def run_conversation_until_complete(messages, max_rounds=9):
    """
    Test function to run a conversation loop until the assistant indicates completion.
    """
    last_response = None  # 用于存储最后一次对话的响应
    round_count = 0  # 对话轮数计数器
    # response = create_chat_completion(messages)
    if ASYNC_LLM_TIME >= 2:
        response = async_llm_chain_call(messages, MODEL)
    else:
        response = create_chat_completion(messages)
    
    while True:
        if round_count >= max_rounds:
            break  # 如果对话轮数超过最大值，则退出循环

        question = response.choices[0].message.content
        select_result = to_select(question)
        messages.append({"role": "assistant", "content": question})
        messages.append({"role": "user", "content": str(select_result)})

        # response = create_chat_completion(messages)
        if ASYNC_LLM_TIME >= 2:
            response = async_llm_chain_call(messages, MODEL)
        else:
            response = create_chat_completion(messages)

        last_response = response.choices[0].message.content  # 存储最后一次响应       
        if "全部完成" in response.choices[0].message.content:
            break  # 如果检测到“回答完成”，则停止循环
        round_count += 1  # 增加对话轮数计数
    return last_response  # 返回最后一次对话的内容

def run_conversation(question, org_question):
    """
    Run a conversation flow given a question by:
      1. Using run_conversation_xietong(question) to get an answer.
      2. Attempting to find and parse JSON from the answer using find_json.
      3. Converting the parsed result (or original text if parsing fails) into a descriptive sentence using process_dict.
    
    Parameters:
        question (str): The question to ask.
        
    Returns:
        str: The final processed answer as a descriptive string.
    """
    org_answer = run_conversation_xietong(question, org_question)
    parsed_data = find_json(org_answer)
    last_answer = process_dict(parsed_data)
    return last_answer,org_answer

def get_answer(question, org_question):
    """
    Attempt to answer the given question by interacting with the 
    conversation model. If an error occurs, return a default error message.
    
    Parameters:
        question (str): The question that needs an answer.
        
    Returns:
        str: The answer string or an error message if an exception occurs.
    """
    try:
        if DEBUG_VER == 3:
            print(f"Attempting to answer the question: {question}")
        last_answer,org_answer = run_conversation(question, org_question)
        return last_answer,org_answer
    except Exception as e:
        if DEBUG_VER == 3:
            print(f"Error occurred while executing get_answer: {e}")
        return "An error occurred while retrieving the answer."

def main_answer(q_json_list, out_path, idx_ranges):
    """
    处理整个 JSON 数据列表：
      - 如果当前索引在 idx_ranges 中任一范围内，则按照原有逻辑提取问题、重写问题、调用 get_answer 得到答案，
        并更新每个问题对应的 answer 字段；
      - 否则将每个 team 成员的 answer 字段更新为空。
    参数：
      q_json_list: JSON 数据列表
      out_path: 输出文件路径
      idx_ranges: 包含多个 (start, end) 范围的列表，例如 [(21, 22), (74, 75), (43, 44)]
    """
    global QUESTION_TYPE, QUESTION_TYPE_LIST, all_tables_name_list, prev_tables_name_list, org_question

    # 如果 idx_ranges 为空，则默认处理全部数据
    if not idx_ranges:
        idx_ranges = [(0, len(q_json_list))]
        
    if REFRASH and Knowledge_file:
        external_answers = load_external_answers(Knowledge_file)
    total = len(q_json_list)
    # 以流式写入的方式生成 JSON 数组
    with open(out_path, 'w', encoding='utf-8') as out_f:
        out_f.write('[\n')
        for i in range(total):
            try:
                print("--> cleaned mem:", gc.collect())
                print("--> id:", i)
                item = q_json_list[i]
                start_time = time.time()
                
                # 判断当前索引是否在任一给定范围内
                process_this_item = any(start <= i < end for start, end in idx_ranges)
                
                if process_this_item:
                    # 处理需要调用 get_answer 生成答案的数据
                    questions_list = [(member["id"], member["question"]) for member in item["team"]]
                    answers_dict = {}
                    history_dict = {}
                    all_previous = ''
                    # 初始化相关全局变量
                    QUESTION_TYPE = ""
                    QUESTION_TYPE_LIST = []
                    question_num = 0
                    all_tables_name_list = []
                    prev_tables_name_list = []
                    
                    if REFRASH:
                        get_ext_answer_num = 1
                    # 遍历当前 item 内所有问题
                    for question_id, question_text in questions_list:
                        try:
                            answer = ""
                            # 更新历史表变量（超过 MAX_TABLES_LIST 后清空）
                            all_tables_name_list = prev_tables_name_list
                            if len(all_tables_name_list) > MAX_TABLES_LIST:
                                if DEBUG_VER == 3:
                                    print(f"----------------历史表过多，进行清空")
                                all_tables_name_list = []

                            question_num += 1
                            org_question = question_text
                            # 根据关键词更新 QUESTION_TYPE_LIST
                            if "港股" in question_text and "港股份" not in question_text and "港股" not in QUESTION_TYPE_LIST:
                                QUESTION_TYPE_LIST.append("港股")
                            if "美股" in question_text and "美股份" not in question_text and "美股" not in QUESTION_TYPE_LIST:
                                QUESTION_TYPE_LIST.append("美股")
                            if "A股" in question_text and "A股" not in QUESTION_TYPE_LIST:
                                QUESTION_TYPE_LIST.append("A股")

                            # 问题预处理
                            # if DEBUG_VER == 3:
                            print(f'---------->Org question_text:{question_text}')
                            question_text = preprocessing_question(question_text)
                            question_text = add_question_mark(question_text)

                            # 如果没有历史问答，则直接使用原问题；否则重写问题
                            if all_previous == '':
                                rewritten_question = question_text
                            else:
                                rewritten_question = question_rew(all_previous, question_text)

                            # 调用 get_answer 获取答案，并进行优化
                            if REFRASH:
                                matched_answer = find_kl_answer(question_num, history_dict, org_question, external_answers)
                                if matched_answer:
                                    if get_ext_answer_num == question_num:
                                        answer = matched_answer
                                        history_dict[org_question] = answer
                                        # 累加问答历史
                                        all_previous += f"第{question_num}个问题：" + question_text + f" 第{question_num}个的回答：" + answer + " \n"
                                        get_ext_answer_num += 1
                            if not answer or answer == "":
                                answer,org_answer = get_answer(rewritten_question, org_question)
                                # 累加问答历史
                                all_previous += f"第{question_num}个问题：" + question_text + f" 第{question_num}个的回答：" + answer + " \n"
                                answer = optimize_answer(rewritten_question, answer, org_answer)
                            # if DEBUG_VER == 3:
                            print(f"---------->answer:{answer}")
                            answer = answer.replace(";", "")
                            answers_dict[question_id] = answer
                            del answer
                            
                        except Exception as e_inner:
                            print(f"Error processing question id {question_id}: {e_inner}")
                            answers_dict[question_id] = "异常"

                    # 更新 item 中各 team 成员的 answer 字段
                    for member in item["team"]:
                        member["answer"] = answers_dict.get(member["id"], "无答案")
                else:
                    # 对于不在处理范围内的数据，直接置空 answer 字段
                    for member in item["team"]:
                        member["answer"] = ""

                updated_data = {"tid": item["tid"], "team": item["team"]}

                elapsed_time = time.time() - start_time
                print(f"Completed processing JSON index {i} in {elapsed_time:.2f} seconds")

                # 写入当前处理完的数据项。若不是最后一个，则尾部添加逗号。
                json_str = json.dumps(updated_data, ensure_ascii=False, indent=4)
                if i < total - 1:
                    out_f.write(json_str + ',\n')
                else:
                    out_f.write(json_str + '\n')
                out_f.flush()  # 每处理完一道题就 flush 到磁盘，降低内存占用

            except Exception as e_outer:
                print(f"Error processing JSON item index {i}: {e_outer}")
        out_f.write(']\n')
