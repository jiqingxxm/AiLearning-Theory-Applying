# agent/sql/exec.py
from config import *
from agent.llm_client import create_chat_completion

def async_llm_chain_call(messages, model=MODEL, tree_node=ASYNC_LLM_TIME):
    """
    使用 multiprocessing.Pool 并行调用 API 接口
    """
    # tree node 1
    response_1 = create_chat_completion(messages,tempt=0)
    node_1 = response_1.choices[0].message.content
    # tree node 2
    node_2_prompt = f"""你根据历史信息以及基于上文信息生成的回复进行一步步思考判断，生成比回复更好的（且明显不同），能解决上文信息的回复。如果你不能给出更好且明显不同的回复，只需回复我<已经是最优回复>
    主要判断的几个方向：1.使用的内容（比如字段）是否存在上文信息中的，而不是凭空捏造。2.字段的用法是否符合上文信息的要求。
    这是上文信息：
    {messages}
    --------------
    回复:
    {node_1}
    --------------
    只需输出回复，不需要分析的过程。如果你不能给出更好的且明显不同回复，只需回复我<已经是最优回复>：
    """
    response_2 = create_chat_completion(messages,tempt=0)
    node_2 = response_2.choices[0].message.content
    if "最优回复" in node_2 or str(node_1) == str(node_2):
        if DEBUG_VER == 3:
            print(f"------------>node1已是最优回复:{node_1}")
        return response_1
    _sorted_results = {}
    _sorted_results[1] = node_1
    _sorted_results[2] = node_2
    idx_ = get_best(messages, _sorted_results)
    if DEBUG_VER == 3:
        print(f"------------>idx_:{idx_}")
    if idx_ == 1:
        return response_1
    elif idx_ == 2:
        return response_2
    else:
        return response_1
    return response_1

def get_best(messages, sorted_results):
    prompt = f"""根据上分信息，判断回复1和回复2哪个更优（是否满足上文信息的需求）。
    主要判断的几个方向：1.使用的内容（比如字段）是否存在上文信息中的，而不是凭空捏造。2.字段的用法是否符合上文信息的要求。
    --------------
    上文信息：
    {str(messages)}
    --------------
    回复1：
    {sorted_results[1]}
    --------------
    回复2:
    {sorted_results[2]}
    仅输出1还是2，不需要判断过程："""
    if DEBUG_VER == 3:
        print(f"------->prompt:{prompt}")
    messages = [{"role": "user", "content": prompt}]
    response = create_chat_completion(messages,tempt=0)
    answer = response.choices[0].message.content
    if "1" in answer:
        return 1
    elif "2" in answer:
        return 2
    else:
        print(f"---->get_best异常:{answer}")
        return 1