# agent/optimization.py
from agent.llm_client import create_chat_completion

def optimize_answer(question_txt, answer_txt, org_answer):
    """
    简化答案（降低长度），去掉非直接答案的补充说明。
    
    参数：
        question_txt (str): 问题语句。
        answer_txt (str): 生成的答案。
    
    返回：
        optimized_answer: 包含优化后的答案。
    """
    prompt = f"""
        请针对问题把当前回答精简到只有关键字段，去掉非直接答案的补充说明（不要遗漏答案，可参考JSON型回答）。用;分号分隔后且分隔后不再加空格，使整体长度短，不要遗漏答案。"
        几种常见需要优化的内容，比如：
        1.回答需要符合问题要求：如问题要求日期按照YYYY年MM月DD日格式回复，则需要改当前的YYYY-MM-DD格式。或者问题希望回答是或者否，而当前回答可能是True或者False，就需要把True改成是来回答。
        2.文本型的内容：去掉如：是问题中的文本但是跟需要回答的关键词无关的。但有的答案会是文本甚至文本间有空格和逗号，如果是答案文本段中的就不要简化它，保持原样。
        以下是几个示例：
        user: 问题：'亿帆医药在2020年的最大担保金额是多少？'，当前回答：'亿帆医药2020年最大担保金额 316000000.0
        assistant: 316000000.0
        user: 问题：'软通动力在2019年报酬总额和领取报酬的管理层人数是多少？'，当前回答：'2019年报酬总额 15802300.0, 2019年领取报酬的管理层人数 11'
        assistant: 15802300.0;11
        user: 问题：'芯片概念概念板块的英文名称是什么？'，当前回答：'ConceptEngName Chip Localization'
        assistant: Chip Localization
        user: 问题：'2020年，湖南华菱钢铁股份有限公司是否成功进行了资产重组？(回答是或者否)'，当前回答：'True'
        assistant: 是
        user: 问题：'博时基金公司成立于？请用YYYY年MM月DD日格式回复我'，当前回答：'1998-07-13'
        assistant: 1998年07月13日
        问题：'{question_txt}',JSON型回答：'{org_answer}'，当前回答：'{answer_txt}'
    """

    messages = [{"role": "user", "content": prompt}]
    response = create_chat_completion(messages)
    return response.choices[0].message.content

def find_kl_answer(question_no: int, history: dict, current_question: str, kb: list) -> str:
    """
    根据题号、历史问答（dict形式，键为问题，值为答案）和当前问题，
    在知识库中进行匹配，返回匹配到的答案字符串（仅返回答案）。
    
    参数:
        question_no: 当前题号（1 表示第一题，2、3、4... 表示后续题）
        history: 历史问答 dict（如果是第一题，此 dict 为空；否则存储前面题目的问题:答案）
        current_question: 当前需要匹配的问题文本
        kb: 知识库数据（例如 external_answers）
    
    返回:
        匹配成功则返回对应答案字符串，否则返回空字符串
    """
    for record in kb:
        team = record.get('team', [])
        # 检查当前记录中题目的数量是否足够匹配
        if len(team) < question_no:
            continue

        if question_no == 1:
            # 第一题：直接匹配 team[0] 的问题文本
            if team[0]['question'] == current_question:
                return team[0]['answer']
        else:
            # 后续题：先判断历史问答数量是否与题号一致（题号-1）
            history_items = list(history.items())  # 保证顺序（Python3.7+）
            if len(history_items) != question_no - 1:
                continue

            # 比较历史中的每一道题的问答
            match_history = True
            for idx, (hist_q, hist_a) in enumerate(history_items):
                if team[idx]['question'] != hist_q or team[idx]['answer'] != hist_a:
                    match_history = False
                    break
            if not match_history:
                continue

            # 历史全部匹配成功后，检查当前问题是否与 team 中对应的问题一致
            if team[question_no - 1]['question'] == current_question:
                return team[question_no - 1]['answer']
    # 未匹配到则返回空字符串
    return ""
