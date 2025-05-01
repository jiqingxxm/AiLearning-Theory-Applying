from config import *
from zhipuai import ZhipuAI

client = ZhipuAI(api_key=api_key)

def create_chat_completion(messages, model=MODEL):
    """
    Create a chat completion using the provided messages and model.
    
    Parameters:
        messages (list): A list of message dictionaries to pass to the model.
        model (str): The model name to use.
    
    Returns:
        response (dict): The response from the chat completion endpoint.
    """
    response = client.chat.completions.create(
        model=model,
        stream=False,
        messages=messages,
        temperature=0,
        seed=42,
    )
    return response

def create_chat_completion_change_zhushi(fact_info, table_info, model=MODEL):
    few_shot_examples = """
    user:已查询获得事实: ('预处理程序通过表格：ConstantDB.SecuMain 查询到以下内容：\n [\n {\n  "InnerCode": 86721,\n  "CompanyCode": 98001,\n  "SecuCode": "601128"}\n] \n', ['ConstantDB.SecuMain'])
        获取的表结构如下:{'数据表名': 'AStockOperationsDB.LC_Staff', '数据表结构': '\n列名                   注释                             数据示例                                              \n\nID                   ID                             619397672060                                      \nCompanyCode          公司代码                           159                                               \nEndDate              日期                             2019-06-30 12:00:00.000]}
    assistant:{'数据表名': 'AStockOperationsDB.LC_Staff', '数据表结构': '\n列名                   注释                             数据示例                                              \n\nID                   ID                             619397672060                                      \nCompanyCode          公司代码                           98001                                               \nEndDate              日期                             2019-06-30 12:00:00.000]}
    """
    
    template = """
    你将根据“已查询获得事实”，更新“获取的表结构”里对应的数据示例列且对应的行的值，没有则不更新。只输出更新后的表结构，不需要额外解释和过程。
    
    以下是几个示例：{few_shot_examples}
    已查询获得事实: {fact_info}
    获取的表结构如下：{table_info}
    """
    def format_prompt(few_shot_examples, fact_info, table_info, template):
        return template.format(few_shot_examples=few_shot_examples,
                               fact_info=fact_info,
                               table_info=table_info)
    
    formatted_prompt = format_prompt(few_shot_examples, fact_info, table_info, template) 
    messages = [
        {'role': 'system', 'content': '你将根据“已查询获得事实”，更新“获取的表结构”里对应的数据示例列且对应的行的值，没有则不更新。只输出更新后的表结构，不需要额外解释和过程。'},
        {'role': 'user', 'content': formatted_prompt}
    ]
    response = client.chat.completions.create(
        model=model,
        stream=False,
        messages=messages,
        temperature=0,
        seed=42,
    )
    result = response.choices[0].message.content
    return result
