# coding=utf-8

import json
import time
import sys
import jsonlines
import numpy as np
import pandas as pd
import jieba
import re
import requests
import os
from zhipuai import ZhipuAI
from collections import Counter
import gc
pd.options.mode.chained_assignment = None  # 禁用链式赋值警告


# 运行所需填写内容
Access_Token = ''  # 填写你的队伍token，以便连接数据库查结果
client = ZhipuAI(api_key='')  # 填写你的GLM模型调用token 

# 全局参数
DEBUG = False
VER = 1
DEBUG_VER = 1 # 1,3 DEBUG级别
TEAM_NAME = 'ShallowRest'
REFRASH = True  # 开启答案库
FRASH_DATA_DESCRIBE = True  # 刷新获取的表结构里的示例值，替换成真实的
DEEP_COLUMNS_RESEARCH = True  # 启动深度检索
USE_LOCAL_VECTORS = True  # 使用本地向量
MAX_TABLES_LIST = 2  # 最大历史表数量
START_DEEP_THRESHOLD = 0.5  # 启动深度思考的相似度阈值
MAX_TOP_COLUMNS = 25  # 只获取表前25个字段
SQL_PROMPT_THRESHOLD = 0.3  # 知识库SQL的相似度阈值
ASYNC_LLM_TIME = 2  # 决策树节点
MAX_ROUNDS = 20  # SQL调用最大循环次数

QUESTION_TYPE = ''
QUESTION_TYPE_LIST = []
prev_tables_name_list = []
all_tables_name_list = []
table_maps = ''
table_maps_LL = ''
MODEL = "glm-4-plus"


def dump_2_report_answer(info, path):
    with open(path, 'w') as output_json_file:
        json.dump(info, output_json_file, ensure_ascii=False, indent=4)

def dump_jsonl(info, path):
    with jsonlines.open(path, "w") as json_file:
        for obj in info:
            json_file.write(obj)

def read_jsonl(path):
    content = []
    with jsonlines.open(path, "r") as json_file:
        for obj in json_file.iter(type=dict, skip_invalid=True):
            content.append(obj)
    return content

# 导入数据
Knowledge_file = './devlop_home/2024-fic-lmc-data-0217/question_all_kl_v12_v3.json'
df_all_embeddings_path = './devlop_home/2024-fic-lmc-data-0217/df_all_embeddings.json'
df_hk_embeddings_path = './devlop_home/2024-fic-lmc-data-0217/df_hk_embeddings.json'
df_us_embeddings_path = './devlop_home/2024-fic-lmc-data-0217/df_us_embeddings.json'
df_cn_embeddings_path = './devlop_home/2024-fic-lmc-data-0217/df_cn_embeddings.json'
knowledge_embeddings_file = './devlop_home/2024-fic-lmc-data-0217/knowledge_questions_embeddings_v12.json'

df_all1 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_v5.xlsx', sheet_name='库表关系')
df_all2 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_v5.xlsx', sheet_name='表字段信息')
file_path_all = '/app/devlop_home/2024-fic-lmc-data-0217/all_tables_schema_v5.txt'

df_all1['库表名英文'] = df_all1['库名英文'] + '.' + df_all1['表英文']
df_all1['库表名中文'] = df_all1['库名中文'] + '.' + df_all1['表中文']

database_name_all = list(df_all1['库名中文'])
table_name_all = list(df_all1['表中文'])
table_name_en_all = list(df_all1['表英文'])
database_table_ch_all = list(df_all1['库表名中文'])
database_table_en_all = list(df_all1['库表名英文'])
database_table_en_zs_all = {'库表名': database_table_en_all, '对应中文注释说明': table_name_all}
database_table_map_all = df_all1.set_index('库表名中文')['库表名英文'].to_dict()

database_L_all = []
database_L_zh_all = []
for i in table_name_en_all:
    df3 = df_all2[df_all2['table_name'] == i]
    name = df_all1[df_all1['表英文'] == i]['库表名英文'].iloc[0]
    column_name = list(df3['column_name'])
    column_name_zh = list(df3['column_description'])
    column_name_2 = list(df3['注释'].dropna())

    dict_1 = {'数据表名': name, '列名': column_name, '注释': column_name_2}
    dict_2 = {'数据表名': name, '列名': column_name, '列名中文描述': column_name_zh, '注释': column_name_2}
    database_L_all.append(dict_1)
    database_L_zh_all.append(dict_2)

with open(file_path_all, 'r', encoding='utf-8') as file:
    content = file.read()
input_text_all = content
df_all = df_all2.merge(df_all1, left_on="table_name", right_on="表英文")
df_all["table_describe"] = df_all["库名中文"]+"，"+df_all["表中文"]+"，"+df_all["表描述"]


content_ALL = """我有如下数据库表{'库表名': ['AStockBasicInfoDB.LC_StockArchives',
  'AStockBasicInfoDB.LC_NameChange',
  'AStockBasicInfoDB.LC_Business',
  'AStockIndustryDB.LC_ExgIndustry',
  'AStockIndustryDB.LC_ExgIndChange',
  'AStockIndustryDB.LC_IndustryValuation',
  'AStockIndustryDB.LC_IndFinIndicators',
  'AStockIndustryDB.LC_COConcept',
  'AStockIndustryDB.LC_ConceptList',
  'AStockOperationsDB.LC_SuppCustDetail',
  'AStockShareholderDB.LC_SHTypeClassifi',
  'AStockShareholderDB.LC_MainSHListNew',
  'AStockShareholderDB.LC_SHNumber',
  'AStockShareholderDB.LC_Mshareholder',
  'AStockShareholderDB.LC_ActualController',
  'AStockShareholderDB.LC_ShareStru',
  'AStockShareholderDB.LC_StockHoldingSt',
  'AStockShareholderDB.LC_ShareTransfer',
  'AStockShareholderDB.LC_ShareFP',
  'AStockShareholderDB.LC_ShareFPSta',
  'AStockShareholderDB.LC_Buyback',
  'AStockShareholderDB.LC_BuybackAttach',
  'AStockShareholderDB.LC_LegalDistribution',
  'AStockShareholderDB.LC_NationalStockHoldSt',
  'AStockShareholderDB.CS_ForeignHoldingSt',
  'AStockFinanceDB.LC_AShareSeasonedNewIssue',
  'AStockFinanceDB.LC_ASharePlacement',
  'AStockFinanceDB.LC_Dividend',
  'AStockFinanceDB.LC_CapitalInvest',
  'AStockMarketQuotesDB.CS_StockCapFlowIndex',
  'AStockMarketQuotesDB.CS_TurnoverVolTecIndex',
  'AStockMarketQuotesDB.CS_StockPatterns',
  'AStockMarketQuotesDB.QT_DailyQuote',
  'AStockMarketQuotesDB.QT_StockPerformance',
  'AStockMarketQuotesDB.LC_SuspendResumption',
  'AStockFinanceDB.LC_BalanceSheetAll',
  'AStockFinanceDB.LC_IncomeStatementAll',
  'AStockFinanceDB.LC_CashFlowStatementAll',
  'AStockFinanceDB.LC_IntAssetsDetail',
  'AStockFinanceDB.LC_MainOperIncome',
  'AStockFinanceDB.LC_OperatingStatus',
  'AStockFinanceDB.LC_AuditOpinion',
  'AStockOperationsDB.LC_Staff',
  'AStockOperationsDB.LC_RewardStat',
  'AStockEventsDB.LC_Warrant',
  'AStockEventsDB.LC_Credit',
  'AStockEventsDB.LC_SuitArbitration',
  'AStockEventsDB.LC_EntrustInv',
  'AStockEventsDB.LC_Regroup',
  'AStockEventsDB.LC_MajorContract',
  'AStockEventsDB.LC_InvestorRa',
  'AStockEventsDB.LC_InvestorDetail',
  'AStockShareholderDB.LC_ESOP',
  'AStockShareholderDB.LC_ESOPSummary',
  'AStockShareholderDB.LC_TransferPlan',
  'AStockShareholderDB.LC_SMAttendInfo',
  'HKStockDB.HK_EmployeeChange',
  'HKStockDB.HK_StockArchives',
  'HKStockDB.CS_HKStockPerformance',
  'USStockDB.US_CompanyInfo',
  'USStockDB.US_DailyQuote',
  'PublicFundDB.MF_FundArchives',
  'PublicFundDB.MF_FundProdName',
  'PublicFundDB.MF_InvestAdvisorOutline',
  'PublicFundDB.MF_Dividend',
  'CreditDB.LC_ViolatiParty',
  'IndexDB.LC_IndexBasicInfo',
  'IndexDB.LC_IndexComponent',
  'InstitutionDB.LC_InstiArchive',
  'ConstantDB.SecuMain',
  'ConstantDB.HK_SecuMain',
  'ConstantDB.CT_SystemConst',
  'ConstantDB.QT_TradingDayNew',
  'ConstantDB.LC_AreaCode',
  'InstitutionDB.PS_EventStru',
  'ConstantDB.US_SecuMain',
  'InstitutionDB.PS_NewsSecurity'],
 '对应中文注释说明': ['公司概况',
  '公司名称更改状况',
  '公司经营范围与行业变更',
  '公司行业划分表',
  '公司行业变更表',
  '行业估值指标',
  '行业财务指标表',
  '概念所属公司表',
  '概念板块常量表',
  '公司供应商与客户',
  '股东类型分类表',
  '股东名单(新)',
  '股东户数',
  '大股东介绍',
  '公司实际控制人',
  '公司股本结构变动',
  '股东持股统计',
  '股东股权变动',
  '股东股权冻结和质押',
  '股东股权冻结和质押统计',
  '股份回购',
  '股份回购关联表',
  '法人配售与战略投资者',
  'A股国家队持股统计',
  '外资持股统计',
  'A股增发',
  'A股配股',
  '公司分红',
  '资金投向说明',
  '境内股票交易资金流向指标',
  '境内股票成交量技术指标',
  '股票技术形态表',
  '日行情表',
  '股票行情表现(新)',
  '停牌复牌表',
  '资产负债表_新会计准则',
  '利润分配表_新会计准则',
  '现金流量表_新会计准则',
  '公司研发投入与产出',
  '公司主营业务构成',
  '公司经营情况述评',
  '公司历年审计意见',
  '公司职工构成',
  '公司管理层报酬统计',
  '公司担保明细',
  '公司借贷明细',
  '公司诉讼仲裁明细',
  '重大事项委托理财',
  '公司资产重组明细',
  '公司重大经营合同明细',
  '投资者关系活动',
  '投资者关系活动调研明细',
  '员工持股计划',
  '员工持股计划概况',
  '股东增减持计划表',
  '股东大会出席信息',
  '港股公司员工数量变动表',
  '港股公司概况',
  '港股行情表现',
  '美股公司概况',
  '美股日行情',
  '公募基金概况',
  '公募基金产品名称',
  '公募基金管理人概况',
  '公募基金分红',
  '违规当事人处罚',
  '指数基本情况',
  '指数成份',
  '机构基本资料',
  '证券主表,包含字段InnerCode,CompanyCode,SecuCode,ChiName,ChiNameAbbr 代表中文名称缩写,EngName,EngNameAbbr,SecuAbbr 代表 证券简称,ListedDate',
  '港股证券主表，包含字段InnerCode,CompanyCode,SecuCode,ChiName,EngName,EngNameAbbr,SecuAbbr 代表 证券简称,ListedDate',
  '系统常量表',
  '交易日表(新)',
  '国家城市代码表',
  '事件体系指引表',
  '美股证券主表',
  '证券舆情表']}
已查询获得事实：<<fact_1>>
我想回答问题
"<<question>>"

如果已查询获得事实可以直接总结答案，需要是明确的答案数据不是需要查询数据库表，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我。
如果不能直接总结答案，需要查询的数据库表,请从上面数据库表中筛选出还需要哪些数据库表，记得提示我：<需要查询的数据库表>,只返回需要数据列表,不要回答其他内容。"""

# ### A股库表

"""
A股去掉: HK_EmployeeChange,HK_StockArchives,CS_HKStockPerformance,US_CompanyInfo,
US_DailyQuote,HK_SecuMain,US_SecuMain
"""

df_cn1 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_cn_v5.xlsx', sheet_name='库表关系')
df_cn2 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_cn_v5.xlsx', sheet_name='表字段信息')
file_path_cn = '/app/devlop_home/2024-fic-lmc-data-0217/all_tables_schema_cn_v5.txt'

df_cn1['库表名英文'] = df_cn1['库名英文'] + '.' + df_cn1['表英文']
df_cn1['库表名中文'] = df_cn1['库名中文'] + '.' + df_cn1['表中文']

database_name_cn = list(df_cn1['库名中文'])
table_name_cn = list(df_cn1['表中文'])
table_name_en_cn = list(df_cn1['表英文'])
database_table_ch_cn = list(df_cn1['库表名中文'])
database_table_en_cn = list(df_cn1['库表名英文'])
database_table_en_zs_cn = {'库表名': database_table_en_cn, '对应中文注释说明': table_name_cn}
database_table_map_cn = df_cn1.set_index('库表名中文')['库表名英文'].to_dict()

database_L_cn = []
database_L_zh_cn = []
for i in table_name_en_cn:
    df3 = df_cn2[df_cn2['table_name'] == i]
    name = df_cn1[df_cn1['表英文'] == i]['库表名英文'].iloc[0]
    column_name = list(df3['column_name'])
    column_name_zh = list(df3['column_description'])
    column_name_2 = list(df3['注释'].dropna())

    dict_1 = {'数据表名': name, '列名': column_name, '注释': column_name_2}
    dict_2 = {'数据表名': name, '列名': column_name, '列名中文描述': column_name_zh, '注释': column_name_2}
    database_L_cn.append(dict_1)
    database_L_zh_cn.append(dict_2)

with open(file_path_cn, 'r', encoding='utf-8') as file:
    content = file.read()
input_text_cn = content
df_cn = df_cn2.merge(df_cn1, left_on="table_name", right_on="表英文")
df_cn["table_describe"] = df_cn["库名中文"]+"，"+df_cn["表中文"]+"，"+df_cn["表描述"]

content_CN = """我有如下数据库表{'库表名': ['AStockBasicInfoDB.LC_StockArchives',
  'AStockBasicInfoDB.LC_NameChange',
  'AStockBasicInfoDB.LC_Business',
  'AStockIndustryDB.LC_ExgIndustry',
  'AStockIndustryDB.LC_ExgIndChange',
  'AStockIndustryDB.LC_IndustryValuation',
  'AStockIndustryDB.LC_IndFinIndicators',
  'AStockIndustryDB.LC_COConcept',
  'AStockIndustryDB.LC_ConceptList',
  'AStockOperationsDB.LC_SuppCustDetail',
  'AStockShareholderDB.LC_SHTypeClassifi',
  'AStockShareholderDB.LC_MainSHListNew',
  'AStockShareholderDB.LC_SHNumber',
  'AStockShareholderDB.LC_Mshareholder',
  'AStockShareholderDB.LC_ActualController',
  'AStockShareholderDB.LC_ShareStru',
  'AStockShareholderDB.LC_StockHoldingSt',
  'AStockShareholderDB.LC_ShareTransfer',
  'AStockShareholderDB.LC_ShareFP',
  'AStockShareholderDB.LC_ShareFPSta',
  'AStockShareholderDB.LC_Buyback',
  'AStockShareholderDB.LC_BuybackAttach',
  'AStockShareholderDB.LC_LegalDistribution',
  'AStockShareholderDB.LC_NationalStockHoldSt',
  'AStockShareholderDB.CS_ForeignHoldingSt',
  'AStockFinanceDB.LC_AShareSeasonedNewIssue',
  'AStockFinanceDB.LC_ASharePlacement',
  'AStockFinanceDB.LC_Dividend',
  'AStockFinanceDB.LC_CapitalInvest',
  'AStockMarketQuotesDB.CS_StockCapFlowIndex',
  'AStockMarketQuotesDB.CS_TurnoverVolTecIndex',
  'AStockMarketQuotesDB.CS_StockPatterns',
  'AStockMarketQuotesDB.QT_DailyQuote',
  'AStockMarketQuotesDB.QT_StockPerformance',
  'AStockMarketQuotesDB.LC_SuspendResumption',
  'AStockFinanceDB.LC_BalanceSheetAll',
  'AStockFinanceDB.LC_IncomeStatementAll',
  'AStockFinanceDB.LC_CashFlowStatementAll',
  'AStockFinanceDB.LC_IntAssetsDetail',
  'AStockFinanceDB.LC_MainOperIncome',
  'AStockFinanceDB.LC_OperatingStatus',
  'AStockFinanceDB.LC_AuditOpinion',
  'AStockOperationsDB.LC_Staff',
  'AStockOperationsDB.LC_RewardStat',
  'AStockEventsDB.LC_Warrant',
  'AStockEventsDB.LC_Credit',
  'AStockEventsDB.LC_SuitArbitration',
  'AStockEventsDB.LC_EntrustInv',
  'AStockEventsDB.LC_Regroup',
  'AStockEventsDB.LC_MajorContract',
  'AStockEventsDB.LC_InvestorRa',
  'AStockEventsDB.LC_InvestorDetail',
  'AStockShareholderDB.LC_ESOP',
  'AStockShareholderDB.LC_ESOPSummary',
  'AStockShareholderDB.LC_TransferPlan',
  'AStockShareholderDB.LC_SMAttendInfo',
  'PublicFundDB.MF_FundArchives',
  'PublicFundDB.MF_FundProdName',
  'PublicFundDB.MF_InvestAdvisorOutline',
  'PublicFundDB.MF_Dividend',
  'CreditDB.LC_ViolatiParty',
  'IndexDB.LC_IndexBasicInfo',
  'IndexDB.LC_IndexComponent',
  'InstitutionDB.LC_InstiArchive',
  'ConstantDB.SecuMain',
  'ConstantDB.CT_SystemConst',
  'ConstantDB.QT_TradingDayNew',
  'ConstantDB.LC_AreaCode',
  'InstitutionDB.PS_EventStru',
  'InstitutionDB.PS_NewsSecurity'],
 '对应中文注释说明': ['公司概况',
  '公司名称更改状况',
  '公司经营范围与行业变更',
  '公司行业划分表',
  '公司行业变更表',
  '行业估值指标',
  '行业财务指标表',
  '概念所属公司表',
  '概念板块常量表',
  '公司供应商与客户',
  '股东类型分类表',
  '股东名单(新)',
  '股东户数',
  '大股东介绍',
  '公司实际控制人',
  '公司股本结构变动',
  '股东持股统计',
  '股东股权变动',
  '股东股权冻结和质押',
  '股东股权冻结和质押统计',
  '股份回购',
  '股份回购关联表',
  '法人配售与战略投资者',
  'A股国家队持股统计',
  '外资持股统计',
  'A股增发',
  'A股配股',
  '公司分红',
  '资金投向说明',
  '境内股票交易资金流向指标',
  '境内股票成交量技术指标',
  '股票技术形态表',
  '日行情表',
  '股票行情表现(新)',
  '停牌复牌表',
  '资产负债表_新会计准则',
  '利润分配表_新会计准则',
  '现金流量表_新会计准则',
  '公司研发投入与产出',
  '公司主营业务构成',
  '公司经营情况述评',
  '公司历年审计意见',
  '公司职工构成',
  '公司管理层报酬统计',
  '公司担保明细',
  '公司借贷明细',
  '公司诉讼仲裁明细',
  '重大事项委托理财',
  '公司资产重组明细',
  '公司重大经营合同明细',
  '投资者关系活动',
  '投资者关系活动调研明细',
  '员工持股计划',
  '员工持股计划概况',
  '股东增减持计划表',
  '股东大会出席信息',
  '公募基金概况',
  '公募基金产品名称',
  '公募基金管理人概况',
  '公募基金分红',
  '违规当事人处罚',
  '指数基本情况',
  '指数成份',
  '机构基本资料',
  '证券主表,包含字段InnerCode,CompanyCode,SecuCode,ChiName,ChiNameAbbr 代表中文名称缩写,EngName,EngNameAbbr,SecuAbbr 代表 证券简称,ListedDate',
  '系统常量表',
  '交易日表(新)',
  '国家城市代码表',
  '事件体系指引表',
  '证券舆情表']}
已查询获得事实：<<fact_1>>
我想回答问题
"<<question>>"

如果已查询获得事实可以直接总结答案，需要是明确的答案数据不是需要查询数据库表，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我（注意答案不能是XX这种占位符）。
如果不能直接总结答案，需要查询的数据库表,请从上面数据库表中筛选出还需要哪些数据库表，记得提示我：<需要查询的数据库表>,只返回需要数据列表,不要回答其他内容。"""

# %% [markdown] {"jupyter":{"outputs_hidden":false}}
# ### 港股库表

# %% [code] {"jupyter":{"source_hidden":true},"execution":{"iopub.status.busy":"2025-02-28T14:23:49.412283Z","iopub.execute_input":"2025-02-28T14:23:49.412590Z","iopub.status.idle":"2025-02-28T14:23:49.605180Z","shell.execute_reply.started":"2025-02-28T14:23:49.412564Z","shell.execute_reply":"2025-02-28T14:23:49.603881Z"}}
"""
港股去掉: LC_ExgIndChange,LC_IndFinIndicators,LC_COConcept,LC_ConceptList,
LC_SuppCustDetail,LC_ShareTransfer,CS_ForeignHoldingSt,LC_AShareSeasonedNewIssue,
LC_ASharePlacement,CS_StockCapFlowIndex,CS_TurnoverVolTecIndex,CS_StockPatterns,QT_DailyQuote,
QT_StockPerformance,LC_SuspendResumption,SecuMain,
US_CompanyInfo,US_DailyQuote,US_SecuMain
"""
df_hk1 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_hk_v5.xlsx', sheet_name='库表关系')
df_hk2 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_hk_v5.xlsx', sheet_name='表字段信息')
file_path_hk = '/app/devlop_home/2024-fic-lmc-data-0217/all_tables_schema_hk_v5.txt'

df_hk1['库表名英文'] = df_hk1['库名英文'] + '.' + df_hk1['表英文']
df_hk1['库表名中文'] = df_hk1['库名中文'] + '.' + df_hk1['表中文']

database_name_hk = list(df_hk1['库名中文'])
table_name_hk = list(df_hk1['表中文'])
table_name_en_hk = list(df_hk1['表英文'])
database_table_ch_hk = list(df_hk1['库表名中文'])
database_table_en_hk = list(df_hk1['库表名英文'])
database_table_en_zs_hk = {'库表名': database_table_en_hk, '对应中文注释说明': table_name_hk}
database_table_map_hk = df_hk1.set_index('库表名中文')['库表名英文'].to_dict()

database_L_hk = []
database_L_zh_hk = []
for i in table_name_en_hk:
    df3 = df_hk2[df_hk2['table_name'] == i]
    name = df_hk1[df_hk1['表英文'] == i]['库表名英文'].iloc[0]
    column_name = list(df3['column_name'])
    column_name_zh = list(df3['column_description'])
    column_name_2 = list(df3['注释'].dropna())

    dict_1 = {'数据表名': name, '列名': column_name, '注释': column_name_2}
    dict_2 = {'数据表名': name, '列名': column_name, '列名中文描述': column_name_zh, '注释': column_name_2}
    database_L_hk.append(dict_1)
    database_L_zh_hk.append(dict_2)

with open(file_path_hk, 'r', encoding='utf-8') as file:
    content = file.read()
input_text_hk = content
df_hk = df_hk2.merge(df_hk1, left_on="table_name", right_on="表英文")
df_hk["table_describe"] = df_hk["库名中文"]+"，"+df_hk["表中文"]+"，"+df_hk["表描述"]

content_HK = """我有如下数据库表{'库表名': ['HKStockDB.HK_EmployeeChange',
  'HKStockDB.HK_StockArchives',
  'HKStockDB.CS_HKStockPerformance',
  'PublicFundDB.MF_FundArchives',
  'PublicFundDB.MF_FundProdName',
  'PublicFundDB.MF_InvestAdvisorOutline',
  'PublicFundDB.MF_Dividend',
  'CreditDB.LC_ViolatiParty',
  'IndexDB.LC_IndexBasicInfo',
  'IndexDB.LC_IndexComponent',
  'InstitutionDB.LC_InstiArchive',
  'ConstantDB.HK_SecuMain',
  'ConstantDB.CT_SystemConst',
  'ConstantDB.LC_AreaCode',
  'InstitutionDB.PS_EventStru',
  'InstitutionDB.PS_NewsSecurity'],
 '对应中文注释说明': ['港股公司员工数量变动表',
  '港股公司概况',
  '港股行情表现',
  '公募基金概况',
  '公募基金产品名称',
  '公募基金管理人概况',
  '公募基金分红',
  '违规当事人处罚',
  '指数基本情况',
  '指数成份',
  '机构基本资料',
  '港股证券主表，包含字段InnerCode,CompanyCode,SecuCode,ChiName,EngName,EngNameAbbr,SecuAbbr 代表 证券简称,ListedDate',
  '系统常量表',
  '国家城市代码表',
  '事件体系指引表',
  '证券舆情表']}
已查询获得事实：<<fact_1>>
我想回答问题
"<<question>>"

如果已查询获得事实可以直接总结答案，需要是明确的答案数据不是需要查询数据库表，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我。
如果不能直接总结答案，需要查询的数据库表,请从上面数据库表中筛选出还需要哪些数据库表，记得提示我：<需要查询的数据库表>,只返回需要数据列表,不要回答其他内容。"""

# ### 美股库表

"""
美股去掉: LC_ExgIndChange,LC_IndFinIndicators,LC_COConcept,LC_ConceptList,
LC_SuppCustDetail,LC_ShareTransfer,CS_ForeignHoldingSt,LC_AShareSeasonedNewIssue,
LC_ASharePlacement,CS_StockCapFlowIndex,CS_TurnoverVolTecIndex,CS_StockPatterns,QT_DailyQuote,
QT_StockPerformance,LC_SuspendResumption,SecuMain,
HK_SecuMain,HK_EmployeeChange,HK_StockArchives,CS_HKStockPerformance
"""
df_us1 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_us_v5.xlsx', sheet_name='库表关系')
df_us2 = pd.read_excel('/app/devlop_home/2024-fic-lmc-data-0217/data_dictionary_us_v5.xlsx', sheet_name='表字段信息')
file_path_us = '/app/devlop_home/2024-fic-lmc-data-0217/all_tables_schema_us_v5.txt'

df_us1['库表名英文'] = df_us1['库名英文'] + '.' + df_us1['表英文']
df_us1['库表名中文'] = df_us1['库名中文'] + '.' + df_us1['表中文']

database_name_us = list(df_us1['库名中文'])
table_name_us = list(df_us1['表中文'])
table_name_en_us = list(df_us1['表英文'])
database_table_ch_us = list(df_us1['库表名中文'])
database_table_en_us = list(df_us1['库表名英文'])
database_table_en_zs_us = {'库表名': database_table_en_us, '对应中文注释说明': table_name_us}
database_table_map_us = df_us1.set_index('库表名中文')['库表名英文'].to_dict()

database_L_us = []
database_L_zh_us = []
for i in table_name_en_us:
    df3 = df_us2[df_us2['table_name'] == i]
    name = df_us1[df_us1['表英文'] == i]['库表名英文'].iloc[0]
    column_name = list(df3['column_name'])
    column_name_zh = list(df3['column_description'])
    column_name_2 = list(df3['注释'].dropna())

    dict_1 = {'数据表名': name, '列名': column_name, '注释': column_name_2}
    dict_2 = {'数据表名': name, '列名': column_name, '列名中文描述': column_name_zh, '注释': column_name_2}
    database_L_us.append(dict_1)
    database_L_zh_us.append(dict_2)

with open(file_path_us, 'r', encoding='utf-8') as file:
    content = file.read()
input_text_us = content
df_us = df_us2.merge(df_us1, left_on="table_name", right_on="表英文")
df_us["table_describe"] = df_us["库名中文"]+"，"+df_us["表中文"]+"，"+df_us["表描述"]

content_US = """我有如下数据库表{'库表名': ['USStockDB.US_CompanyInfo',
  'USStockDB.US_DailyQuote',
  'PublicFundDB.MF_FundArchives',
  'PublicFundDB.MF_FundProdName',
  'PublicFundDB.MF_InvestAdvisorOutline',
  'PublicFundDB.MF_Dividend',
  'CreditDB.LC_ViolatiParty',
  'IndexDB.LC_IndexBasicInfo',
  'IndexDB.LC_IndexComponent',
  'InstitutionDB.LC_InstiArchive',
  'ConstantDB.CT_SystemConst',
  'ConstantDB.LC_AreaCode',
  'InstitutionDB.PS_EventStru',
  'ConstantDB.US_SecuMain',
  'InstitutionDB.PS_NewsSecurity'],
 '对应中文注释说明': ['美股公司概况',
  '美股日行情',
  '公募基金概况',
  '公募基金产品名称',
  '公募基金管理人概况',
  '公募基金分红',
  '违规当事人处罚',
  '指数基本情况',
  '指数成份',
  '机构基本资料',
  '系统常量表',
  '国家城市代码表',
  '事件体系指引表',
  '美股证券主表'，包含字段InnerCode,CompanyCode,SecuCode,ChiName,EngName,EngNameAbbr,SecuAbbr 代表 证券简称,ListedDate',
  '证券舆情表']}
已查询获得事实：<<fact_1>>
我想回答问题
"<<question>>"

如果已查询获得事实可以直接总结答案，需要是明确的答案数据不是需要查询数据库表，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我。
如果不能直接总结答案，需要查询的数据库表,请从上面数据库表中筛选出还需要哪些数据库表，记得提示我：<需要查询的数据库表>,只返回需要数据列表,不要回答其他内容。"""

def save_embeddings(text_list, save_path, batch_size=64):
    """
    保存文本的向量到本地文件，分批请求最多 64 个文本。
    """
    all_embeddings = []
    
    # 按批次处理文本
    for i in range(0, len(text_list), batch_size):
        chunk = text_list[i:i+batch_size]
        
        # 批量请求文本的嵌入向量
        response = client.embeddings.create(
            model="embedding-3",
            input=chunk
        )
        
        # 提取每个文本的 embedding
        embeddings = [item.embedding for item in response.data]
        all_embeddings.extend(embeddings)  # 累加所有的 embeddings
    
    # 保存到文件
    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(all_embeddings, f)
    
    print(f"Embeddings saved to {save_path}")

if USE_LOCAL_VECTORS:
    # 检查并保存 df_tmp 中的文本向量
    if not os.path.exists(df_all_embeddings_path):
        df_unique = df_all[['库表名英文', 'table_describe']].drop_duplicates()
        candidate_ids_deep = df_unique['库表名英文'].tolist()
        candidate_texts_deep = df_unique['table_describe'].tolist()
        save_embeddings(candidate_texts_deep, df_all_embeddings_path)
    else:
        print("df_all_embeddings.json already exists, skipping save.")
    
    if not os.path.exists(df_hk_embeddings_path):
        df_unique = df_hk[['库表名英文', 'table_describe']].drop_duplicates()
        candidate_ids_deep = df_unique['库表名英文'].tolist()
        candidate_texts_deep = df_unique['table_describe'].tolist()
        save_embeddings(candidate_texts_deep, df_hk_embeddings_path)
    else:
        print("df_hk_embeddings.json already exists, skipping save.")
    
    if not os.path.exists(df_us_embeddings_path):
        df_unique = df_us[['库表名英文', 'table_describe']].drop_duplicates()
        candidate_ids_deep = df_unique['库表名英文'].tolist()
        candidate_texts_deep = df_unique['table_describe'].tolist()
        save_embeddings(candidate_texts_deep, df_us_embeddings_path)
    else:
        print("df_us_embeddings.json already exists, skipping save.")
    
    if not os.path.exists(df_cn_embeddings_path):
        df_unique = df_cn[['库表名英文', 'table_describe']].drop_duplicates()
        candidate_ids_deep = df_unique['库表名英文'].tolist()
        candidate_texts_deep = df_unique['table_describe'].tolist()
        save_embeddings(candidate_texts_deep, df_cn_embeddings_path)
        del df_unique,candidate_ids_deep,candidate_texts_deep
    else:
        print("df_cn_embeddings.json already exists, skipping save.")
        

if USE_LOCAL_VECTORS:
    # 检查并保存 Knowledge_file 中的 knowledge_question 向量
    if not os.path.exists(knowledge_embeddings_file):
        with open(Knowledge_file, 'r', encoding='utf-8') as f:
            knowledge_data = json.load(f)
        
        candidate_questions = []
        for entry in knowledge_data:
            for item in entry.get("team", []):
                knowledge_question = item.get("question", "")
                candidate_questions.append(knowledge_question)
        
        save_embeddings(candidate_questions, knowledge_embeddings_file)
        del knowledge_data,candidate_questions,knowledge_question,candidate_questions
    else:
        print(f"{knowledge_embeddings_file} already exists, skipping save.")

# 读取上面生成items_another.json文件
with open('/app/devlop_home/2024-fic-lmc-data-0217/items_another.json', 'r', encoding='utf-8') as f:
    items_another = json.load(f)

# # Clean

del df_all1,df_cn1,df_hk1,df_us1,content,database_name_all,table_name_all,table_name_en_all,database_table_ch_all,database_table_en_zs_all,database_table_map_all,dict_1,dict_2
del df3,column_name,column_name_zh,column_name_2
del database_name_cn,table_name_cn,table_name_en_cn,database_table_ch_cn,database_table_en_zs_cn,database_table_map_cn
del database_name_hk,table_name_hk,table_name_en_hk,database_table_ch_hk,database_table_en_zs_hk,database_table_map_hk
del database_name_us,table_name_us,table_name_en_us,database_table_ch_us,database_table_en_zs_us,database_table_map_us
print(gc.collect())

# ## 工具函数
# 
# 这里提到了本项目会用到的所有工具函数，为完成任务所设置。具体功能可以查看代码中关于解释的部分。

# %% [code] {"ExecuteTime":{"end_time":"2024-12-16T17:51:00.860313Z","start_time":"2024-12-16T17:51:00.845378Z"},"execution":{"iopub.status.busy":"2025-03-07T09:14:36.365183Z","iopub.execute_input":"2025-03-07T09:14:36.365587Z","iopub.status.idle":"2025-03-07T09:14:36.403502Z","shell.execute_reply.started":"2025-03-07T09:14:36.365557Z","shell.execute_reply":"2025-03-07T09:14:36.402270Z"},"jupyter":{"source_hidden":true}}
def load_external_answers(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        ext_data = json.load(f)
    return ext_data


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


def find_json(text):
    """
    Attempt to extract and parse a JSON object from the provided text.
    The function tries up to three attempts using two patterns:
      1. A Markdown code block with ```json ... ```
      2. A more general JSON-like pattern using { and }

    If successful, returns the parsed JSON data.
    If parsing fails after all attempts, returns the original text.
    
    Parameters:
        text (str): The input text from which to extract JSON.
    
    Returns:
        dict or str: Parsed JSON dictionary if successful, else the original text.
    """
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        json_pattern = r"```json\n(.*?)\n```"
        match = re.search(json_pattern, text, re.DOTALL)
        if not match:
            json_pattern2 = r"({.*?})"
            match = re.search(json_pattern2, text, re.DOTALL)

        if match:
            json_string = match.group(1) if match.lastindex == 1 else match.group(0)
            # Remove Markdown formatting if present
            json_string = json_string.replace("```json\n", "").replace("\n```", "")
            try:
                data = json.loads(json_string)
                return data
            except json.JSONDecodeError as e:
                if DEBUG_VER == 3:
                    print(f"--------------------find_json if match error text:{text}")
                if attempt < max_attempts:
                    text = fix_json(json_string, e, model=MODEL)
                    if DEBUG_VER == 3:
                        print(f"Attempt {attempt}: Failed to parse JSON, reason: {e}. Retrying...")
                else:
                    if DEBUG_VER == 3:
                        print(f"All {max_attempts} attempts to parse JSON failed. Returning original text.")
                    pass
        else:
            if DEBUG_VER == 3:
                print(f"--------------------find_json else text:{text}")
            if attempt < max_attempts:
                if DEBUG_VER == 3:
                    print(f"Attempt {attempt}: No JSON string found in the text. Retrying...")
                pass
            else:
                if DEBUG_VER == 3:
                    print("No matching JSON string found. Returning original text.")
                pass

        # If no match or no success in this attempt, return the original text
        return text


def fix_json(text, json_error, model):
    """
    修复JSON字符串，使其成为有效的JSON。
    """
    NAIVE_FIX = f"""Instructions:
--------------
请修复JSON字符串，使其成为有效的JSON。
--------------

下面是原始的JSON字符串：
--------------
{text}
--------------
下面是的错误信息：
--------------
{json_error}
--------------

请仅回复json，用```json ... ```包裹json字符串："""
    
    messages = [{"role": "user", "content": NAIVE_FIX}]
    response = create_chat_completion(messages, model)
    answer = response.choices[0].message.content
    return answer

def dict_to_sentence(data):
    """
    Convert a dictionary into a descriptive sentence by enumerating key-value pairs.
    For example: {"name": "John", "age": 30} -> "name 是 John, age 是 30"
    
    Parameters:
        data (dict): The dictionary to convert.
        
    Returns:
        str: A sentence describing the dictionary keys and values.
    """
    try:
        if not isinstance(data, dict):
            raise ValueError("Input is not a dictionary")

        return ", ".join(f"{key} 是 {value}" for key, value in data.items())
    except Exception as e:
        if DEBUG_VER == 3:
            print(f"Error in dict_to_sentence: {e}")
        return str(data)


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


def clean_text(text):
    """
    Remove any parenthetical segments (including Chinese parentheses) and trim whitespace.
    For example, "This is a sentence(remark)" -> "This is a sentence"
    
    Parameters:
        text (str): The text to clean.
        
    Returns:
        str: The cleaned text.
    """
    pattern = r'[\(（][^\)）]*[\)）]'  # Pattern to match parentheses and their contents
    cleaned_text = re.sub(pattern, '', text).strip()
    return cleaned_text


def find_dict_by_element(dict_list, target_element):
    """
    Given a list of dictionaries, return all dictionaries where  '列名中文描述' contains the target_element.
    Parameters:
        dict_list (list): A list of dictionaries, each expected to have '列名中文描述' key.
        target_element (str): The element to search for.
        
    Returns:
        list: A list of dictionaries that contain target_element in '列名中文描述'.
    """
    return [d for d in dict_list if target_element in d.get('列名中文描述', [])]


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


def exec_sql_s(sql):
    """
    Execute a given SQL query on a remote endpoint and return the result.
    Uses 'Access_Token' for authorization and limits the result to 10 rows.

    Parameters:
        sql (str): The SQL query to be executed.

    Returns:
        list: The query result as a list of rows (dictionaries), or None if not found.
    """
    headers = {
        "Authorization": f'Bearer {Access_Token}',
        "Accept": "application/json"
    }
    url = "https://comm.chatglm.cn/finglm2/api/query"

    response = requests.post(url, headers=headers, json={
        "sql": sql,
        "limit": 50
    })
    response_json = response.json()

    # If there's no 'data' field, print the full response for debugging
    if 'data' not in response_json:
        if DEBUG_VER == 3:
            print(response_json)
        pass

    # Return 'data' if present
    return response_json.get('data', None)
    

if ASYNC_LLM_TIME >= 2:
    def create_chat_completion(messages, model=MODEL,tempt=0):
        """
        调用 API 接口返回信息
        """
        response = client.chat.completions.create(
            model=model,
            stream=False,
            messages=messages,
            temperature=tempt
        )
        return response
        
    
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
        print(f"---->get_best异常:{answer}")
        return 1

# #  大脑 Brain
# 用于做各种思考判断的模块。
# ## 问题模块
# ### 问题预处理
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

# ### 问题主体的背景板
# 预处理使用躺躺的方案，在这里进行了相关的解释说明。

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

# ### 问题重写

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

def find_question_keywords(original_question):
    prompt = (
        f"问题：'{original_question}'。"
        "提取问题中最重要的1到2个关键词（必须是原文里的）。只返回关键词，不要过程和解释。"
        "以下是示例：\n"
        "user:问题：'他一共披露过多少次分红信息？2021年披露了多少次？'。"
        "assistant:披露、分红信息"
        "user:问题：'20201月1日至年底退休了多少人？'。"
        "assistant:退休"
    )

    messages = [{"role": "user", "content": prompt}]
    response = create_chat_completion(messages)
    return response.choices[0].message.content

# ## SQL模块

# ### SQL专家提示优化->正向知识库

def way_string_2(question):
    way_string_2 = ">>查询参考："

    if "近一个月最高价" in question:
        way_string_2 += "查询近一个月最高价,你写的sql语句可以优先考虑表中已有字段HighPriceRM  近一月最高价(元)\n"
    if "近一个月最低价" in question:
        way_string_2 += "查询近一月最低价(元),你写的sql语句直接调用已有字段LowPriceRM\n"
    if ('年度报告' in question and '最新更新' in question) or '比例合计' in question:
        way_string_2 += """特别重要一定注意，查询最新更新XXXX年年度报告，机构持有无限售流通A股数量合计InstitutionsHoldProp最多公司代码，优先使用查询sql语句，SELECT *
                            FROM AStockShareholderDB.LC_StockHoldingSt
                            WHERE date(EndDate) = 'XXXX-12-31'
                              AND UpdateTime = (
                                SELECT MAX(UpdateTime)
                                FROM AStockShareholderDB.LC_StockHoldingSt
                                WHERE date(EndDate) = 'XXXX-12-31'
                              ) ORDER BY InstitutionsHoldings DESC LIMIT 1 ，XXXX代表问题查询年度，sql语句禁止出现group by InnerCode;

                              查询最新更新XXXX年年度报告,公司机构持有无限售流通A股比例合计InstitutionsHoldProp是多少,优先使用查询sql语句，SELECT InstitutionsHoldProp
                            FROM AStockShareholderDB.LC_StockHoldingSt
                            WHERE date(EndDate) = 'XXXX-12-31'
                              AND UpdateTime = (
                                SELECT MAX(UpdateTime)
                                FROM AStockShareholderDB.LC_StockHoldingSt
                                WHERE date(EndDate) = 'XXXX-12-31'
                              ) ORDER BY InstitutionsHoldings DESC LIMIT 1 ，XXXX代表问题查询年度，sql语句禁止出现group by InnerCode;\n"""

    if '新高' in question:
        way_string_2 += """新高 要用AStockMarketQuotesDB.CS_StockPatterns现有字段
        
        查询今天是2021年01月01日，创近半年新高的股票有几只示。示例sql语句:SELECT count(*)  FROM AStockMarketQuotesDB.CS_StockPatterns
                WHERE  IfHighestHPriceRMSix=1 AND date(TradingDay)='2021-01-01;
                判断某日 YY-MM-DD  InnerCode XXXXXX 是否创近一周的新高，查询结果1代表是,IfHighestHPriceRW字段可以根据情况灵活调整  SELECT   InnerCode,TradingDay,IfHighestHPriceRW  FROM AStockMarketQuotesDB.CS_StockPatterns
                WHERE  date(TradingDay)='2021-12-20' AND InnerCode = '311490';\n
                
                """
    if '成交额' in question and '平均' in question:
        way_string_2 += """查询这家公司5日内平均成交额是多少。示例sql语句:SELECT count(*)  FROM AStockMarketQuotesDB.CS_StockPatterns
                WHERE  IfHighestHPriceRMSix=1 AND date(TradingDay)='2021-01-01;\n"""
    if '半年度报告' in question:
        way_string_2 += """查询XXXX年半年度报告的条件为：year(EndDate) = XXXX AND InfoSource='半年度报告';\n"""

    if "硕士或研究生学历（及以上）" in question:
        way_string_2 += """查询某公司的硕士或研究生学历（及以上）的人数,可参考 AStockOperationsDB.LC_Staff 表中 TypeName = '研究生及以上学历 ，来查询'\n"""

    if "退休"  in question and ('多少人' in question or '几个人' in question or '多少个人' in question):
        way_string_2 += """查询某公司某年的退休人数的条件可参考：ClassfiedMethod = 9300 AND year(EndDate) = XXXX; XXXX表示要查的年份，9300表示离退人数
                           查询某年（只要该年）的某公司退休人数，示例问题：'2020年1月1日至年底常熟银行退休了多少人？'示例sql语句：SELECT SUM(CASE WHEN date(EndDate) BETWEEN '2020-01-01' AND '2020-12-31' THEN EmployeeSum ELSE 0 END) -  SUM(CASE WHEN date(EndDate) BETWEEN '2019-01-01' AND '2019-12-31' THEN EmployeeSum ELSE 0 END) AS EmployeeDifference FROM AStockOperationsDB.LC_Staff WHERE CompanyCode = 71598 AND ClassfiedMethod = 9300; 解析：条件过滤ClassfiedMethod=9300表示退离人数、CompanyCode（公司代码），得到的EmployeeSum就是对应人数，由于是求某一年所以得把当年的值减去前一年的。\n
                        """

    if "董秘" in question:
        way_string_2 += """查询某公司董秘,你写的sql语句可以优先考虑 AStockBasicInfoDB.LC_StockArchives 表中已有字段 SecretaryBD \n"""

    if "前三高管" in question and ('报酬' in question or '薪资' in question or '薪酬' in question):
        way_string_2 += """查询某公司前三高管薪资，优先考虑 AStockOperationsDB.LC_RewardStat 表中字段 High3Managers 前三名高管报酬(元)\n"""

    if "CN" in question:
        way_string_2 += """查询CN公司的条件可参考 USStockDB.US_CompanyInfo 表中字段 PEOStatus = 'CN' 来查询\n"""
    # V40新增
    if "上市" in question and ("年" in question or "时" in question):  # 上市公司或时间
        way_string_2 += """查询某公司上市的时间条件可参考 date(ListedDate) AND ListedState = 1 来查询,ListedDate是上市日期，ListedState是上市状态，等于1表示上市\n"""
    if "基金" in question:  # 这支基金是什么时候生效的
        if "生效" in question and "时" in question:
            way_string_2 += """查询某基金生效时间的SQL可以优先考虑 PublicFundDB.MF_FundProdName 库表中已有字段 EffectiveDate 生效日期\n"""
    if "指数" in question:
        if "多少种": # 中证指数有限公司发布了多少种指数？
            way_string_2 += """查询某公司发布了多少种指数的SQL，优先考虑 IndexDB.LC_IndexBasicInfo 库表中已有字段 IndexType 指数类别\n"""
        if "多少" in question and "公司" in question:
            way_string_2 += """查询发布的指数涵盖多少家公司的SQL，可以使用 IndexDB.LC_IndexBasicInfo 库表中已有字段 IndexCode 指数内部编码 和 证券主表中的 InnerCode 关联\n"""
    if "非公开增发" in question: # 该公司在过去的进行了几次非公开增发？
        way_string_2 += """查询某公司非公开增发的信息，可以使用 AStockFinanceDB.LC_AShareSeasonedNewIssue 库表中已有字段 IssueType = 21 表示非公开增发的类别\n"""
    if "年报" in question or "年度报告" in question:
        way_string_2 += """查询年报（年度报告）可参考 库表中存在的字段 InfoSource = '年度报告' 来查询\n"""
    # v60新增的：
    if "子类" in question and "概念" in question:
        way_string_2 += """查询某子类概念的纳入数量，示例问题：'化工纳入过多少个子类概念？'示例sql语句：SELECT count(distinct(ConceptCode)) FROM AStockIndustryDB.LC_ConceptList WHERE SubclassName LIKE '%化工%'; 解析：通过SubclassName（所属2级概念代码），统计ConceptCode（所属概念板块编码）对应的数量\n"""
        way_string_2 += """查询某子类概念的某年纳入过多少个以及哪些，示例问题：'2021年纳入过多少个？分别是？ '示例sql语句：SELECT ConceptCode, ConceptName FROM AStockIndustryDB.LC_ConceptList WHERE SubclassName LIKE '%化工%' AND year(begindate) = 2021; 解析：通过SubclassName（所属2级概念代码）和begindate（起始日期）找到对应的ConceptCode（所属概念板块编码）和ConceptName（概念名称）\n"""
        if "股票" in question and "多少" in question:
            way_string_2 += """查询某子类概念某年的纳入股票数量有多少，示例问题：'2021年化工纳入的这些概念的股票有多少只？'示例sql语句：SELECT COUNT(DISTINCT(InnerCode)) AS 纳入的这些概念的有多少 FROM AStockIndustryDB.LC_COConcept WHERE ConceptCode in (SELECT ConceptCode FROM AStockIndustryDB.LC_ConceptList WHERE SubclassName LIKE '%化工%' AND year(begindate) = 2021); 解析通过SubclassName（所属2级概念代码）和begindate（起始日期）找到对应的ConceptCode（所属概念板块编码），然后通过ConceptCode（所属概念板块编码）关联其它表找到InnerCode（证券内部编码）\n"""
        if "三级行业" in question and "年" in question and "纳入" in question:
            way_string_2 += """查询某子类概念某年的纳入股票分别属于哪些三级行业，示例问题：'2021年化工纳入的这些概念的股票分别属于哪些三级行业（仅考虑2021年新纳入的行业数据）？'示例sql语句：SELECT DISTINCT ThirdIndustryCode, ThirdIndustryName FROM AStockIndustryDB.LC_ExgIndustry WHERE year(InfoPublDate) = 2021 AND CompanyCode in (SELECT DISTINCT(CompanyCode) FROM ConstantDB.SecuMain WHERE InnerCode in (SELECT DISTINCT(InnerCode) FROM AStockIndustryDB.LC_COConcept WHERE ConceptCode in (SELECT ConceptCode FROM AStockIndustryDB.LC_ConceptList WHERE SubclassName LIKE '%化工%' AND year(begindate) = 2021)));\n"""
    if "三级行业" in question and "属于" in question:
        way_string_2 += """查询某公司属于哪个三级行业，示例问题：'天顺风能属于哪个三级行业？'示例sql语句：SELECT ThirdIndustryName FROM AStockIndustryDB.LC_ExgIndustry WHERE CompanyCode = 81722 AND IfPerformed = 1 ORDER BY InfoPublDate DESC LIMIT 1; 解析：IfPerformed（是否执行，1-是；2-否），CompanyCode（公司代码）指向要查询的公司，ThirdIndustryName（对应三级行业名称）\n"""
    if "多少" in question and "发布" in question and "行业" in question and "股票" in question:
        way_string_2 += """查询某三级行业发布了多少支股票，示例问题：'2021年发布的该行业的股票有多少只？'示例sql语句：SELECT COUNT(DISTINCT CompanyCode) AS 风电零部件_2021 FROM AStockIndustryDB.LC_ExgIndustry WHERE ThirdIndustryName = '风电零部件' AND YEAR(InfoPublDate) = 2021 AND IfPerformed = 1;解析：通过多个条件IfPerformed（是否执行，1-是；2-否）和InfoPublDate（发布日期）和ThirdIndustryName（三级行业名称）去过滤出CompanyCode（公司代码）并去重统计\n"""
    if "行业" in question and "行业营业收入" in question:
        way_string_2 += """查询某三级行业某时间发布的最高行业营业收入，示例问题：'2021年8月，风电零部件行业公布的最高行业营业收入是多少万元？'示例sql语句：SELECT MAX(IndOperatingRevenue) AS MaxOperatingRevenue FROM AStockIndustryDB.LC_IndFinIndicators WHERE YEAR(InfoPublDate) = 2021 AND MONTH(InfoPublDate) = 8 AND IndustryCode = (SELECT DISTINCT(ThirdIndustryCode) FROM AStockIndustryDB.LC_ExgIndustry WHERE ThirdIndustryName = '风电零部件');解析：子sql过滤ThirdIndustryName（三级行业名称）找到ThirdIndustryCode（三级行业代码）去跟IndustryCode（行业代码）关联起来，InfoPublDate去指定对应的年月，找到最大的IndOperatingRevenue（行业营业收入TTM(万元)）\n"""        
    if "年度" in question and "发布日期" in question:
        way_string_2 += """查询某公司某年度经营情况的信息发布日期，示例问题：'000958公司2021年度经营情况的信息发布日期是（XXXX-XX-XX）？'示例sql语句：SELECT distinct(InfoPublDate) FROM AStockFinanceDB.LC_MainOperIncome WHERE CompanyCode = 515 AND year(EndDate) = 2021 AND InfoSource = '年度报告';解析：过滤需要通过InfoSource（信息来源）=年度报告 和 EndDate（业务日期）来筛选，得出InfoPublDate（发布日期）\n"""
    if "近一年最低价" in question:
        way_string_2 += """查询某公司某时间的近一年最低价，示例问题：'2020年6月24日，阅文集团近一年最低价是多少？'示例sql语句：SELECT HighPriceRM FROM HKStockDB.CS_HKStockPerformance WHERE InnerCode = 1150048 AND DATE(TradingDay) = '2020-06-24'; 解析：条件过滤TradingDay（交易日期）='对应时间' 和 InnerCode（证券内部编码），得到HighPriceRM（近一月最高价(元)）\n"""
    if "差额" in question:
        way_string_2 += """求两个值的差额，示例问题：'阅文集团56.00元22.95元之间的差额是多少？'示例sql语句：SELECT 56.00 - 22.95; 解析：涉及差额等计算的用sql语句来计算，得到最终结果。注意：这两个值请替换成实际值\n"""
    if "名称" in question and "变更" in question:
        way_string_2 += """查询某时间内，有多少公司的名称发生变更，示例问题：'2020年之间 哪些公司进行公司名称全称变更，公司代码是什么？'示例sql语句：SELECT CompanyCode,ChiName FROM AStockBasicInfoDB.LC_NameChange WHERE year(InfoPublDate) = 2020 OR year(ChangeDate) = 2020; 解析：需要获取InfoPublDate（发布时间）和ChangeDate（变更时间）的所有公司，尽可能获取到更多，CompanyCode（公司代码）,ChiName（公司名称）则是最终结果\n"""
    if "名下" in question and "公司" in question:
        way_string_2 += """查询某实控人名下有多少公司，示例问题：'中国东方航空集团有限公司名下一共有多少家公司？"'示例sql语句：SELECT COUNT(DISTINCT CompanyCode) FROM AStockShareholderDB.LC_ActualController WHERE ControllerName = '中国东方航空集团有限公司'; 解析：通过ControllerName（实际控制人名称）找到CompanyCode（公司代码）并统计去重后的数量\n"""
    if "业务" in question and "从事" in question and "多少" in question:
        way_string_2 += """查询某些公司中，有多少家从事某业务，示例问题：'中国东方航空集团有限公司名下的公司中，从事物流业务的有多少家？'示例sql语句：SELECT count(DISTINCT CompanyCode) FROM AStockBasicInfoDB.LC_Business WHERE MainBusiness like '%物流%' AND CompanyCode in (SELECT DISTINCT(CompanyCode) FROM AStockShareholderDB.LC_ActualController WHERE ControllerName = '中国东方航空集团有限公司'); 解析：子sql找到对应的CompanyCode（公司代码）并提供给主sql，配合MainBusiness（MainBusiness）找到满足条件的CompanyCode（公司代码）\n"""
    if "前十大股东" in question:
        if "持股比例" in question:
            way_string_2 += """查询某公司某年度前十大股东的持股比例，示例问题：'航锦科技股份有限公司2020年度，其前十大股东的持股比例变成了多少？'示例sql语句：SELECT Top10StockholdersProp FROM AStockShareholderDB.LC_StockHoldingSt WHERE CompanyCode = 414 AND year(EndDate) = 2020 AND InfoSource ='年度报告'; 解析：年度使用EndDate（业务日期）字段配合CompanyCode（公司代码）找到Top10StockholdersProp（前十大股东持股比例合计(%)）\n"""
    if "申万" in question and "行业"in question:
        if "新版" in question:
            way_string_2 += """查询新版申万某行业数据，示例问题：'2021年末，按新版申万行业分类，现在均胜电子属于什么申万一级行业？'示例sql语句：SELECT FirstIndustryName,FirstIndustryCode,InfoPublDate FROM AStockIndustryDB.LC_ExgIndustry WHERE CompanyCode = 1632 AND Standard = 38 AND IfPerformed = 1 AND InfoPublDate <= '2021-12-31' ORDER BY InfoPublDate DESC; 解析：WHERE条件Standard=38（表示用申万行业新版数据） 和 IfPerformed（是否执行了，1-是；2-否），找到对应一级行业则带上FirstIndustryName（所属一级行业名称）和FirstIndustryCode（所属一级行业代码）\n"""
        else:
            way_string_2 += """查询申万某行业数据，示例问题：'申万一级行业是交通运输的公司有多少家？'示例sql语句：SELECT COUNT(DISTINCT CompanyCode) AS CompanyCount FROM AStockIndustryDB.LC_ExgIndustry WHERE InfoSource = '申万研究所' AND FirstIndustryName = '交通运输' AND IfPerformed = 1; 解析：WHERE条件InfoSource = '申万研究所'（表示用申万的数据,问题中没有'新版'字眼则默认用这个，不用Standard） 和 IfPerformed（是否执行了，1-是；2-否），要找对应一级行业则带上FirstIndustryName，去统计去重后的CompanyCode（公司代码）\n"""
    if "变更" in question and "行业"in question:
        if "取消" in question:
            way_string_2 += """查询哪些公司某年变更了行业但在某年又取消了，示例问题：' 2021年，申万一级行业为交通运输的公司中，哪些公司在2020年变更了行业后又在2021年取消了变更？'示例sql语句：SELECT CompanyCode FROM AStockIndustryDB.LC_ExgIndustry WHERE year(InfoPublDate) = 2020 AND year(CancelDate) = 2021 AND CompanyCode in (SELECT DISTINCT CompanyCode FROM AStockIndustryDB.LC_ExgIndustry WHERE  InfoSource = '申万研究所' AND FirstIndustryName = '交通运输' AND IfPerformed = 1); 解析：子sql查到对应的CompanyCode（公司代码），并通过InfoPublDate（发布日期）匹配变更开始的时间和CancelDate（取消日期）取消的时间过滤出最终满足条件的CompanyCode（公司代码）\n"""
        else:
            way_string_2 += """查询哪些公司某年变更了行业，示例问题：'申万一级行业是交通运输的公司中多少家是20年变更的行业？'示例sql语句：SELECT COUNT(DISTINCT CompanyCode)  FROM AStockIndustryDB.LC_ExgIndChange WHERE year(InfoPublDate) = 2020 AND CompanyCode in (SELECT DISTINCT CompanyCode FROM AStockIndustryDB.LC_ExgIndustry WHERE InfoSource = '申万研究所' AND FirstIndustryName = '交通运输' AND IfPerformed = 1); 解析：子sql找到相关CompanyCode（公司代码）并配合InfoPublDate（发布日期）过滤，只要出现在库表AStockIndustryDB.LC_ExgIndChange（上市公司行业板块.公司行业变更表）里，则是变更行业的公司\n"""
    if "股东" in question and "基金管理" in question and "公司" in question:
        way_string_2 += """查询某些股东里，有几家是基金管理公司，示例问题：'陵榨菜公司上市公告书中的前十大股东里，有几家是基金管理公司？'示例sql语句：SELECT ThirdLvCode,SHName FROM AStockShareholderDB.LC_SHTypeClassifi WHERE ThirdLvCode in (2050400,2050500) AND SHName in (SELECT distinct SHList FROM AStockShareholderDB.LC_MainSHListNew WHERE CompanyCode = 81336 AND InfoTypeCode = 1 AND SHNo <= 10); 解析：子sql通过条件CompanyCode（公司代码）和InfoTypeCode=1（表示前十大股东）和SHNo<=10（表示股东排名）得到去重后的SHList（股东名称），主sql通过得到的股东名称加上'ThirdLvCode in (2050400,2050500)'（三级分类代码，2050400-公募基金管理公司，2050500-私募基金管理公司）过滤得到最终结果SHName（股东名称）\n"""
    if "多少" in question and "上市" in question and ("公司" in question or "企业" in question):
        way_string_2 += """查询某时间上市了多少家公司（企业），示例问题：'2010年一共上市了多少家企业？'示例sql语句：SELECT COUNT(distinct InnerCode) AS TotalListedCompanies FROM ConstantDB.SecuMain WHERE year(ListedDate) = 2010 AND ListedState = 1; 解析：过滤ListedDate（上市日期）和ListedState（上市状态，1-上市，5-终止，9-其他）得到上市公司数量\n"""
    if ("公司" in question or "企业" in question) and "注册" in question and "在" in question and "办公" not in question:
        way_string_2 += """查询某时某些公司里有多少在某地注册的，示例问题：'2021年上市的企业有多少是在北京注册的？'示例sql语句：SELECT COUNT(DISTINCT(ConstantDB.SecuMain.CompanyCode)) AS BeijingListedCompanies FROM ConstantDB.SecuMain JOIN AStockBasicInfoDB.LC_StockArchives ON ConstantDB.SecuMain.CompanyCode = AStockBasicInfoDB.LC_StockArchives.CompanyCode WHERE year(ListedDate) = 2010 AND ListedState = 1 AND AStockBasicInfoDB.LC_StockArchives.RegAddr like '%北京市%'; 解析：WHERE条件RegAddr（公司注册地址）可以过滤某地区和ListedDate（上市日期）和ListedState（上市状态，1-上市，5-终止，9-其他），得到在某年某市注册的公司数量\n"""
    if ("公司" in question or "企业" in question) and "注册" in question and "在" in question and "办公" in question:
        way_string_2 += """查询某时某些公司里有多少的注册和办公都在某地的，示例问题：'2021年上市的企业有多少注册和办公都在海淀的？'示例sql语句：SELECT DISTINCT ConstantDB.SecuMain.CompanyCode,InnerCode,ConstantDB.SecuMain.ChiName FROM ConstantDB.SecuMain JOIN AStockBasicInfoDB.LC_StockArchives ON ConstantDB.SecuMain.CompanyCode = AStockBasicInfoDB.LC_StockArchives.CompanyCode WHERE year(ListedDate) = 2010 AND ListedState = 1 AND AStockBasicInfoDB.LC_StockArchives.RegAddr like '%海淀%' AND AStockBasicInfoDB.LC_StockArchives.OfficeAddr like '%海淀%';解析：WHERE条件RegAddr（公司注册地址）、OfficeAddr（公司办公地址）、ListedDate（上市日期）、ListedState（上市状态，1-上市，5-终止，9-其他），得到最终结果\n"""
    if  "公司" in question or "企业" in question:
        if "季报" in question:
            if "预付款项" in question:
                way_string_2 += """查询某年某公司某一季报的预付款项，示例问题：'中南出版传媒集团股份有限公司2019年母公司一季报中预付款项是多少？'示例sql语句：SELECT AdvancePayment FROM AStockFinanceDB.LC_BalanceSheetAll WHERE CompanyCode = 80194 AND year(EndDate) = 2019 AND InfoSourceCode = 110103 AND IfMerged = 2; 解析：WHERE条件IfMerged（是否合并，1-合并报表；2-母公司报表）、InfoSourceCode（信息来源编码，110101-定期报告:年度报告，110102-定期报告:半年度报告，110103-定期报告:第一季报，110104-定期报告:第三季报，110105-定期报告:审计报告，120102-临时公告:年度报告(更正后)，120103-临时公告:半年度报告(更正后)，120104-临时公告:第一季报(更正后)，120105-临时公告:第三季报(更正后)）、CompanyCode（公司代码）、EndDate（业务日期），过滤得到AdvancePayment（预付款项(元)）\n"""
            if "总营收" in question or "营业总收入" in question:
                way_string_2 += """查询某年某公司某一季报的总营收，示例问题：'中南出版传媒集团股份有限公司2019年母公司一季报中总营收是多少？'示例sql语句：SELECT TotalOperatingRevenue FROM AStockFinanceDB.LC_IncomeStatementAll WHERE CompanyCode = 80194 AND year(EndDate) = 2019 AND InfoSourceCode = 110103 AND IfMerged = 2; 解析：WHERE条件IfMerged（是否合并，1-合并报表；2-母公司报表）、InfoSourceCode（信息来源编码，110101-定期报告:年度报告，110102-定期报告:半年度报告，110103-定期报告:第一季报，110104-定期报告:第三季报，110105-定期报告:审计报告，120102-临时公告:年度报告(更正后)，120103-临时公告:半年度报告(更正后)，120104-临时公告:第一季报(更正后)，120105-临时公告:第三季报(更正后)）、CompanyCode（公司代码）、EndDate（业务日期），过滤得到TotalOperatingRevenue（营业总收入）\n"""
        if "行业" in question:
            if "总市值" in question:
                way_string_2 += """查询某时间某公司所在某级行业的总市值，示例问题：'2021年8月4日，科达制造所属的二级行业当日行业总市值有多少？'示例sql语句：SELECT TotalMV FROM AStockIndustryDB.LC_IndustryValuation WHERE date(TradingDay) = '2021-08-04' AND IndustryName like (SELECT SecondIndustryName FROM AStockIndustryDB.LC_ExgIndustry WHERE CompanyCode = 1442 AND InfoPublDate <= '2021-08-04' ORDER BY InfoPublDate DESC LIMIT 1); 解析：子sql查询最近的InfoPublDate（发布时间）该公司对应的SecondIndustryName（所属二级行业名称），主sql通过IndustryName（行业名称）匹配SecondIndustryName（所属二级行业名称）配合TradingDay（交易日期），查到TotalMV（总市值(元)）\n"""
        if "处罚" in question:
            way_string_2 += """查询某时间有哪些公司收处罚，示例问题：'2019年12月12日哪家证券公司受到了处罚'示例sql语句：SELECT PartyName FROM CreditDB.LC_ViolatiParty WHERE year(BeginDate) = 2019 AND month(BeginDate) = 12 AND day(BeginDate) = 12; 解析：从CreditDB.LC_ViolatiParty（违规当事人处罚库表）中获取，通过BeginDate（起始日期）过滤对应时间，拿到PartyName（当事人）信息\n"""
        if "股东" in question:
            if "多少" in question or "哪些" in question or "几" in question:
                way_string_2 += """查询某公司是多少或哪些公司的股东，示例问题：'西南证券股份有限公司是多少家公司的股东？'示例sql语句：SELECT COUNT(DISTINCT CompanyCode) AS ShareholderCount FROM AStockShareholderDB.LC_MainSHListNew WHERE SHList = '西南证券股份有限公司'; 解析：通过SHList（股东名称）来过滤并统计去重后的CompanyCode（公司代码）\n"""
        if "借贷" in question:
            way_string_2 += """查询某公司的借贷情况，示例问题：'公司代码（1332, 298546）两家公司，哪家公司当年的借贷最多，共计多少？'示例sql语句：SELECT CompanyCode, sum(LatestLoanSum) as TotalLoanAmount FROM AStockEventsDB.LC_Credit WHERE CompanyCode in (1332, 298546) AND year(InfoPublDate)=2019 GROUP BY CompanyCode ORDER BY TotalLoanAmount DESC; 解析：LatestLoanSum（最新借贷金额(元)）\n"""
    if "怎么管理" in question or "如何管理" in question or "管理模式" in question:
        way_string_2 += """查询某公司持股的管理模式，优先考虑Management（管理模式）字段。示例问题：'Titan Wind Energy (Suzhou) Co., Ltd.在2020年首次信息发布中，如何管理（管理模式）？'示例sql语句：SELECT Management FROM AStockShareholderDB.LC_ESOP WHERE InnerCode = 12064 AND year(IniInfoPublDate) = 2020; 解析：通过IniInfoPublDate（首次发布信息时间）和InnerCode（证券内部编码）来筛选出对应的Management（管理模式）\n"""
    if "担保" in question:
        if "提供" in question:
            way_string_2 += """查询某公司为多少家提供过担保，示例问题：'东兴证券2019年一共为多少家公司提供过担保？'示例sql语句：SELECT COUNT(DISTINCT ObjectCode) AS CompanyCount FROM AStockEventsDB.LC_Warrant WHERE CompanyCode = 74956 AND ActionWays = 1201 AND year(InfoPublDate) = 2019; 解析：通过条件ActionWays（行为方式，1201-提供担保）、InfoPublDate（发布时间）、CompanyCode（公司代码）来过滤出次数。\n"""
        if "金额" in question:
            way_string_2 += """查询某公司提供担保项目中的金额，示例问题：' 2019年，东兴证券为这些公司提供的担保项目中，担保最多的金额是多少？'示例sql语句：SELECT MAX(LatestGuaranteeSum) AS MaxGuaranteeAmount FROM AStockEventsDB.LC_Warrant WHERE CompanyCode = 74956 AND ActionWays = 1201 AND year(InfoPublDate) = 2019; 解析：通过条件ActionWays（行为方式，1201-提供担保）、InfoPublDate（发布时间）、CompanyCode（公司代码）来过滤出LatestGuaranteeSum（最新担保金额(元)）并获取最大的\n"""
    if "基金" in question:
        if "管理" in question and "支" in question:
            way_string_2 += """查询某基金经理管理的基金情况，示例问题：'李一硕一共管理了多少支基金'示例sql语句：SELECT COUNT(distinct InnerCode) AS FundCount FROM PublicFundDB.MF_FundArchives WHERE Manager = '李一硕'; 解析：过滤条件Manager（管理经理）得到InnerCode并去重统计数量\n"""
        if "规模" in question:
            way_string_2 += """查询某基金的规模，示例问题：'李一硕管理的6支基金中，规模最大的是哪一个？'示例sql语句：SELECT InnerCode, FoundedSize FROM  PublicFundDB.MF_FundArchives WHERE Manager = '李一硕' ORDER BY FoundedSize DESC LIMIT 1; 解析：Manager（基金经理），FoundedSize（基金设立规模(份)）\n"""
        if "法人" in question:
            way_string_2 += """查询某基金的法人代表，示例问题：'289822基金的管理人法人是谁？'示例sql语句：SELECT LegalRepr FROM PublicFundDB.MF_InvestAdvisorOutline WHERE InvestAdvisorCode = (SELECT InvestAdvisorCode FROM PublicFundDB.MF_FundArchives WHERE InnerCode = '289822'); 解析：多表查询，先获取到InvestAdvisorCode（基金管理人代码），然后去匹配PublicFundDB.MF_InvestAdvisorOutline（公募基金管理人概况库表），得到最终结果LegalRepr（法人代表）\n"""
    if "成立" in question and "时间" in question and "多久" in question:
        way_string_2 += """如果要计算某公司成立多久，需要用2024年去相减，而不是CURRENT_DATE。\n"""
        
    # 示例问题：''示例sql语句：
    sql_prompts, highest_is_similar = find_sql_prompt(question, Knowledge_file)
    
    way_string_2 += sql_prompts
    if len(way_string_2) < 10:
        if "发布" in question:
            way_string_2 += "WHERE 时间字段优先考虑表中的 InfoPublDate(信息发布日期)。涉及范围时间的优先用BETWEEN做。\n"
        else:
            way_string_2 += "问题中没有时间特指时，时间字段优先考虑表中的 TradingDay(交易日期)或 EndDate(业务截止日期)。涉及范围时间的优先用BETWEEN做。\n"
        way_string_2 += to_get_question_columns(question)
    
    # print(f"------------------way_string_2:{way_string_2}")
    return way_string_2, highest_is_similar

def find_sql_prompt(question, Knowledge_file, threshold=SQL_PROMPT_THRESHOLD, top_n=1, use_local_vectors=USE_LOCAL_VECTORS):
    """
    批量比较 question 与多个 knowledge_question：
      1. 从知识库文件中加载数据，提取所有 knowledge_question 及对应的 sql_prompt。
      2. 一次性调用 calculate_similarity，将 question 作为基准，与所有 knowledge_question 进行比较。
      3. 筛选出相似度 >= threshold 的记录，按相似度降序排列，返回前 top_n 个 sql_prompt，
         同时返回最高相似度分数。
    """
    sql_prompts = []
    with open(Knowledge_file, "r", encoding="utf-8") as f:
        knowledge_data = json.load(f)
        
    candidate_questions = []      # 收集所有的 knowledge_question
    candidate_sql_prompts = []    # 对应的 sql_prompt
    
    # 遍历知识库数据，提取所有 candidate_questions 和对应的 sql_prompt
    for entry in knowledge_data:
        for item in entry.get("team", []):
            knowledge_question = item.get("question", "")
            sql_prompt_candidate = item.get("sql_prompt", "")
            candidate_questions.append(knowledge_question)
            candidate_sql_prompts.append(sql_prompt_candidate)
            
    highest_similarity = 0
    # 如果没有候选问题，则直接返回空结果
    if not candidate_questions:
        return "", highest_similarity

    if use_local_vectors:
        texts_for_batch = [question]
        similarity_scores = calculate_similarity(texts_for_batch,local_vectors=knowledge_embeddings_file)
    else:
        texts_for_batch = [question] + candidate_questions
        similarity_scores = calculate_similarity(texts_for_batch)
    
    # 遍历所有候选问题的相似度
    for idx, sim in enumerate(similarity_scores):
        if sim >= threshold and len(candidate_sql_prompts[idx]) >= 20:
            sql_prompts.append({
                "sql_prompt": candidate_sql_prompts[idx],
                "similarity": sim
            })
            if sim > highest_similarity:
                highest_similarity = sim
                
    # 如果没有符合条件的 sql_prompt，则返回空结果
    if not sql_prompts:
        return "", 0
        
    # 根据相似度降序排序
    sql_prompts_sorted = sorted(sql_prompts, key=lambda x: x['similarity'], reverse=True)
    
    # 获取前 top_n 个 sql_prompt，并拼接成字符串返回
    top_sql_prompts_list = [item["sql_prompt"] for item in sql_prompts_sorted[:top_n]]
    top_sql_prompts = "\n\n".join(top_sql_prompts_list) + "\n\n" if top_sql_prompts_list else ""
    if DEBUG_VER == 3:
        print(f"--------->highest_similarity:{highest_similarity},top_sql_prompts:{top_sql_prompts}")
    return top_sql_prompts, highest_similarity

# ### sql优化工具

"""
本方案中需要对模型生成呢的SQL语句进行优化。我们对由模型生成的 SQL 语句进行一个小的优化步骤，以使其在查询接口中能够正确执行。主要的优化措施包括：

1. 日期字段格式转换：函数 replace_date_with_day 会将形如 TradingDate = 'YYYY-MM-DD' 的条件自动转化为 date(TradingDate) = 'YYYY-MM-DD' 的格式。这样可以确保在特定查询引擎或数据库中根据日期进行正确的查询过滤。
2. SQL语句提取: 函数 extract_sql 会从给定的文本中提取出被 sql ...  包围的 SQL 代码片段，从而从较复杂的文本中获得纯净的 SQL 语句。
3. 接口查询执行：将优化后的 SQL 语句通过 select_data 函数发送到指定的 API 接口进行查询，并以 JSON 格式返回结果。
"""
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

# ### SQL范围收缩

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


def remove_parentheses(content):
    if isinstance(content, str):
        # 使用正则表达式清除括号及其内容
        content = re.sub(r'\([^)]*\)', '', content)  # 清理英文括号及其内容
        content = re.sub(r'（[^）]*）', '', content)  # 清理中文括号及其内容
    return content


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

# %% [markdown] {"jupyter":{"outputs_hidden":false}}
# ### 深度库表搜索

# %% [code] {"execution":{"iopub.status.busy":"2025-03-07T09:14:36.645323Z","iopub.execute_input":"2025-03-07T09:14:36.645670Z","iopub.status.idle":"2025-03-07T09:14:36.670860Z","shell.execute_reply.started":"2025-03-07T09:14:36.645641Z","shell.execute_reply":"2025-03-07T09:14:36.669712Z"},"jupyter":{"source_hidden":true}}
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

# ## 回答模块

def extract_table_names(sql):
    # 正则匹配 FROM 关键字后的库表名，库表名后会有空格
    matches = re.findall(r'FROM\s+([\w\.]+)\s', sql, re.IGNORECASE)
    return matches

def all_tables_in_prompt(tables_name_list, main_sql_prompts):
    lower_prompts = main_sql_prompts.lower()
    return all(table.lower() in lower_prompts for table in tables_name_list)


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

# ### 回答优化模块

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

# ### 匹配历史问答对

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

# ## 运行脚本解决问题
# 这里展现了对单个问题的完整流程。主程序将会遍历这个过程，直到完成所有问题。

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


if __name__ == "__main__":
    '''
      online devlop 
    '''
    in_param_path = sys.argv[1]
    out_path = sys.argv[2]
    
    try:
        main_start_time = time.time()
        with open(in_param_path, 'r') as load_f:
            input_params = json.load(load_f)
        questionFile = input_params["fileData"]["questionFilePath"]
        # 读取问题文件
        with open(questionFile, 'r') as load_f:
            q_json_list = json.load(load_f)

        idx_ranges = [(0,100)]
        results = main_answer(q_json_list, out_path, idx_ranges)
        main_elapsed_time = time.time() - main_start_time
        print(f"Completed main in {main_elapsed_time:.2f} seconds")
    except Exception as e:
        print("Error: %s" % e)
        sys.exit(1) 