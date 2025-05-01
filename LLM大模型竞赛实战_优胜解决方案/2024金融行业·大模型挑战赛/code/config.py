import pandas as pd
import json
from zhipuai import ZhipuAI


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

# %% [markdown] {"jupyter":{"outputs_hidden":false}}
# ### A股库表

# %% [code] {"jupyter":{"source_hidden":true},"execution":{"iopub.status.busy":"2025-02-28T14:23:48.686737Z","iopub.execute_input":"2025-02-28T14:23:48.687128Z","iopub.status.idle":"2025-02-28T14:23:49.400010Z","shell.execute_reply.started":"2025-02-28T14:23:48.687094Z","shell.execute_reply":"2025-02-28T14:23:49.398887Z"}}
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

# %% [code] {"jupyter":{"source_hidden":true},"execution":{"iopub.status.busy":"2025-02-28T14:23:49.401247Z","iopub.execute_input":"2025-02-28T14:23:49.401559Z","iopub.status.idle":"2025-02-28T14:23:49.408029Z","shell.execute_reply.started":"2025-02-28T14:23:49.401522Z","shell.execute_reply":"2025-02-28T14:23:49.406723Z"}}
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

# %% [code] {"jupyter":{"source_hidden":true},"execution":{"iopub.status.busy":"2025-02-28T14:23:49.607423Z","iopub.execute_input":"2025-02-28T14:23:49.607743Z","iopub.status.idle":"2025-02-28T14:23:49.612387Z","shell.execute_reply.started":"2025-02-28T14:23:49.607716Z","shell.execute_reply":"2025-02-28T14:23:49.611305Z"}}
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

# %% [markdown] {"jupyter":{"outputs_hidden":false}}
# ### 美股库表

# %% [code] {"jupyter":{"source_hidden":true},"execution":{"iopub.status.busy":"2025-02-28T14:23:49.613668Z","iopub.execute_input":"2025-02-28T14:23:49.613943Z","iopub.status.idle":"2025-02-28T14:23:49.793535Z","shell.execute_reply.started":"2025-02-28T14:23:49.613920Z","shell.execute_reply":"2025-02-28T14:23:49.792347Z"}}
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

# %% [code] {"ExecuteTime":{"end_time":"2024-12-16T17:51:00.837232Z","start_time":"2024-12-16T17:51:00.806726Z"},"jupyter":{"source_hidden":true},"execution":{"iopub.status.busy":"2025-02-28T14:23:49.794694Z","iopub.execute_input":"2025-02-28T14:23:49.795036Z","iopub.status.idle":"2025-02-28T14:23:49.801122Z","shell.execute_reply.started":"2025-02-28T14:23:49.795009Z","shell.execute_reply":"2025-02-28T14:23:49.799520Z"}}
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

with open('./devlop_home/2024-fic-lmc-data-0217/items_another.json', 'r', encoding='utf-8') as f:
    items_another = json.load(f)

