# agent/sql/knowledge.py
import json
from config import *
from agent.embeddings.embedding_utils import calculate_similarity
from agent.schema.schema_utils import to_get_question_columns

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

    # if "硕士或研究生学历（及以上）" in question:
    #     way_string_2 += """查询XXXX年某公司的硕士或研究生学历（及以上）的条件可参考：year(EndDate) = XXXX AND TypeName = '研究生及以上学历'"""
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
