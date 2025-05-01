# 2024金融行业·大模型挑战赛



## [官网地址](<https://competitions.zhipuai.cn/matchDetail?id=120241202000000003>)

来源于由清华大学基础模型研究中心主办的《2024金融行业·大模型挑战赛》。400道涉及77张表、3000+字段、涵盖了58个二级市场各个领域（股权、交易、基金、财务、行业、港股、美股等）的类似题目。完全复刻了金融行业二级市场的真实需求。比赛历时4个月，设有初赛A/B轮、复赛A/B轮及决赛答辩多个阶段，吸引1337位选手、300余支队伍参与，最终44支队伍晋级复赛，13支队伍进入决赛，充分展现出金融智能化应用的创新潜力。

赛事支持： 智谱, 博时基金, 安硕信息, 恒生聚源, Gitee, 魔搭社区, ZLead硅谷委员会, Huggingface, WaytoAGI



## 我们的主题：让大模型像人一样思考

我们的答辩PPT：[ShallowRest浅寻止步_决赛答辩.pdf](https://github.com/ben1234560/AiLearning-Theory-Applying/blob/master/LLM%E5%A4%A7%E6%A8%A1%E5%9E%8B%E7%AB%9E%E8%B5%9B%E5%AE%9E%E6%88%98_%E4%BC%98%E8%83%9C%E8%A7%A3%E5%86%B3%E6%96%B9%E6%A1%88/2024%E9%87%91%E8%9E%8D%E8%A1%8C%E4%B8%9A%C2%B7%E5%A4%A7%E6%A8%A1%E5%9E%8B%E6%8C%91%E6%88%98%E8%B5%9B/ShallowRest_%E6%B5%85%E5%AF%BB%E6%AD%A2%E6%AD%A5.pdf)

亮点：自适应思维机制（自动快或者深思考）、自纠正分解决策树。



## 项目目录结构

```
code/
├── config.py +++++++++++++++++++++++++++++++ 配置信息和数据读取
├── main.py +++++++++++++++++++++++++++++++++ 运行主函数
├── main_all.py +++++++++++++++++++++++++++++ 运行主函数备份（全部代码没拆的提交版）
├── Dockerfile ++++++++++++++++++++++++++++++ 用来构建docker镜像的文件
└── agent/
    ├── __init__.py
    ├── brain.py ++++++++++++++++++++++++++++ 逻辑运行主代码
    ├── llm_client.py +++++++++++++++++++++++++++ LLM调用代码
    ├── optimization.py +++++++++++++++++++++ 获取及调整答案
    ├── sql/
    │   ├── __init__.py
    │   ├── exec.py +++++++++++++++++++++++++ 运行sql
    │   ├── process_sql.py ++++++++++++++++++ sql优化
    │   └── knowledge.py ++++++++++++++++++++ sql提示词
    ├── embeddings/
    │   ├── __init__.py
    │   └── embedding_utils.py+++++++++++++++ 求相似度
    ├── schema/
    │   ├── __init__.py
    │   ├── schema_utils.py +++++++++++++++++ 策略代码，表的匹配等
    └── question/
    │   ├── __init__.py
    │   └── question_utils.py +++++++++++++++ 问题重写及获取背景板
    └── utils/
    │   ├── __init__.py
    │   └── async_util.py +++++++++++++++++++ 决策树代码
    │   └── io_utils.py +++++++++++++++++++++ 读取数据函数
    │   └── json_utils.py +++++++++++++++++++ json相关代码
    │   └── text_utils.py +++++++++++++++++++ text相关代码
    └── devlop_home/
    │   ├── 2024-fic-lmc-data-0217/ +++++++++ 相关数据
    │   ├── input_param.json ++++++++++++++++ 按赛方要求放的
    │   ├── question.json +++++++++++++++++++ 问题（初赛A榜的，我用来测试）
    │   ├── requirements.txt ++++++++++++++++ 代码运行所需依赖
    └── devlop_data/ ++++++++++++++++++++++++ 赛方线上运行时，放数据的地方
    └── devlop_result/ ++++++++++++++++++++++ 运行后放结果的地方
```

## 运行环境

Python版本为3.10（3.9+兼容），各个Python包版本见requirements.txt，使用如下命令即可安装：

```
pip install -r code/devlop_home/requirements.txt
```



## 构建镜像运行代码

~~~
1. 登录认证（可以不登录，比赛方的环境可能没有了）：
docker login hubdocker.aminer.cn

2. 基础镜像获取：
docker pull hubdocker.aminer.cn/library/python-base:1.0.0

创建：docker build -t hubdocker.aminer.cn/00c5912021a54b938e4f22a10eeb96cb/shallowrest_submit:v_1 .
其中hubdocker.aminer.cn是比赛方给的，换成别的也可以
运行测试：
docker run -d --name test_v_1 hubdocker.aminer.cn/00c5912021a54b938e4f22a10eeb96cb/shallowrest_submit:v_1 tail -f /dev/null
docker exec -it test_v_1 /bin/bash

运行：
cp /app/devlop_home/*.json /app/devlop_data/
/app/py_devlop.sh /app/devlop_data/input_param.json /app/devlop_result/answer.json
head -n40 /app/devlop_result/answer.json
~~~



## 一些额外的点

1. 举办方的支持很到位，是我打过这么多国内比赛中，响应最到位的了。（尤其是在新赛题的情况下）
2. 应该能走的很远，看钱够不够了。

**祝清竞越办越好🎉🎉🎉**



## 终榜季军（图还没给，但钱到账了，神速！）





