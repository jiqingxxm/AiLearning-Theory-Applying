FROM hubdocker.aminer.cn/library/python-base:1.0.0

WORKDIR /app

# 创建目录

COPY devlop_home/ /app/devlop_home/
COPY main.py /app/devlop_home

RUN pip install --upgrade pip --progress-bar off  -i https://mirrors.aliyun.com/pypi/simple/
RUN pip install --no-cache-dir jsonlines
RUN pip install --no-cache-dir --progress-bar off -r /app/devlop_home/requirements.txt  -i https://mirrors.aliyun.com/pypi/simple/

ENV OPENBLAS_NUM_THREADS=1

CMD ["/app/py_devlop.sh", "/app/devlop_data/input_param.json", "/app/devlop_result/answer.json" ]