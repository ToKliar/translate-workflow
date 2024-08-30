FROM apache/airflow:2.9.2


USER root
# 安装所需的系统包
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget build-essential zlib1g-dev libssl-dev libbz2-dev libreadline-dev libsqlite3-dev liblzo2-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 下载并安装 Python 3.12.3
RUN wget https://www.python.org/ftp/python/3.12.3/Python-3.12.3.tgz && \
    tar xzf Python-3.12.3.tgz && \
    cd Python-3.12.3 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.12.3 Python-3.12.3.tgz

# 设置 Python 3.12.3 为默认 Python 版本
RUN update-alternatives --install /usr/bin/python python /usr/local/bin/python3.12 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.12 1

USER airflow

# 下载需要的 python package
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# 安装 nltk 并下载分词器
RUN python -m nltk.downloader punkt

# 安装 spacy 并下载 en_core_web_sm 进行分句
RUN python -m spacy download en_core_web_sm

# 2. 导入演示 dag 展示基本功能，也许需要清除默认的 dag？
# COPY --chown=airflow:root DEMO_DAG_PATH /opt/airflow/dags

COPY nlp_bert_document-segmentation_english-base  /opt/airflow/nlp_bert_document-segmentation_english-base

# 3. 导入默认全局配置项
ENV AIRFLOW__CORE__LOAD_EXAMPLES=True