from airflow import DAG
from datetime import datetime, timedelta

from operators.output import OutputOperator
from operators.segment import SegmentOperator
from operators.split import SplitOperator
from operators.translate import LLMTranslateOperator
from operators.input import InputOperator

default_args = {
    'owner': 'mayixiao',
    'depends_on_past': True,
    'email': ['1224769259@qq.com'],
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('basic_translate', default_args=default_args, schedule_interval='@once')

t1 = InputOperator(task_id = "1", file_path="/opt/airflow/dags/story.txt", text_name="story", dag=dag)
t2 = SegmentOperator(task_id = "2", dag=dag)
t3 = SplitOperator(task_id="3", dag=dag)
t4 = LLMTranslateOperator(task_id = "4", src_lang='en', tgt_lang='zh-CHS', dag=dag)
t5 = OutputOperator(task_id = "5", dag=dag, file_path="/opt/airflow/dags/story_trans.txt")

t1 >> t2 >> t3 >> t4 >> t5
