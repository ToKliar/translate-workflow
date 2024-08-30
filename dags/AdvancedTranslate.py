from airflow import DAG
from datetime import datetime, timedelta

from operators.choose import ChooseOperator
from operators.input import InputOperator
from operators.output import OutputOperator
from operators.proof import ProofOperator
from operators.segment import SegmentOperator
from operators.split import SplitOperator
from operators.translate import LLMTranslateOperator, MachineTranslateOperator

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

def seg_paragraph(doc: str) -> list[str]:
  result = doc.split("\n")
  return [item for item in result if len(item) != 0]

def split_tu(sentences: list[str]) -> list[list[int]]:
  return [list(range(len(sentences)))]


dag = DAG('advanced_translate', default_args=default_args, schedule_interval='@once')

t1 = InputOperator(task_id = "1", file_path="/opt/airflow/dags/story.txt", text_name="story", dag=dag)
t2 = SegmentOperator(task_id = "2", dag=dag, seg_paragraph=seg_paragraph)
t3 = SplitOperator(task_id="3", dag=dag, split=split_tu)


t4 = MachineTranslateOperator(task_id = "4", src_lang='en', tgt_lang='zh-CHS', dag=dag)
t5 = LLMTranslateOperator(task_id = "5", src_lang='en', tgt_lang='zh-CHS', dag=dag)
t6 = LLMTranslateOperator(task_id = "6", src_lang='en', tgt_lang='zh-CHS', dag=dag)
t7 = ProofOperator(task_id = "7", dag=dag)

t8 = OutputOperator(task_id = "8", dag=dag, file_path="/opt/airflow/dags/story_proof.txt")

t9 = ChooseOperator(task_id = "9", dag=dag)
t10 = OutputOperator(task_id = "10", dag=dag, file_path="/opt/airflow/dags/story_choose.txt")

t1.set_downstream(t2)
t2.set_downstream(t3)
t3.set_downstream([t4, t5, t6])
t6.set_downstream(t7)
t7.set_downstream(t8)

t7.set_downstream(t9)
t4.set_downstream(t9)
t5.set_downstream(t9)
t9.set_downstream(t10)



