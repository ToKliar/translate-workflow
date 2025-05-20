from airflow import DAG
from datetime import datetime, timedelta

from data.relational_data import ParagraphHandler, TranslationSegmentHandler, WordHandler, \
    TranslationMemoryHandler, MatchHandler
from data.slot import DataSlot
from operators.back_translation import BackTranslationOperator
from operators.extract import ExtractOperator
from operators.input import InputOperator
from operators.output import OutputOperator
from operators.retrieve import RetrieveOperator
from operators.segment import SegmentOperator, SegmentConfig
from operators.translate import TranslateOperator
from operators.word_translate import WordTranslateOperator
from operators.introduce import IntroduceOperator

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


dag_id = "a_word"

def generate_slot(type_name: str) -> DataSlot:
    return DataSlot(slot_id=f"{dag_id}", type_name=type_name)

dag = DAG(dag_id, default_args=default_args, schedule_interval='@once')

pargraph_slot = generate_slot(ParagraphHandler.type_name)
segment_slot = generate_slot(TranslationSegmentHandler.type_name)
word_slot = generate_slot(WordHandler.type_name)
translation_memory_slot = generate_slot(TranslationMemoryHandler.type_name)
match_slot = generate_slot(MatchHandler.type_name)

file_names = []

text_name = "bleak_house"
for i in range(2):
    file_names.append(f"/opt/airflow/dags/data/{text_name}_{i+1}.txt")

t1 = InputOperator(task_id = "1", file_paths=file_names, text_name=text_name, dag=dag, output_slot=pargraph_slot)
t2 = SegmentOperator(task_id = "2", dag=dag, input_slots=[pargraph_slot], output_slot=segment_slot)
t1 >> t2 # >> t3 >> t4