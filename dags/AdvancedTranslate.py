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


dag_id = "advanced_translate"

def generate_slot(type_name: str) -> DataSlot:
    return DataSlot(slot_id=f"{dag_id}", type_name=type_name)

dag = DAG(dag_id, default_args=default_args, schedule_interval='@once')

pargraph_slot = generate_slot(ParagraphHandler.type_name)
segment_slot = generate_slot(TranslationSegmentHandler.type_name)
word_slot = generate_slot(WordHandler.type_name)
translation_memory_slot = generate_slot(TranslationMemoryHandler.type_name)
match_slot = generate_slot(MatchHandler.type_name)


file_names = [f"/opt/airflow/dags/data/financial.txt"]

text_name = "financial"

t1 = InputOperator(task_id = "1", file_paths=file_names, text_name=text_name, dag=dag, output_slot=pargraph_slot)
t2 = SegmentOperator(task_id = "2", dag=dag, input_slots=[pargraph_slot], output_slot=segment_slot)
t3 = ExtractOperator(task_id = "3", dag=dag, title="term", input_slots=[segment_slot], output_slot=word_slot)
t4 = WordTranslateOperator(task_id = "4", dag=dag, input_slots=[word_slot], output_slot=word_slot)
t5 = IntroduceOperator(task_id = "5", dag=dag, file_paths=["/opt/airflow/dags/tm.json"], title="translation memory", output_slot=translation_memory_slot)
t6 = RetrieveOperator(task_id = "6", dag=dag, input_slots=[word_slot, segment_slot, translation_memory_slot], output_slot=match_slot)
t7 = TranslateOperator(task_id = "7", dag=dag, input_slots=[segment_slot, match_slot], output_slot=segment_slot)
t9 = BackTranslationOperator(task_id = "8", dag=dag, input_slots=[segment_slot], output_slot=segment_slot)
t8 = OutputOperator(task_id = "9", dag=dag, file_path="/opt/airflow/dags/financial_advanced.txt", input_slots=[segment_slot])

t1 >> t2 >> t3 >> t4 >> t6 >> t7 >> t9 >> t8
t5 >> t6