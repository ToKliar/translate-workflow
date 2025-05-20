from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from data.relational_data import ParagraphHandler, TranslationSegmentHandler
from operators.input import InputOperator
from operators.segment import SegmentOperator, SegmentConfig
from operators.translate import TranslateOperator
from operators.output import OutputOperator
from data.slot import DataSlot

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

paragraph_slot = DataSlot(slot_id=f"basic", type_name=ParagraphHandler.type_name)
segment_slot = DataSlot(slot_id=f"basic", type_name=TranslationSegmentHandler.type_name)

def end():
    data = paragraph_slot.read_data()
    print(f"length data from table: {len(data)}")

file_names = [f"/opt/airflow/dags/data/financial.txt"]

text_name = "financial"

t1 = InputOperator(task_id = "1", file_paths=file_names, text_name=text_name, dag=dag, output_slot=paragraph_slot)
# t0 = PythonOperator(task_id = "2", dag=dag, python_callable=end)
# t1 >> t0

t2 = SegmentOperator(task_id = "2", dag=dag, input_slots=[paragraph_slot], output_slot=segment_slot)
t3 = TranslateOperator(task_id="3", dag=dag, input_slots=[segment_slot], model="qwen-plus-latest", output_slot=segment_slot)
t4 = OutputOperator(task_id = "4", dag=dag, file_path="/opt/airflow/dags/financial_basic_trans.txt", input_slots=[segment_slot])

t1 >> t2 >> t3 >> t4
