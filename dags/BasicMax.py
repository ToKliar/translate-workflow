from datetime import datetime, timedelta
from airflow import DAG


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

dag = DAG('basic_max_translate', default_args=default_args, schedule_interval='@once')
paragraph_slot = DataSlot(slot_id=f"basic_max", type_name=ParagraphHandler.type_name)
segment_slot = DataSlot(slot_id=f"basic_max", type_name=TranslationSegmentHandler.type_name)

text_name = "bleak_house"
file_names = []
for i in range(1):
    file_names.append(f"/opt/airflow/dags/data/{text_name}_{i+1}.txt")

# requirements = "Translate a selected passage from Bleak House by Charles Dickens into Chinese, ensuring the translation reflects the stylistic and linguistic characteristics of China's Republican Era (early to mid-20th century). The translation should maintain the literary tone and descriptive richness of the original text while adapting its language to align with the formal, elegant, and classical prose typical of that period. Special attention should be given to preserving cultural nuances, employing archaic or semi-archaic vocabulary where appropriate, and avoiding overly modern expressions. The final output should read as though it were written during the Republican Era, blending seamlessly with the conventions of that time while remaining faithful to the meaning and spirit of the original English text."
requirements = "Translate a selected passage from Bleak House by Charles Dickens into Chinese, adapting the language style to align with the conventions of computer science academic papers. The translation should maintain formal and precise language, avoiding literary embellishments while ensuring clarity and logical coherence. Technical terminology relevant to the computer science domain should be appropriately integrated where context permits, and the tone should remain objective and scholarly throughout. The resulting text should read as if it were part of an academic manuscript, preserving the original meaning while conforming to the stylistic and structural norms of scientific writing in the field of computer science"

t1 = InputOperator(task_id = "1", file_paths=file_names, text_name=text_name, dag=dag, output_slot=paragraph_slot)
# t0 = PythonOperator(task_id = "2", dag=dag, python_callable=end)
# t1 >> t0

t2 = SegmentOperator(task_id = "2", dag=dag, input_slots=[paragraph_slot], output_slot=segment_slot)
t3 = TranslateOperator(task_id="3", dag=dag, input_slots=[segment_slot], requirements=requirements, model="qwen-max-latest", output_slot=segment_slot)
t4 = OutputOperator(task_id = "4", dag=dag, file_path="/opt/airflow/dags/bleak_house_computer_trans.txt", input_slots=[segment_slot])

t1 >> t2 >> t3 >> t4