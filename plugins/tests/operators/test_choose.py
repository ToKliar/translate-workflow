import unittest
from functools import cached_property

from data.relational_data import TranslationUnitHandler, TranslationUnit

from airflow.models.dag import DAG
from airflow.utils import timezone

from operators.choose import ChooseOperator

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "test_choose_dag"


class TestChooseOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_execute(self):
        translation_units = [
            [
                TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好,世界!"),
                TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                                paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是气流。欢迎来到翻译工作流程。"),
            ],
            [
                TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0],
                                translation="你好,世界!"),
                TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                                paragraph_id=1, sentence_ids=[1, 2],
                                translation="我的名字是气流。欢迎来到翻译工作流程。"),
            ]
        ]

        after_translation_units = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好,世界!",
                            rank=1, choose_reason="1"),
            TranslationUnit(id=1, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好,世界!",
                            rank=2, choose_reason="2"),
            TranslationUnit(id=2, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是气流。欢迎来到翻译工作流程。",
                            rank=1, choose_reason="1"),
            TranslationUnit(id=3, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是气流。欢迎来到翻译工作流程。",
                            rank=2, choose_reason="2"),
        ]

        def mock_read_data(task_id: str, type_name: str):
            if type_name == TranslationUnitHandler.type_name:
                return translation_units[int(task_id)]
            else:
                return None

        operator = ChooseOperator(task_id='test_translate_task', dag=self.dag)
        operator.read_data = mock_read_data
        operator._post_execute_hook = None
        operator._pre_execute_hook = None

        class MockTask:
            def __init__(self, task_id):
                self.task_id = task_id

            @cached_property
            def get_table_suffix(self):
                return str(self.task_id)

        operator.upstream_tasks = [MockTask('0'), MockTask('1')]

        def get_direct_relatives(upstream: bool):
            return [MockTask('0'), MockTask('1')]

        operator.get_direct_relatives = get_direct_relatives

        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Check outputs
        output_data = operator.output_data[TranslationUnitHandler.type_name]
        output_data.sort(key=lambda x: (x.id, x.rank))
        self.assertEqual(output_data, after_translation_units)


if __name__ == '__main__':
    unittest.main()
