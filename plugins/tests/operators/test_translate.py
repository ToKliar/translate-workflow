import unittest
from functools import cached_property

from data.relational_data import TranslationUnitHandler, TranslationUnit

from airflow.models.dag import DAG
from airflow.utils import timezone

from operators.translate import TranslateOperator, LLMTranslateOperator, MachineTranslateOperator

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "test_translate_dag"


class TestTranslateOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_execute(self):
        translation_units = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0]),
            TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2]),
        ]

        machine_target_data = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好,世界!"),
            TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是气流。欢迎来到翻译工作流程。"),
        ]

        llm_target_data = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好，世界！"),
            TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是气流。歡迎來到翻譯工作流程。"),
        ]

        def mock_read_data(_: str, type_name: str):
            if type_name == TranslationUnitHandler.type_name:
                return translation_units
            else:
                return None

        prev_upstream_count = TranslateOperator.upstream_count
        TranslateOperator.upstream_count = 0

        class MockTask:
            task_id = 1
            output_type: set[str] = (TranslationUnitHandler.type_name)

            @cached_property
            def get_table_suffix(self):
                return ""

        def get_direct_relatives(upstream: bool):
            return [MockTask()]

        def test_translate(kind: str, target_data: list[TranslationUnit]):
            if kind == "Machine":
                operator = MachineTranslateOperator(task_id=f'test_{kind}_translate_task', dag=self.dag)
            else:
                operator = LLMTranslateOperator(task_id=f'test_{kind}_translate_task', dag=self.dag)
            operator.read_data = mock_read_data
            operator._post_execute_hook = None
            operator._pre_execute_hook = None
            operator.upstream_tasks = [MockTask()]
            operator.get_direct_relatives = get_direct_relatives
            operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            # Check outputs
            self.assertEqual(operator.output_data[TranslationUnitHandler.type_name], target_data)
            TranslateOperator.upstream_count = prev_upstream_count
        test_translate("Machine", machine_target_data)
        test_translate("LLM", llm_target_data)


if __name__ == '__main__':
    unittest.main()
