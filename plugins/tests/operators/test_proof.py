import unittest
from functools import cached_property

from data.relational_data import TranslationUnitHandler, TranslationUnit

from airflow.models.dag import DAG
from airflow.utils import timezone

from operators.proof import ProofOperator

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "test_proof_dag"


class TestProofOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_execute(self):
        translation_units = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好,世界!"),
            TranslationUnit(id=0, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是气流。欢迎来到翻译工作流程。"),
        ]
        after_translation_units = translation_units.copy()
        for tu in after_translation_units:
            tu.rewrite = tu.translation

        def mock_read_data(_: str, type_name: str):
            if type_name == TranslationUnitHandler.type_name:
                return translation_units
            else:
                return None

        prev_upstream_count = ProofOperator.upstream_count
        ProofOperator.upstream_count = 0

        operator = ProofOperator(task_id='test_proof_task', dag=self.dag)
        operator.read_data = mock_read_data
        operator._post_execute_hook = None
        operator._pre_execute_hook = None

        class MockTask:
            task_id = 1

            @cached_property
            def get_table_suffix(self):
                return ""

        operator.upstream_tasks = [MockTask()]

        def get_direct_relatives(upstream: bool):
            return [MockTask()]

        operator.get_direct_relatives = get_direct_relatives

        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Check outputs
        self.assertEqual(operator.output_data[TranslationUnitHandler.type_name], after_translation_units)
        ProofOperator.upstream_count = prev_upstream_count


if __name__ == '__main__':
    unittest.main()
