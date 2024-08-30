import os
import unittest
from functools import cached_property

from config import Config, FileType
from data.relational_data import TranslationUnit

from airflow.models.dag import DAG
from airflow.utils import timezone

from operators.output import OutputOperator

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "test_output_dag"


class TestOutputOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_execute(self):
        raw_translation_units = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0],
                            translation="你好,世界!"),
            TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2],
                            translation="我的名字是气流。欢迎来到翻译工作流程。"),
        ]
        raw_data = '\n'.join([tu.translation for tu in raw_translation_units])

        rewrite_translation_units = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0],
                            translation="你好,世界!", rewrite="尔安，天下！"),
            TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2],
                            translation="我的名字是气流。欢迎来到翻译工作流程。",
                            rewrite="吾名气流，迎尔至翻译之作业流。"),
        ]
        rewrite_data = '\n'.join([tu.rewrite for tu in rewrite_translation_units])

        rank_translation_units = [
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好,世界!",
                            rank=1, choose_reason="1"),
            TranslationUnit(id=0, content="Hello, world!", paragraph_id=0, sentence_ids=[0], translation="你好,世界!",
                            rank=2, choose_reason="2"),
            TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是 airflow。欢迎来到翻译工作流程。",
                            rank=1, choose_reason="1"),
            TranslationUnit(id=1, content="My name is airflow. Welcome to the translation workflow.",
                            paragraph_id=1, sentence_ids=[1, 2], translation="我的名字是气流。欢迎来到翻译工作流程。",
                            rank=2, choose_reason="2"),
        ]
        rank_data = '\n'.join([tu.translation for tu in rank_translation_units if tu.rank == 1])

        Config.set_file_type(FileType.TXT)
        file_path = os.path.join(os.getcwd(), "output.txt")

        def test_output(task_id: str, input_data: list[TranslationUnit], expected_data: str):
            def mock_read_data(_: str, type_name: str):
                return input_data

            operator = OutputOperator(task_id=f'test_output_task_{task_id}', dag=self.dag, file_path=file_path)
            operator.read_data = mock_read_data
            operator._post_execute_hook = None
            operator._pre_execute_hook = None

            class MockTask:
                task_id = 1

                @cached_property
                def get_table_suffix(self):
                    return ""

            def get_direct_relatives(upstream: bool):
                return [MockTask()]

            operator.get_direct_relatives = get_direct_relatives

            operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            self.assertTrue(os.path.exists(file_path))
            with open(file_path, "r") as f:
                output_data = "".join(f.readlines())
            self.assertEqual(output_data, expected_data)
            os.remove(file_path)
        test_output("raw", raw_translation_units, raw_data)
        test_output("rewrite", rewrite_translation_units, rewrite_data)
        test_output("rank", rank_translation_units, rank_data)


if __name__ == '__main__':
    unittest.main()
