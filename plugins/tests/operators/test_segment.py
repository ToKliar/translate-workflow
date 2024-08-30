import os
import unittest

from config import Config
from data.es_data import TextHandler, Text
from operators.segment import SegmentOperator, default_seg_paragraph, default_seg_sentence

from data.relational_data import Document, Paragraph, Sentence, DocumentHandler, ParagraphHandler, SentenceHandler

from airflow.models.dag import DAG
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"


class TestSegmentOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_default_seg_sentence(self):
        result = default_seg_sentence("Sentence 1. Sentence 2.")
        self.assertEqual(result, ['Sentence 1.', 'Sentence 2.'])

    def test_execute(self):
        # Setup configuration and context mocks
        raw = "Hello, world!\nMy name is airflow. Welcome to the translation workflow"
        raw_paragraphs = ["Hello, world!", "My name is airflow. Welcome to the translation workflow"]
        raw_sentences = ["Hello, world!", "My name is airflow.", "Welcome to the translation workflow"]
        input_file_path = Config.get_input_file_path(self.dag.dag_id)
        with open(input_file_path, "w") as f:
            f.write(raw)

        prev_upstream_count = SegmentOperator.upstream_count
        SegmentOperator.upstream_count = 0

        # Instantiate the operator
        operator = SegmentOperator(task_id='test_task', dag=self.dag)

        operator._post_execute_hook = None

        # Execute the operator
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Check outputs
        self.assertIsInstance(operator.output_data[DocumentHandler.type_name][0], Document)
        self.assertIsInstance(operator.output_data[ParagraphHandler.type_name][0], Paragraph)
        self.assertIsInstance(operator.output_data[SentenceHandler.type_name][0], Sentence)
        self.assertIsInstance(operator.output_data[TextHandler.type_name][0], Text)

        texts = operator.output_data[TextHandler.type_name]
        self.assertTrue(len(texts) == 1)
        self.assertTrue(texts[0].content == raw)

        docs = operator.output_data[DocumentHandler.type_name]
        self.assertTrue(len(docs) == 1)
        self.assertTrue(docs[0].path == input_file_path)

        paragraphs = operator.output_data[ParagraphHandler.type_name]
        self.assertEqual(len(paragraphs), len(raw_paragraphs))
        for idx, raw_paragraph in enumerate(raw_paragraphs):
            self.assertEqual(paragraphs[idx].content, raw_paragraph)
            self.assertEqual(paragraphs[idx].doc_id, 0)

        sentences = operator.output_data[SentenceHandler.type_name]
        self.assertEqual(len(sentences), len(raw_sentences))
        for idx, raw_sentence in enumerate(raw_sentences):
            self.assertEqual(sentences[idx].content, raw_sentence)

        self.assertEqual(operator.pipeline, (operator.task_id, ""))

        if os.path.exists(input_file_path):
            os.remove(input_file_path)
        SegmentOperator.upstream_count = prev_upstream_count


if __name__ == '__main__':
    unittest.main()
