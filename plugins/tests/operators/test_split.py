import unittest

from operators.segment import SegmentOperator

from data.relational_data import Paragraph, Sentence, ParagraphHandler, SentenceHandler, \
    TranslationUnitHandler, TranslationUnit

from airflow.models.dag import DAG
from airflow.utils import timezone

from operators.split import SplitOperator, SplitMethod

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "test_split_dag"


class TestSplitOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_execute(self):
        raw_paragraphs = ["Hello, world! My name is airflow.", "Welcome to the translation workflow"]
        raw_sentences = [["Hello, world!", "My name is airflow."], ["Welcome to the translation workflow"]]
        paragraphs = [
            Paragraph(id=idx, doc_id=0, content=raw_paragraphs[idx], index=idx) for idx in range(len(raw_paragraphs))
        ]
        sentences = []
        for idx, sentence_group in enumerate(raw_sentences):
            for item_idx, item in enumerate(sentence_group):
                sentences.append(
                    Sentence(id=len(sentences), content=item, index=item_idx, paragraph_id=paragraphs[idx].id)
                )

        translation_units = [
            TranslationUnit(id=idx, content=sentences[idx].content, paragraph_id=sentences[idx].paragraph_id,
                            sentence_ids=[sentences[idx].id]) for idx in range(len(sentences))
        ]

        self_translation_units = []
        for paragraph in paragraphs:
            sentence_ids = []
            for sentence in sentences:
                if sentence.paragraph_id == paragraph.id:
                    sentence_ids.append(sentence.id)
            self_translation_units.append(TranslationUnit(
                id=len(self_translation_units), content=paragraph.content,
                sentence_ids=sentence_ids, paragraph_id=paragraph.id)
            )

        def mock_read_data(task_id: str, type_name: str):
            if type_name == ParagraphHandler.type_name:
                return paragraphs
            elif type_name == SentenceHandler.type_name:
                return sentences
            else:
                return None

        def split(strs: list[str]) -> list[list[int]]:
            return [list(range(len(strs)))]

        prev_upstream_count = SplitOperator.upstream_count
        SplitOperator.upstream_count = 0

        prev_operator = SegmentOperator(task_id='test_seg_task', dag=self.dag)
        prev_operator._post_execute_hook = None
        prev_operator._pre_execute_hook = None

        def test_split(split_method: SplitMethod = None, target_data: list[TranslationUnit] = None, task_id: str = ""):
            # Instantiate the operator
            operator = SplitOperator(task_id=f'test_split_task_{task_id}', dag=self.dag, split=split_method)
            operator.read_data = mock_read_data
            operator._post_execute_hook = None
            operator._pre_execute_hook = None

            # Execute the operator
            operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            # Check outputs
            self.assertEqual(operator.output_data[TranslationUnitHandler.type_name], target_data)

        test_split(None, translation_units)
        test_split(split, self_translation_units, "paragraph")
        SplitOperator.upstream_count = prev_upstream_count


if __name__ == '__main__':
    unittest.main()
