import unittest

from data.es_data import WordDictionaryHandler, WordDictionary

from airflow.models.dag import DAG
from airflow.utils import timezone

from operators.dictionary import ImportDictionaryOperator

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "test_dictionary_dag"


class TestImportDictionaryOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_execute(self):
        after_data: list[WordDictionary] = [
            WordDictionary(
                text='24-7',
                definition='连续发生地;不中断地;无休止地',
                target_definition='If something happens 24-7 , it happens all the time without ever stopping. 24-7 '
                                  'means twenty-four hours a day, seven days a week.',
                category='ADV',
                target_category='副词',
                examples=['I feel like sleeping 24-7.', '...a 24-7 radio station.', '...a 24-7 radio station.'],
                target_examples=['我想一直睡下去。', '24 小时广播电台', '24 小时广播电台']),
            WordDictionary(
                text='911',
                definition='(美国的紧急求助电话号码)',
                target_definition=' 911 is the number that you call in the United States in order to contact the '
                                  'emergency services.',
                category='NUM',
                target_category='数词',
                examples=['The women made their first 911 call about a prowler at 12:46 a.m.'],
                target_examples=['那些女人在上午12 点 46分第一次打911报警说有小偷。']),
            WordDictionary(
                text='999',
                definition='(英国的紧急求助电话号码)',
                target_definition=' 999 is the number that you call in Britain in order to contact the emergency '
                                  'services.',
                category='NUM',
                target_category='数词',
                examples=['...a fire engine answering a 999 call...', 'She dialled 999 on her mobile.'],
                target_examples=['应999紧急呼叫出动的救火车', '她用手机拨打了999。'])]
        operator = ImportDictionaryOperator(task_id='test_import_dictionary_operator', dag=self.dag)
        operator._post_execute_hook = None
        operator._pre_execute_hook = None

        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Check outputs
        output_data = operator.output_data[WordDictionaryHandler.type_name]
        self.assertEqual(len(output_data), 33426)
        output_data = output_data[:len(after_data)]
        self.assertEqual(output_data, after_data)



if __name__ == '__main__':
    unittest.main()