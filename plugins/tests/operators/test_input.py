import os
import unittest

from airflow.models.dag import DAG
from airflow.utils import timezone

from config import Config
from operators.input import InputOperator

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "test_input_dag"


class TestInputOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_execute(self):
        src_file_path = os.path.join(os.getcwd(), "story.txt")
        src_data = '''The City Mouse and the Country Mouse

Once there were two mice. They were friends. One mouse lived in the country; the other mouse lived in the city. After many years the Country mouse saw the City mouse; he said, "Do come and see me at my house in the country." So the City mouse went. The City mouse said. This food is not good, and your house is not good. Why do you live in a hole in the field? You should come and live in the city. You would live in a nice house made of stone. You would have nice food to eat. You must come and see me at my house in the city.

The Country mouse went to the house of the City mouse. It was a very good house. Nice food was set ready for them to eat. But just as they began to eat they heard a great noise. The City mouse cried, " Run! Run! The cat is coming!" They ran away quickly and hid.

After some time they came out. When they came out, the Country mouse said, I do not like living in the city. I like living in my hole in the field. For it is nicer to be poor and happy, than to be rich and afraid.'''

        with open(src_file_path, "w") as f:
            f.write(src_data)

        operator = InputOperator(task_id=f'test_input_task', dag=self.dag, file_path=src_file_path, text_name="story")
        operator._pre_execute_hook = None

        input_file_path = os.path.join(os.getcwd(), "target.txt")

        def get_input_file_path(_: str):
            return input_file_path

        Config.get_input_file_path = get_input_file_path
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        self.assertEqual(Config.TEXT_NAME, "story")
        self.assertTrue(os.path.exists(input_file_path))
        with open(input_file_path, "r") as f:
            target_data = "".join(f.readlines())
            self.assertEqual(target_data, src_data)

        os.remove(input_file_path)
        os.remove(src_file_path)
