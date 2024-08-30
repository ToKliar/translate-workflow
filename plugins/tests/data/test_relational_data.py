import unittest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import ARRAY

from data.relational_data import TranslationUnit, TranslationUnitHandler, DataFactory, PostgresOperations

mock_task_id = "mock"


class TestDataClassAndORMModel(unittest.TestCase):
    def test_generate_dataclass(self):
        set_attrs = ['content', 'translation', 'sentence_ids', 'paragraph_id']
        translation_unit = TranslationUnit(content="hello", translation="你好", sentence_ids=[1, 2, 3], paragraph_id=1)
        self.assertEqual(translation_unit.content, "hello")
        self.assertEqual(translation_unit.translation, "你好")
        self.assertEqual(translation_unit.sentence_ids, [1, 2, 3])
        self.assertEqual(translation_unit.paragraph_id, 1)

        expected_fields = TranslationUnitHandler.fields_mapping.keys()
        # 验证字段是否存在
        for field in expected_fields:
            self.assertTrue(hasattr(translation_unit, field), f"Missing field: {field}")

        for field, (field_type, _) in TranslationUnitHandler.fields_mapping.items():
            if field in set_attrs:
                self.assertIsInstance(getattr(translation_unit, field), field_type,
                                      f"Field {field} is not of type {field_type}")
            else:
                self.assertIsNone(getattr(translation_unit, field), f"Field {field} is not of type {field_type}")

    def test_generate_orm_model(self):
        orm_model = TranslationUnitHandler.initialize_orm_model(mock_task_id)
        self.assertTrue(hasattr(orm_model, '__tablename__'))
        self.assertEqual(orm_model.__tablename__, f"{TranslationUnitHandler.type_name}_{mock_task_id}")

        # 验证字段是否存在并且类型正确
        for field, (_, column_type) in TranslationUnitHandler.fields_mapping.items():
            self.assertTrue(hasattr(orm_model, field), f"Missing field: {field}")
            column = getattr(orm_model, field).property.columns[0]
            self.assertTrue(isinstance(column, Column), f"Field {field} is not a Column")
            if isinstance(column_type, ARRAY):
                self.assertTrue(isinstance(column.type, ARRAY), f"Field {field} column type is not ARRAY")
                # 进一步检查数组中元素的类型
                self.assertTrue(isinstance(column.type.item_type, column_type.item_type.__class__),
                                f"Field {field} array item type is not {column_type.item_type}")
            else:
                self.assertTrue(isinstance(column.type, column_type), f"Field {field} column type is not {column_type}")


class TestDataFactory(unittest.TestCase):
    test_type = TranslationUnitHandler.type_name
    failed_type = "mock"

    def test_get_dataclass(self):
        # 测试获取dataclass
        translation_unit_class = DataFactory.get_dataclass(self.test_type)
        self.assertEqual(translation_unit_class, TranslationUnit)

    def test_get_handler(self):
        # 测试获取handler
        translation_unit_handler = DataFactory.get_handler(self.test_type)
        self.assertEqual(translation_unit_handler, TranslationUnitHandler)

    def test_get_invalid_dataclass(self):
        with self.assertRaises(AirflowException) as context:
            DataFactory.get_dataclass(self.failed_type)
        self.assertEqual(f'Data type {self.failed_type} not supported', str(context.exception))

    def test_get_invalid_handler(self):
        with self.assertRaises(AirflowException) as context:
            DataFactory.get_handler(self.failed_type)
        self.assertEqual(f'Data type {self.failed_type} not supported', str(context.exception))


class TestPostgresOperations(unittest.TestCase):
    table_suffix = "test"
    type_name = TranslationUnitHandler.type_name

    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_connection')
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine')
    def setUp(self, mock_get_sqlalchemy_engine, mock_get_connection):
        # 模拟 get_connection 方法
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # 模拟 get_sqlalchemy_engine 方法
        mock_engine = MagicMock()
        mock_get_sqlalchemy_engine.return_value = mock_engine
        self.postgres_operations = PostgresOperations("test_conn_id")

    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_connection')
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine')
    def test_create_table(self, mock_get_sqlalchemy_engine, mock_get_connection):
        # 测试创建表
        # 模拟 get_sqlalchemy_engine 方法返回值
        mock_engine = MagicMock()
        mock_get_sqlalchemy_engine.return_value = mock_engine
        self.postgres_operations.hook.get_sqlalchemy_engine = mock_get_sqlalchemy_engine

        # 模拟 get_connection 方法返回值
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        self.postgres_operations.hook.get_connection = mock_get_connection

        # 执行测试的方法
        self.postgres_operations.create_table(self.type_name, self.table_suffix)

        # 断言：确保 get_sqlalchemy_engine 被调用
        mock_get_sqlalchemy_engine.assert_called_once()

    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_connection')
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine')
    def test_insert_data(self, mock_get_sqlalchemy_engine, mock_get_connection):
        # 测试插入数据
        mock_session = MagicMock()
        self.postgres_operations.Session = MagicMock(return_value=mock_session)

        mock_engine = MagicMock()
        mock_get_sqlalchemy_engine.return_value = mock_engine
        self.postgres_operations.hook.get_sqlalchemy_engine = mock_get_sqlalchemy_engine

        # 模拟 get_connection 方法返回值
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        self.postgres_operations.hook.get_connection = mock_get_connection

        translation_unit = TranslationUnit(content="hello", translation="你好", sentence_ids=[1, 2, 3], paragraph_id=1)
        self.postgres_operations.insert_data(self.type_name, self.table_suffix, translation_unit)
        mock_session.add.assert_called()
        mock_session.commit.assert_called()

    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_connection')
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine')
    def test_insert_batch(self, mock_get_sqlalchemy_engine, mock_get_connection):
        mock_session = MagicMock()
        self.postgres_operations.Session = MagicMock(return_value=mock_session)

        mock_engine = MagicMock()
        mock_get_sqlalchemy_engine.return_value = mock_engine
        self.postgres_operations.hook.get_sqlalchemy_engine = mock_get_sqlalchemy_engine

        # 模拟 get_connection 方法返回值
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        self.postgres_operations.hook.get_connection = mock_get_connection

        translation_units = [
            TranslationUnit(content="hello", translation="你好", sentence_ids=[1, 2, 3], paragraph_id=1),
            TranslationUnit(content="world", translation="世界", sentence_ids=[4, 5], paragraph_id=2)
        ]

        self.postgres_operations.insert_batch(self.type_name, self.table_suffix, translation_units)
        self.assertEqual(mock_session.bulk_save_objects.call_count, 1)
        mock_session.commit.assert_called()

    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_connection')
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine')
    def test_retrieve_data(self, mock_get_sqlalchemy_engine, mock_get_connection):
        # 测试检索数据
        mock_session = MagicMock()
        self.postgres_operations.Session = MagicMock(return_value=mock_session)

        mock_engine = MagicMock()
        mock_get_sqlalchemy_engine.return_value = mock_engine
        self.postgres_operations.hook.get_sqlalchemy_engine = mock_get_sqlalchemy_engine

        # 模拟 get_connection 方法返回值
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        self.postgres_operations.hook.get_connection = mock_get_connection

        query_conditions = {"content": "hello"}

        self.postgres_operations.retrieve_data(self.type_name, self.table_suffix, query_conditions)
        mock_session.query.assert_called()
        mock_session.query.return_value.filter_by.assert_called_with(content="hello")

    def tearDown(self):
        self.postgres_operations.Session = None
        self.postgres_operations.drop_table(self.type_name, self.table_suffix)


if __name__ == '__main__':
    unittest.main()
