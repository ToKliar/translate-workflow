import unittest
from unittest.mock import patch, MagicMock

import elasticsearch
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from elasticsearch import Elasticsearch

from data.es_data import to_document, FixTranslationHandler, FixTranslation, ElasticSearchOperations, TextHandler, Text


class TestGenerateDataclass(unittest.TestCase):
    def test_generate_dataclass(self):
        set_attrs = ['text', 'translation']
        fix_translation = FixTranslation(text="hello", translation="你好")
        self.assertEqual(fix_translation.text, "hello")
        self.assertEqual(fix_translation.translation, "你好")

        expected_fields = FixTranslationHandler.fields_mapping.keys()
        # 验证字段是否存在
        for field in expected_fields:
            self.assertTrue(hasattr(fix_translation, field), f"Missing field: {field}")

        for field, (field_type, _) in FixTranslationHandler.fields_mapping.items():
            if field in set_attrs:
                self.assertIsInstance(getattr(fix_translation, field), field_type,
                                      f"Field {field} is not of type {field_type}")
            else:
                self.assertIsNone(getattr(fix_translation, field), f"Field {field} is not of type {field_type}")


class TestToDocument(unittest.TestCase):
    def test_to_document(self):
        fix_translation = FixTranslation(text="hello", translation="你好")

        doc = to_document(fix_translation)
        self.assertEqual(doc, {"text": "hello", "translation": "你好", "category": None, "source": None})


class TestEsDataHandler(unittest.TestCase):
    def test_get_index_name(self):
        self.assertEqual(FixTranslationHandler.get_type_name("suffix"), "fix_translation_suffix")

    def test_get_index_body(self):
        expected_body = {
            "settings": FixTranslationHandler.index_settings,
            "mappings": {
                "properties": {
                    "text": {"type": "text"},
                    'source': {'type': 'keyword'},
                    'translation': {'type': 'text'},
                    'category': {'type': 'keyword'},
                }
            }
        }
        self.assertEqual(FixTranslationHandler.get_index_body(), expected_body)


class TestElasticSearchOperations(unittest.TestCase):
    table_suffix = "test"
    type_name = FixTranslationHandler.type_name
    index_name = f"{type_name}_{table_suffix}"

    @patch('airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchSQLHook.get_conn')
    def setUp(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        self.es_operations = ElasticSearchOperations()
        self.mock_es_conn = mock_conn
        self.es_operations.es_conn = self.mock_es_conn

    def test_create_index(self):
        self.es_operations.create_table(self.type_name, self.table_suffix)

    def test_insert_data(self):
        fix_translation = FixTranslation(text="hello", translation="你好")
        self.es_operations.insert_data(self.type_name, self.table_suffix, fix_translation)
        self.mock_es_conn.index.assert_called_once_with(index=self.index_name,
                                                        document=to_document(fix_translation))

    @patch('elasticsearch.helpers.bulk')
    def test_insert_batch(self, mock_bulk):
        data_list = [FixTranslation(text="hello", translation="你好"), FixTranslation(text="world", translation="世界")]
        self.es_operations.insert_batch(self.type_name, self.table_suffix, data_list)
        actions = [
            {"_index": self.index_name, "_source": to_document(item)}
            for item in data_list
        ]
        mock_bulk.assert_called_once_with(self.mock_es_conn, actions)

    def test_retrieve_data(self):
        query_conditions = {"match_all": {}}
        self.es_operations.retrieve_data(self.type_name, self.table_suffix, query_conditions)
        self.mock_es_conn.search.assert_called_once()


class TestEsConnection(unittest.TestCase):
    def test_connection(self):
        es_args = {
            "request_timeout": 30,
            "max_retries": 10,
            "retry_on_timeout": True
        }
        es_conn = Elasticsearch(["http://elasticsearch:9200"], **es_args)
        data_handler = TextHandler
        index_body = data_handler.get_index_body()
        index_name = f"{data_handler.type_name}_test"
        result = es_conn.indices.exists(index=index_name)
        if not result:
            es_conn.indices.create(index=index_name, body=index_body, ignore=400)
        exists = es_conn.indices.exists(index=index_name)
        self.assertTrue(exists)

        data = Text(content="raw", name="name", translation_name="", translation="")
        print(to_document(data))
        result = es_conn.index(index=index_name, id="test", document=to_document(data))
        print(result)


if __name__ == '__main__':
    unittest.main()
