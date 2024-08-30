from dataclasses import dataclass, asdict, field
from typing import Any, Dict, Type, Optional

from airflow.exceptions import AirflowException
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from elasticsearch import helpers

import elasticsearch

def generate_dataclass(fields: Dict[str, Any], type_name: str) -> Any:
    class_dict = {}
    annotations = {}

    for k, v in fields.items():
        # 假设v是一个元组，v[0]是类型，v[1]是SQLAlchemy列对象
        annotations[k] = v[0]
        class_dict[k] = field(default=None)

    # 设置类型注解
    class_dict['__annotations__'] = annotations

    # 使用type创建类
    data_type = type(type_name, (object,), class_dict)

    # 应用dataclass装饰器
    return dataclass(data_type)


def to_document(doc: dataclass) -> Dict[str, Any]:
    return asdict(doc)


class EsDataHandler:
    fields_mapping: Dict[str, Any] = None
    index_settings: Dict[str, Any] = {
        "analysis": {
            "analyzer": {
                "ik_smart_analyzer": {
                    "type": "custom",
                    "tokenizer": "ik_smart"
                }
            }
        }
    }
    type_name: str = ""

    @classmethod
    def get_type_name(cls, table_suffix: str) -> str:
        return f"{cls.type_name}_{table_suffix}"

    @classmethod
    def get_index_body(cls) -> Dict[str, Any]:
        es_mapping = {"properties": {}}
        for field_name, (field_type, es_info) in cls.fields_mapping.items():
            # 更新类属性以仅包含数据类型信息
            cls.__annotations__[field_name] = field_type
            # 构建ES映射
            es_mapping["properties"][field_name] = es_info

        es_index_body = {"settings": cls.index_settings, "mappings": es_mapping}
        return es_index_body

    @classmethod
    def initialize_dataclass(cls) -> Any:
        return generate_dataclass(cls.fields_mapping, cls.type_name)


class FixTranslationHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "text"}
        ),
        "category": (
            str,
            {"type": "keyword"}
        ),
        "source": (
            str,
            {"type": "keyword"}
        )
    }
    type_name: str = "fix_translation"


class WordDictionaryHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "keyword"}
        ),
        "definition": (
            str,
            {"type": "text"}
        ),
        "target_definition": (
            str,
            {"type": "text"}
        ),
        "category": (
            str,
            {"type": "keyword"}
        ),
        "target_category": (
            str,
            {"type": "keyword"}
        ),
        "examples": (
            list[str],
            {"type": "text"}
        ),
        "target_examples": (
            list[str],
            {"type": "text"}
        )
    }
    type_name: str = "word_dictionary"


class TextHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "content": (
            str,
            {"type": "text"}
        ),
        "name": (
            str,
            {"type": "keyword"}
        ),
        "translation_name": (
            str,
            {"type": "keyword"}
        ),
        "translation": (
            str,
            {"type": "text", "analyzer": "ik_smart_analyzer"}
        )
    }
    type_name: str = "text"


class StoreSentenceHandler:
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "keyword", "analyzer": "ik_smart_analyzer"}
        ),
        "rewrite": (
            str,
            {"type": "keyword", "analyzer": "ik_smart_analyzer"}
        ),
        "final": (
            str,
            {"type": "keyword", "analyzer": "ik_smart_analyzer"}
        ),
        "style": (
            str,
            {"type": "keyword"}
        ),
        "sentence_id": {
            "type": "integer"
        },
        "doc_id": {
            "type": "integer"
        }
    }
    type_name: str = "sentence"


class TermHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "keyword", "analyzer": "ik_smart_analyzer"}
        ),
        "category": (
            str,
            {"type": "keyword"}
        ),
        "explanation": (
            str,
            {"type": "text"}
        ),
    }
    type_name: str = "term"


class SlangHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "text"}
        ),
        "definition": (
            str,
            {"type": "text"}
        ),
        "category": (
            str,
            {"type": "keyword"}
        )
    }
    type_name: str = "slang"


class ProverbHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "text"}
        )
    }
    type_name: str = "proverb"


class IdiomHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "text"}
        ),
        "meaning": (
            str,
            {"type": "text"}
        ),
        "example": (
            str,
            {"type": "text"}
        ),
        "translation_example": (
            str,
            {"type": "text"}
        )
    }
    type_name: str = "idiom"


class NameHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "text"}
        ),
        "country": (
            str,
            {"type": "keyword"}
        )
    }
    type_name: str = "name"


class LocationHandler(EsDataHandler):
    fields_mapping: Dict[str, Any] = {
        "text": (
            str,
            {"type": "text"}
        ),
        "translation": (
            str,
            {"type": "keyword", "analyzer": "ik_smart_analyzer"}
        ),
        "country": (
            str,
            {"type": "keyword"}
        )
    }
    type_name: str = "location"


FixTranslation = FixTranslationHandler.initialize_dataclass()
Text = TextHandler.initialize_dataclass()
Slang = SlangHandler.initialize_dataclass()
Proverb = ProverbHandler.initialize_dataclass()
Idiom = IdiomHandler.initialize_dataclass()
Name = NameHandler.initialize_dataclass()
Location = LocationHandler.initialize_dataclass()
WordDictionary = WordDictionaryHandler.initialize_dataclass()


class DataFactory:
    data_handler_map: Dict[str, Type[EsDataHandler]] = {
        FixTranslationHandler.type_name: FixTranslationHandler,
        TextHandler.type_name: TextHandler,
        SlangHandler.type_name: SlangHandler,
        ProverbHandler.type_name: ProverbHandler,
        IdiomHandler.type_name: IdiomHandler,
        NameHandler.type_name: NameHandler,
        LocationHandler.type_name: LocationHandler,
        WordDictionaryHandler.type_name: WordDictionaryHandler
    }

    dataclass_map: Dict[str, Type[dataclass]] = {
        FixTranslationHandler.type_name: FixTranslation,
        TextHandler.type_name: Text,
        SlangHandler.type_name: Slang,
        ProverbHandler.type_name: Proverb,
        IdiomHandler.type_name: Idiom,
        NameHandler.type_name: Name,
        LocationHandler.type_name: Location,
        WordDictionaryHandler.type_name: WordDictionary
    }

    @classmethod
    def get_data_handler(cls, data_type: str) -> Type[EsDataHandler]:
        if data_type in cls.data_handler_map.keys():
            return cls.data_handler_map[data_type]
        raise AirflowException(f'Data type {data_type} not supported')

    @classmethod
    def get_dataclass(cls, data_type: str) -> dataclass:
        if data_type in cls.dataclass_map.keys():
            return cls.dataclass_map[data_type]
        raise AirflowException(f'Data type {data_type} not supported')


class ElasticSearchOperations:
    def __init__(self):
        es_args = {
            "timeout": 30,
            "max_retries": 10,
            "retry_on_timeout": True
        }
        self.hook = ElasticsearchPythonHook(
            hosts=["http://elasticsearch:9200"],
            es_conn_args=es_args
        )
        self.es_conn = self.hook.get_conn

    @staticmethod
    def judge_data_type(type_name: str, data: dataclass):
        data_type = DataFactory.get_dataclass(type_name)
        if not isinstance(data, data_type):
            raise AirflowException(f'specified type {data_type} and type of data {type(data)} not match')

    @staticmethod
    def get_data_handler(type_name: str):
        return DataFactory.get_data_handler(type_name)

    @staticmethod
    def get_index_name(type_name: str, table_suffix: str):
        data_handler = DataFactory.get_data_handler(type_name)
        return data_handler.get_type_name(table_suffix)

    def create_table(self, type_name: str, table_suffix: str):
        data_handler = self.get_data_handler(type_name)
        index_body = data_handler.get_index_body()
        index_name = data_handler.get_type_name(table_suffix)
        try:
            if not self.es_conn.indices.exists(index=index_name):
                result = self.es_conn.indices.create(index=index_name, body=index_body, ignore=400)
                print(result)
        except Exception as e:
            raise AirflowException(f"create es index {index_name} with mapping {index_body} failed for {e}")

    def insert_data(self, type_name: str, table_suffix: str, data: dataclass):
        self.judge_data_type(type_name, data)
        index_name = self.get_index_name(type_name, table_suffix)
        try:
            result = self.es_conn.index(index=index_name, document=to_document(data))
        except Exception as e:
            raise AirflowException(f"insert single data to es index {index_name} failed for {e}")

    def insert_batch(self, type_name: str, table_suffix: str, batch: list[dataclass]):
        if len(batch) == 0:
            return
        self.judge_data_type(type_name, batch[0])
        index_name = self.get_index_name(type_name, table_suffix)
        try:
            actions = [
                {"_index": index_name, "_source": to_document(item)}
                for item in batch
            ]
            helpers.bulk(self.es_conn, actions)
        except Exception as e:
            raise AirflowException(f"insert batch data to es index {index_name} failed for {e}")

    def retrieve_data(self,
                      type_name: str,
                      table_suffix: str,
                      query_conditions: Dict[str, Any] = None,
                      filter_conditions: Optional[Dict[str, Any]] = None):
        """
        :param type_name: 类型名
        :param table_suffix: 表后缀
        :param query_conditions: 查询条件
        :param filter_conditions: 过滤条件（可选）
        :return: 检索结果列表
        """

        query = {
            "query": {
                "bool": {
                    "must": query_conditions
                }
            }
        }

        # 添加过滤条件
        if filter_conditions:
            query["query"]["bool"]["filter"] = filter_conditions

        index_name = self.get_index_name(type_name, table_suffix)
        data_type = DataFactory.get_dataclass(type_name)
        try:
            response = self.es_conn.search(index=index_name, body=query)
            hits = response['hits']['hits']
            return [data_type(**hit['_source']) for hit in hits]
        except Exception as e:
            raise AirflowException(f"retrieve from es index {index_name} failed for {e}")
