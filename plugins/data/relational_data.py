import uuid
from dataclasses import dataclass, field, asdict, fields
from typing import Any, Dict, Type

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


# 动态生成 dataclass
def generate_dataclass(fields: Dict[str, Any], type_name: str) -> Any:
    class_dict = {"id": field(default=None, metadata={"sa": Integer})}
    annotations = {"id": int}

    for k, v in fields.items():
        # 假设v是一个元组，v[0]是类型，v[1]是SQLAlchemy列对象
        annotations[k] = v[0]
        class_dict[k] = field(default=None, metadata={"sa": v[1]})

    # 设置类型注解
    class_dict['__annotations__'] = annotations

    # 使用type创建类
    data_type = type(type_name, (object,), class_dict)

    # 应用dataclass装饰器
    return dataclass(data_type)


# 动态生成 ORM 模型
def generate_orm_model(fields: Dict[str, Any], type_name: str, table_suffix: str) -> Any:
    table_name = f'{type_name}_{table_suffix}'
    attrs = {
        '__tablename__': table_name,
        '__table_args__': {'extend_existing': True},
        'id': Column(Integer, primary_key=True)
    }
    attrs.update({k: Column(v[1]) for k, v in fields.items()})
    return type(table_name, (Base,), attrs)


class DataHandler:
    fields_mapping: Dict[str, Any] = ()
    type_name: str = ''

    @classmethod
    def initialize_dataclass(cls) -> Any:
        return generate_dataclass(cls.fields_mapping, cls.type_name)

    @classmethod
    def initialize_orm_model(cls, table_suffix: str) -> Any:
        return generate_orm_model(cls.fields_mapping, cls.type_name, table_suffix)


class WordHandler(DataHandler):
    fields_mapping = {
        'content': (str, String),
        'translation': (str, String),
        'explanation': (str, String),
        'type': (str, String),
        'sentence_id': (int, Integer),
        'tu_id': (int, Integer),
        'doc_id': (int, Integer),
    }
    type_name = 'word'


class SentenceHandler(DataHandler):
    fields_mapping = {
        'paragraph_id': (int, Integer),
        'content': (str, String),
        'index': (int, Integer),
        'is_top': (bool, Boolean),
        'is_long_complex': (bool, Boolean),
    }
    type_name = 'sentence'


class ParagraphHandler(DataHandler):
    fields_mapping = {
        'doc_id': (int, Integer),
        'content': (str, String),
        'index': (int, Integer),
    }
    type_name = 'paragraph'


class DocumentHandler(DataHandler):
    fields_mapping = {
        'uuid': (str, UUID),
        'name': (str, String(255)),
        'path': (str, String),
        'es_index': (str, String),
        'category': (str, String(100)),
        'summary': (str, String),
    }
    type_name = 'doc'


class TranslationUnitHandler(DataHandler):
    fields_mapping = {
        'content': (str, String),
        'sentence_ids': (list, ARRAY(Integer)),
        'paragraph_id': (int, Integer),
        'translation': (str, String),
        'rewrite': (str, String),
        'choose_reason': (str, String),
        'rank': (int, Integer)
    }
    type_name = 'translation_unit'


Word = WordHandler.initialize_dataclass()
Sentence = SentenceHandler.initialize_dataclass()
Paragraph = ParagraphHandler.initialize_dataclass()
Document = DocumentHandler.initialize_dataclass()
TranslationUnit = TranslationUnitHandler.initialize_dataclass()


class DataFactory:
    data_handler_map: Dict[str, DataHandler] = {
        WordHandler.type_name: WordHandler,
        SentenceHandler.type_name: SentenceHandler,
        ParagraphHandler.type_name: ParagraphHandler,
        DocumentHandler.type_name: DocumentHandler,
        TranslationUnitHandler.type_name: TranslationUnitHandler,
    }

    dataclass_map: Dict[str, dataclass] = {
        WordHandler.type_name: Word,
        SentenceHandler.type_name: Sentence,
        ParagraphHandler.type_name: Paragraph,
        DocumentHandler.type_name: Document,
        TranslationUnitHandler.type_name: TranslationUnit,
    }

    @classmethod
    def get_dataclass(cls, data_type: str):
        if data_type in cls.dataclass_map.keys():
            return cls.dataclass_map[data_type]
        raise AirflowException(f'Data type {data_type} not supported')

    @classmethod
    def get_handler(cls, data_type: str):
        if data_type in cls.data_handler_map.keys():
            return cls.data_handler_map[data_type]
        raise AirflowException(f'Data type {data_type} not supported')

    @staticmethod
    def get_data(data_type: Type[dataclass], raw_data: dict) -> dataclass:
        data_fields = fields(data_type)
        data_dict = {}
        for data_field in data_fields:
            data_dict[data_field.name] = raw_data[data_field.name]
        return data_type(**data_dict)

class OrmModelCache:
    orm_model_catch: dict[str, Type[Base]] = {}

    @classmethod
    def get_orm_model(cls, type_name: str, table_suffix: str) -> Type[Base]:
        orm_model_name = f'{type_name}_{table_suffix}'
        if orm_model_name not in cls.orm_model_catch:
            handler = DataFactory.get_handler(type_name)
            cls.orm_model_catch[orm_model_name] = handler.initialize_orm_model(table_suffix)
        return cls.orm_model_catch[orm_model_name]


class PostgresOperations:
    def __init__(self, conn_id: str):
        """
        初始化数据库操作类
        :param conn_id: Airflow 中定义的 Postgres 连接 ID
        """
        self.conn_id = conn_id
        self.hook = PostgresHook(self.conn_id)
        engine = self.hook.get_sqlalchemy_engine()
        self.Session = sessionmaker(bind=engine)

    @staticmethod
    def get_model(type_name: str, table_suffix: str) -> Type[Base]:
        return OrmModelCache.get_orm_model(type_name, table_suffix)

    def create_table(self, type_name: str, table_suffix: str):
        engine = self.hook.get_sqlalchemy_engine()
        model: Type[Base] = self.get_model(type_name, table_suffix)
        model.metadata.create_all(bind=engine)
        session = self.Session()
        session.query(model).delete()
        session.commit()

    @staticmethod
    def judge_data_type(type_name: str, data: dataclass):
        data_type = DataFactory.get_dataclass(type_name)
        if not isinstance(data, data_type):
            raise AirflowException(f'specified type {data_type} and type of data {type(data)} not match')

    def insert_data(self, type_name: str, table_suffix: str, data: dataclass):
        self.judge_data_type(type_name, data)
        session = self.Session()
        model: Type[Base] = self.get_model(type_name, table_suffix)
        try:
            record = model(**asdict(data))
            session.add(record)
            session.commit()
        except Exception as e:
            session.rollback()
            raise AirflowException(f"insert data to table {model.__tablename__} failed due to {e}")
        finally:
            session.close()

    def insert_batch(self, type_name: str, table_suffix: str, batch: list[dataclass]):
        if len(batch) == 0:
            return
        self.judge_data_type(type_name, batch[0])
        session = self.Session()
        model: Type[Base] = self.get_model(type_name, table_suffix)
        try:
            records = [model(**asdict(data)) for data in batch]
            session.bulk_save_objects(records)
            session.commit()
        except Exception as e:
            session.rollback()
            raise AirflowException(f"insert batch to table {model.__tablename__} failed due to {e}")
        finally:
            session.close()

    def retrieve_data(self, type_name: str, table_suffix: str, query_conditions: Dict[str, Any] = None) \
            -> list[dataclass]:
        session = self.Session()
        model: Type[Base] = self.get_model(type_name, table_suffix)
        data_type = DataFactory.get_dataclass(type_name)
        try:
            if query_conditions is not None:
                result = session.query(model).filter_by(**query_conditions).all()
            else:
                result = session.query(model).all()
            return [DataFactory.get_data(data_type, record.__dict__) for record in result]
        except Exception as e:
            raise AirflowException(f"insert data to table {model.__tablename__} failed due to {e}")
        finally:
            session.close()

    def drop_table(self, type_name: str, table_suffix: str):
        engine = self.hook.get_sqlalchemy_engine()
        model: Type[Base] = self.get_model(type_name, table_suffix)
        model.metadata.drop_all(bind=engine)
