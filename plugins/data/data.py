from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Type

from airflow.exceptions import AirflowException

from config import Config
from data import es_data, relational_data
from data.relational_data import PostgresOperations
from data.es_data import ElasticSearchOperations


class DatabaseType(Enum):
    Postgres = 0
    ElasticSearch = 1


class DataInterface:
    type_list: list[Any] = [
        relational_data.DataFactory.data_handler_map.keys(),
        es_data.DataFactory.data_handler_map.keys()
    ]

    pg_operation = PostgresOperations(Config.PG_CONN_ID)
    es_operation = ElasticSearchOperations()

    @classmethod
    def get_dbtype(cls, type_name: str) -> DatabaseType:

        if type_name in cls.type_list[0]:
            return DatabaseType.Postgres
        elif type_name in cls.type_list[1]:
            return DatabaseType.ElasticSearch
        else:
            raise AirflowException(f"type {type_name} not supported")

    @classmethod
    def get_data_type(cls, type_name: str) -> Type[dataclass]:
        if type_name in cls.type_list[0]:
            return relational_data.DataFactory.get_dataclass(type_name)
        elif type_name in cls.type_list[1]:
            return es_data.DataFactory.get_dataclass(type_name)
        else:
            raise AirflowException(f"type {type_name} not supported")

    @classmethod
    def get_operations(cls, type_name: str):
        print(cls.type_list)
        if type_name in cls.type_list[0]:
            return cls.pg_operation
        elif type_name in cls.type_list[1]:
            return cls.es_operation
        else:
            raise AirflowException(f"type {type_name} not supported")

    @classmethod
    def read_data(
            cls,
            type_name: str,
            table_suffix: str,
            query_conditions: Dict[str, Any],
            filter_conditions: Optional[Dict[str, Any]] = None) -> list[dataclass]:

        data_type = cls.get_dbtype(type_name)

        if data_type == DatabaseType.Postgres:
            return cls.pg_operation.retrieve_data(type_name, table_suffix, query_conditions)
        else:
            return cls.es_operation.retrieve_data(type_name, table_suffix, query_conditions, filter_conditions)

    @classmethod
    def create_data(cls, type_name: str, table_suffix: str):
        cls.get_operations(type_name).create_table(type_name, table_suffix)

    @classmethod
    def write_data(cls, type_name: str, table_suffix: str, data: dataclass):
        cls.get_operations(type_name).insert_data(type_name, table_suffix, data)

    @classmethod
    def write_batch(cls, type_name: str, table_suffix: str, batch: list[dataclass]):
        cls.get_operations(type_name).insert_batch(type_name, table_suffix, batch)
