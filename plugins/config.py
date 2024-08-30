import os
from enum import Enum

from airflow.exceptions import AirflowException


class FileType(Enum):
    TXT = 1
    PDF = 2
    EPUB = 3
    MDX = 4


def get_file_type(path):
    path = os.path.normpath(path)

    # 获取文件扩展名
    _, ext = os.path.splitext(path)

    # 根据文件扩展名判断文件类型
    if ext.lower() == '.pdf':
        return FileType.PDF
    elif ext.lower() == '.epub':
        return FileType.EPUB
    elif ext.lower() == '.txt':
        return FileType.TXT
    elif ext.lower() == '.mdx':
        return FileType.MDX
    else:
        raise AirflowException(f"Unsupported file type {ext}")


class Config:
    INPUT_FILE_NAME = "input.txt"
    PG_CONN_ID: str = "pg_default"
    INPUT_FILE_TYPE: FileType = FileType.TXT
    TEXT_NAME = "default"
    DEFAULT_DICTIONARY_PATH = "/opt/airflow/plugins/柯林斯高阶英汉词典.mdx"

    @classmethod
    def set_file_type(cls, file_type: FileType):
        cls.INPUT_FILE_TYPE = file_type

    @classmethod
    def set_text_name(cls, text_name: str):
        cls.TEXT_NAME = text_name

    @classmethod
    def get_input_file_path(cls, dag_id: str):
        return "/opt/airflow/" + cls.INPUT_FILE_NAME
        # folder_path = os.path.join(os.getcwd(), dag_id)
        # if not os.path.exists(folder_path):
        #     os.makedirs(folder_path, exist_ok=True)
        # return os.path.join(folder_path, cls.INPUT_FILE_NAME)
