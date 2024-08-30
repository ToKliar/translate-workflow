#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# fmt: off
from __future__ import annotations

import os
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.utils.types import ArgNotSet

from config import Config, FileType, get_file_type
from operators.basic import BasicOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FileInputProcessor:
    """
    todo: implement processor of other kinds ("PDF, EPUB", ...)
    """

    def __init__(self, file_path: str):
        self.file_path = file_path

    def process(self):
        raise NotImplementedError

    def store(self, folder_path: str):
        raise NotImplementedError


class TxtInputProcessor(FileInputProcessor):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.text: str = ""
        self.file_path = file_path

    def process(self):
        # may need more preprocess step, here just read and combine
        with open(self.file_path, 'r') as f:
            self.text = "".join(f.readlines())

    def store(self, path: str):
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.text)


class FileProcessorFactory:
    @staticmethod
    def get_processor(file_type: FileType, file_path: str) -> FileInputProcessor:
        if file_type == FileType.TXT:
            return TxtInputProcessor(file_path)
        else:
            raise AirflowException(f"Unsupported file type {file_type}")


class InputOperator(BasicOperator):
    r"""
    input file to dag, beginning of the translation dag

    :param file_path: Path to the input file.
    """
    ui_color = "#e71b64"

    def __init__(self, *, file_path: str | ArgNotSet, text_name: str | ArgNotSet, **kwargs) -> None:
        super().__init__(**kwargs)
        file_type = get_file_type(file_path)
        self.log.info(f"Executing input {file_path}")
        self.processor = FileProcessorFactory.get_processor(file_type, file_path)
        Config.set_text_name(text_name)

    def execute(self, context: Context):
        self.processor.process()
        output_path = Config.get_input_file_path(self.dag_id)
        self.processor.store(output_path)
        with open(output_path, "r", encoding="utf-8") as f:
            txt = "".join(f.readlines())
            self.log.info(f"raw text {txt}")
