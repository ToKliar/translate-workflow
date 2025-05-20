
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

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Any

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.context import Context

from data.relational_data import WordHandler, TranslationMemoryHandler, TranslationMemory, Word
from operators.basic import BasicOperator

import pandas as pd


class ImportDataOperator(BasicOperator):
    must_output = True
    output_type = [WordHandler.type_name, TranslationMemoryHandler.type_name]

    def __init__(self, *, file_paths: list[str], title: str="", **kwargs) -> None:
        super().__init__(**kwargs)
        self.file_paths = file_paths
        self.title = title
        if self.title == "":
            if self.output_slot.type_name == TranslationMemoryHandler.type_name:
                self.title = "translation memory"
            else:
                self.title = "word"

    def execute(self, context: Context) -> Any:
        data = []
        import_tm = self.output_slot.type_name == TranslationMemoryHandler.type_name
        for file_path in self.file_paths:
            path = os.path.normpath(file_path)

            # 获取文件扩展名
            _, ext = os.path.splitext(path)

            if ext == ".csv":
                df = pd.read_csv(file_path)
                if import_tm:
                    for _, row in df.iterrows():
                        tm = TranslationMemory(
                            src = row['src'],
                            translation = row["translation"],
                        )
                        data.append(tm)
                else:
                    for _, row in df.iterrows():
                        word = Word(
                            word = row['src'],
                            title = self.title,
                            translations = [row["translation"]],
                            examples = [row["example"]]
                        )
                        data.append(word)

            elif ext == ".json":
                with open(file_path, mode="r", encoding="utf-8") as file:
                    json_data = json.load(file)  # 加载 JSON 数据为 Python 对象
                if import_tm:
                    for item in json_data:
                        tm = TranslationMemory(
                            src=item['src'],
                            translation=item["translation"],
                        )
                        data.append(tm)
                else:
                    for item in json_data:
                        word = Word(
                            word=item['src'],
                            title=self.title,
                            translations=[item["translation"]],
                            examples=[item["example"]]
                        )
                        data.append(word)

            else:
                raise AirflowException(f"Unsupported file type: {ext}")
        self.output_slot.set_title(self.title)
        self.output_slot.write_batch(data)
