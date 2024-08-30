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

from typing import TYPE_CHECKING

from config import Config
from data.relational_data import TranslationUnitHandler, TranslationUnit
from operators.basic import BasicOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OutputOperator(BasicOperator):
    ui_color = "#e71b64"
    input_type = [TranslationUnitHandler.type_name]
    output_type = input_type

    def __init__(self, *, file_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.file_type = Config.INPUT_FILE_TYPE
        self.file_path = file_path

    def add_format(self):
        raise NotImplementedError

    def execute(self, context: Context):
        upstream_tasks = self.get_direct_relatives(True)

        for upstream_task in upstream_tasks:
            translation_units = self.read_data(upstream_task.get_table_suffix, TranslationUnitHandler.type_name)
            output = self.merge(translation_units)
            self.log.info("after merge:\n" + output)

            with open(f"{self.file_path}", "w") as output_file:
                output_file.write(output)
            break

    @staticmethod
    def merge(translation_units: list[TranslationUnit]) -> str:
        """
        just for sentence merge
        """
        if len(translation_units) == 0:
            return ""
        paragraphs: list[str] = []
        print(translation_units)

        def get_translation_txt(tu: TranslationUnit) -> str:
            if (tu.rank is not None and tu.rank == 1) or tu.rank is None:
                if tu.rewrite is not None:
                    return tu.rewrite
                return tu.translation
            return ""

        index = 0
        while index < len(translation_units):
            paragraph_id = translation_units[index].paragraph_id
            start_index = index
            while index < len(translation_units) and translation_units[index].paragraph_id == paragraph_id:
                index += 1
            contents: list[str] = [get_translation_txt(tu) for tu in translation_units[start_index:index]]
            print(start_index, index, contents)
            for i in range(len(contents)):
                if contents[i] is None:
                    print(contents[i], translation_units[i + start_index])
            paragraphs.append("".join(contents))

        return "\n".join(paragraphs)
