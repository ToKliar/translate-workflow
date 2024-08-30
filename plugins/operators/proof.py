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

from data.relational_data import TranslationUnitHandler
from operators.basic import BasicOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ProofOperator(BasicOperator):
    r"""
    Operator to call LLM
    """
    # todo: add support for NE
    input_type = [TranslationUnitHandler.type_name]
    output_type = input_type

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        print("proof")

    def proof(self, src: str) -> str:
        self.log.info("proof before: {}, after: {}".format(src, src))
        return src

    def execute(self, context: Context) -> None:
        upstream_tasks = self.get_direct_relatives(True)

        for upstream_task in upstream_tasks:
            translation_units = self.read_data(upstream_task.get_table_suffix, TranslationUnitHandler.type_name)
            for translation_unit in translation_units:
                if len(translation_unit.content) != 0:
                    translation_unit.rewrite = self.proof(translation_unit.translation)
            self.output_data[TranslationUnitHandler.type_name] = translation_units
            break
