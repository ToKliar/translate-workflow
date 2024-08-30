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

import random
from typing import TYPE_CHECKING


from data.relational_data import TranslationUnit, TranslationUnitHandler
from operators.basic import BasicOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ChooseOperator(BasicOperator):
    r"""
    Operator to call LLM
    """
    input_type = [TranslationUnitHandler.type_name]
    output_type = input_type
    upstream_count = 1 << 31

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def choose(self, translation_units: list[TranslationUnit]):
        # just a mock
        # todo: use llm to choose or choose manually
        total = len(translation_units)
        rank_list = list(range(1, total + 1))
        for idx, translation_unit in enumerate(translation_units):
            self.log.info(f"rank: {rank_list[idx]}, total: {total}, comment: {rank_list[idx]}\n"
                          f"text: {translation_unit.content}")
            translation_unit.choose_reason = str(rank_list[idx])
            translation_unit.rank = rank_list[idx]

    def execute(self, context: Context) -> None:
        upstream_tasks = self.get_direct_relatives(True)

        upstream_translation_units: list[list[TranslationUnit]] = []
        for upstream_task in upstream_tasks:
            translation_units = self.read_data(upstream_task.get_table_suffix, TranslationUnitHandler.type_name)
            upstream_translation_units.append(translation_units)

        line_translation_units: list[list[TranslationUnit]] = [[] for _ in range(len(upstream_translation_units[0]))]
        for i in range(len(upstream_translation_units[0])):
            for j in range(len(upstream_translation_units)):
                line_translation_units[i].append(upstream_translation_units[j][i])

        total_translation_units: list[TranslationUnit] = []
        for multi_translation_units in line_translation_units:
            self.choose(multi_translation_units)
            total_translation_units.extend(multi_translation_units)
        total_translation_units.sort(key=lambda x: (x.id, x.rank))
        for idx, translation_unit in enumerate(total_translation_units):
            translation_unit.id = idx

        self.output_data[TranslationUnitHandler.type_name] = total_translation_units
        print(total_translation_units)
