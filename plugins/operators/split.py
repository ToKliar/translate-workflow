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

from typing import Callable, TYPE_CHECKING

from airflow.exceptions import AirflowException

from data.relational_data import ParagraphHandler, SentenceHandler, TranslationUnitHandler, \
    TranslationUnit, Paragraph, Sentence
from operators.segment import SegmentOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

from operators.basic import BasicOperator

SplitMethod = Callable[[list[str]], list[list[int]]]


def default_split(sentences: list[str]) -> list[list[int]]:
    indexes = []
    for idx in range(len(sentences)):
        indexes.append([idx])
    return indexes


class SplitOperator(BasicOperator):
    input_type = (
        ParagraphHandler.type_name,
        SentenceHandler.type_name,
    )
    output_type = (
        TranslationUnitHandler.type_name,
    )
    upstream_count = 1

    def __init__(self, *, split: SplitMethod = None, **kwargs):
        super().__init__(**kwargs)
        if split is None:
            self.split = default_split
        else:
            self.split = split

    def execute(self, context: Context):
        table_suffix = ""
        for upstream_task in self.upstream_list:
            if not isinstance(upstream_task, SegmentOperator):
                raise AirflowException(f"Upstream task must be a SegmentOperator, but {type(upstream_task)}")
            table_suffix = upstream_task.get_table_suffix
            self.pipeline = (upstream_task.task_id, self.task_id)
            break
        paragraphs: list[Paragraph] = self.read_data(table_suffix, ParagraphHandler.type_name)
        sentences: list[Sentence] = self.read_data(table_suffix, SentenceHandler.type_name)
        print(sentences)
        print(paragraphs)
        translation_units: list[TranslationUnit] = []
        index = 0
        for paragraph in paragraphs:
            while index < len(sentences) and sentences[index].paragraph_id < paragraph.id:
                index += 1
            start_index = index
            while index < len(sentences):
                if sentences[index].paragraph_id > paragraph.id:
                    break
                index += 1

            contents = [sentence.content for sentence in sentences[start_index:index]]
            sentence_ranges = self.split(contents)
            for sentence_range in sentence_ranges:
                if len(sentence_range) == 0:
                    continue
                translation_unit_content = " ".join([contents[idx] for idx in sentence_range])
                translation_units.append(
                    TranslationUnit(
                        id=len(translation_units),
                        content=translation_unit_content,
                        sentence_ids=[sentences[idx+start_index].id for idx in sentence_range],
                        paragraph_id=paragraph.id,
                    )
                )
        self.log.info(f"output data - translation units: {translation_units}")
        self.output_data[TranslationUnitHandler.type_name] = translation_units


