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

import time
from typing import TYPE_CHECKING, Sequence

from data.es_data import WordDictionaryHandler, WordDictionary
from data.relational_data import TranslationUnitHandler
from operators.basic import BasicOperator
from utils.llm import QwenTranslator
from utils.machine import TranslationService

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TranslateOperator(BasicOperator):
    r"""
    Use public translate engine to translate TU(translate unit)
    """
    template_fields: Sequence[str] = ("src_lang", "tgt_lang")
    ui_color = "#40e0cd"

    # todo: add support for NE
    input_type = [TranslationUnitHandler.type_name, WordDictionaryHandler.type_name]
    output_type = input_type

    def __init__(
        self,
        *,
        src_lang: str = "en",
        tgt_lang: str = "zh-CHS",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.src_lang = src_lang
        self.tgt_lang = tgt_lang

    def translate(self, src: str) -> str:
        raise NotImplementedError


class MachineTranslateOperator(TranslateOperator):
    def __init__(
        self,
        *,
        translate_engine_type: str = 'youdao',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.translate_service = TranslationService(self.src_lang, self.tgt_lang)
        self.translate_engine_type = translate_engine_type

    def translate(self, src: str) -> str:
        return self.translate_service.translate(src, engine_type=self.translate_engine_type)

    def execute(self, context: Context) -> None:
        upstream_tasks = self.get_direct_relatives(True)

        for upstream_task in upstream_tasks:
            translation_units = self.read_data(upstream_task.get_table_suffix, TranslationUnitHandler.type_name)

            print(translation_units)
            for translation_unit in translation_units:
                if len(translation_unit.content) != 0:
                    translation_unit.translation = self.translate(translation_unit.content)
            self.log.info(f"output data - translation units: {translation_units}")
            self.output_data[TranslationUnitHandler.type_name] = translation_units
            break


class LLMTranslateOperator(TranslateOperator):
    r"""
    Operator to call LLM
    """

    def __init__(self, *, prompt: str = "", **kwargs):
        super().__init__(**kwargs)
        self.prompt = prompt
        self.translator = QwenTranslator(self.src_lang, self.tgt_lang)

    def translate(self, src: str, words: list[WordDictionary] = None) -> str:
        return self.translator.translate(src)

    def execute(self, context: Context) -> None:
        upstream_tasks = self.get_direct_relatives(True)
        words: list[WordDictionary] = []
        for upstream_task in upstream_tasks:
            if WordDictionaryHandler.type_name in upstream_task.output_type:
                words = self.read_data(upstream_task.get_table_suffix, WordDictionaryHandler.type_name)
                continue

            translation_units = self.read_data(upstream_task.get_table_suffix, TranslationUnitHandler.type_name)

            for translation_unit in translation_units:
                if len(translation_unit.content) != 0:
                    translation_unit.translation = self.translate(translation_unit.content, words)
                    time.sleep(0.5)
            self.log.info(f"output data - translation units: {translation_units}")
            self.output_data[TranslationUnitHandler.type_name] = translation_units
            break
