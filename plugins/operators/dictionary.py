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

from typing import TYPE_CHECKING, Callable, Any

from airflow.exceptions import AirflowException

import config
from data.es_data import WordDictionary, WordDictionaryHandler
from operators.basic import BasicOperator
from readmdict import MDX
from bs4 import BeautifulSoup
import threading

if TYPE_CHECKING:
    from airflow.utils.context import Context

ItemProcessMethod = Callable[[Any], Any]


def colins_item_process(item) -> WordDictionary:
    key, val = item
    key = key.decode()
    html = val.decode()
    soup = BeautifulSoup(html, 'lxml')

    # 提取词性和释义
    caption = soup.find('div', class_='caption')
    if caption:
        parts_of_speech = caption.find('span', class_='st').text if caption.find('span', class_='st') \
            else "No part of speech found"
        definition = caption.find('span', class_='text_blue').text if caption.find('span', class_='text_blue') \
            else "No definition found"
        # 提取英文释义
        definition_span = caption.find('span', class_='text_blue')
        text_parts = []
        if definition_span:
            # Collect text from next siblings
            for sibling in definition_span.next_siblings:
                if sibling.string:
                    text_parts.append(sibling.string.strip())
            english_definition = " ".join(text_parts)  # Join parts with spaces
        else:
            english_definition = "No English definition found"

        if definition == "" or english_definition == "No English definition found":
            return None

        category = ""
        target_category = ""
        if parts_of_speech != "No part of speech found":
            categories = [item for item in parts_of_speech.split() if item != '']
            if len(categories) == 2:
                category, target_category = categories[0], categories[1]
            elif len(categories) == 1:
                category = categories[0]

        src_examples = []
        tgt_examples = []
        examples = soup.find_all('li')
        for example in examples:
            if example.find('p'):
                src_examples.append(example.find('p').text)
            if len(example.find_all('p')) > 1:
                tgt_examples.append(example.find_all('p')[1].text)
        return WordDictionary(text=key, definition=definition, target_definition=english_definition,
                              category=category, target_category=target_category,
                              examples=src_examples, target_examples=tgt_examples)
    return None


class DictionaryProcessor:
    def __init__(self, file_path: str, item_process: ItemProcessMethod):
        self.file_path = file_path
        self.item_process = item_process

    def process(self) -> list[WordDictionary]:
        raise NotImplementedError


class MDXDictionaryProcessor(DictionaryProcessor):
    def __init__(self, file_path: str, item_process: ItemProcessMethod):
        super().__init__(file_path, item_process)
        if self.item_process is None:
            self.item_process = colins_item_process

    def process(self) -> list[WordDictionary]:
        word_list: list[WordDictionary] = []
        try:
            items = [*MDX(self.file_path).items()]
            for item in items:
                res = self.item_process(item)
                if res is not None and not isinstance(res, WordDictionary):
                    raise AirflowException("item process result is not what we need")
                if res is not None:
                    word_list.append(res)
        except Exception as e:
            if isinstance(e, AirflowException):
                raise e
            raise AirflowException(f"process mdx dictionary failed due to {e}")
        return word_list


class DictionaryProcessorFactory:
    @staticmethod
    def get_processor(file_path: str, item_process: ItemProcessMethod) -> DictionaryProcessor:
        file_type = config.get_file_type(file_path)
        if file_type == config.FileType.MDX:
            return MDXDictionaryProcessor(file_path, item_process)
        raise AirflowException(f"file type {file_type} not supported")


class ImportDictionaryOperator(BasicOperator):
    output_type = (
        WordDictionaryHandler.type_name
    )

    def __init__(self, *, file_path: str = config.Config.DEFAULT_DICTIONARY_PATH, item_process: ItemProcessMethod = None, **kwargs):
        super().__init__(**kwargs)
        self.processor = DictionaryProcessorFactory.get_processor(file_path, item_process)

    def execute(self, context: Context) -> Any:
        self.output_data[WordDictionaryHandler.type_name] = self.processor.process()
