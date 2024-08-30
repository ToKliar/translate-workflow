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

import uuid
from typing import TYPE_CHECKING, Callable

# from modelscope.outputs import OutputKeys
# from modelscope.pipelines import pipeline
# from modelscope.utils.constant import Tasks
import spacy

from config import Config
from data.es_data import TextHandler, Text
from data.relational_data import DocumentHandler, ParagraphHandler, SentenceHandler, Document, Paragraph, Sentence
from operators.basic import BasicOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

SegmentMethod = Callable[[str], list[str]]


def default_seg_document(raw: str) -> list[str]:
    # p = pipeline(task=Tasks.document_segmentation, model='../nlp_bert_document-segmentation_english-base')
    # result = p(documents=raw)
    # return result[OutputKeys.TEXT]
    return [raw]


def default_seg_paragraph(document: str) -> list[str]:
    return document.split("\n")


def default_seg_sentence(paragraph: str) -> list[str]:
    nlp = spacy.load('en_core_web_sm')
    doc = nlp(paragraph)
    sentences: list[str] = []
    for sent in doc.sents:
        sentences.append(sent.text)
    return sentences


class SegmentOperator(BasicOperator):
    r"""
    segment raw text into doc - paragraph - sentence
    """
    output_type = (
        DocumentHandler.type_name,
        ParagraphHandler.type_name,
        SentenceHandler.type_name,
        TextHandler.type_name
    )
    upstream_count = 1

    def __init__(
            self,
            *,
            seg_document: SegmentMethod = default_seg_document,
            seg_paragraph: SegmentMethod = default_seg_paragraph,
            seg_sentence: SegmentMethod = default_seg_sentence,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.seg_document = seg_document
        self.seg_paragraph = seg_paragraph
        self.seg_sentence = seg_sentence
        self.pipeline = (self.task_id, "")

    def execute(self, context: Context):
        raw_file_path = Config.get_input_file_path(self.dag_id)
        with open(raw_file_path, 'r', encoding='utf-8') as f:
            raw_text = "".join(f.readlines())

        raw_docs = self.seg_document(raw_text)
        text_name = Config.TEXT_NAME

        texts: list[Text] = []
        docs: list[Document] = []
        paragraphs: list[Paragraph] = []
        sentences: list[Sentence] = []

        for doc_idx, raw_doc in enumerate(raw_docs):
            texts.append(Text(content=raw_doc, name=text_name, translation_name="", translation=""))
            doc = Document(id=len(docs), uuid=str(uuid.uuid1()), path=Config.get_input_file_path(self.dag_id), name=text_name)
            docs.append(doc)

            raw_paragraphs = self.seg_paragraph(raw_doc)
            for paragraph_idx, raw_paragraph in enumerate(raw_paragraphs):
                paragraph = Paragraph(id=len(paragraphs), doc_id=doc.id, content=raw_paragraph, index=paragraph_idx)
                paragraphs.append(paragraph)

                raw_sentences = self.seg_sentence(raw_paragraph)
                for sentence_idx, raw_sentence in enumerate(raw_sentences):
                    sentence = Sentence(paragraph_id=paragraph.id, content=raw_sentence, index=sentence_idx)
                    sentences.append(sentence)

        self.log.info(f"output data - doc: {docs}, paragraphs: {paragraphs},"
                      f" sentences: {sentences}, texts: {texts}")

        self.output_data[DocumentHandler.type_name] = docs
        self.output_data[ParagraphHandler.type_name] = paragraphs
        self.output_data[SentenceHandler.type_name] = sentences
        self.output_data[TextHandler.type_name] = texts
