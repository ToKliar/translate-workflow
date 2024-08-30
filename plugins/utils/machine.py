import hashlib
import time
import uuid
from enum import Enum

import requests
from abc import ABC, abstractmethod

from airflow.exceptions import AirflowException


def truncate(q: str):
    if q is None:
        return None
    size = len(q)
    return q if size <= 20 else q[0:10] + str(size) + q[size - 10:size]


def encrypt(sign_str: str):
    hash_algorithm = hashlib.sha256()
    hash_algorithm.update(sign_str.encode('utf-8'))
    return hash_algorithm.hexdigest()


class TranslationService:
    def __init__(self, src_lang, dest_lang, delay=0.5):  # 默认每次请求之间延迟0.5秒
        self.src_lang = src_lang
        self.dest_lang = dest_lang
        self.delay = delay
        self.translators = {
            'youdao': YouDaoTranslator(src_lang, dest_lang)
        }

    def translate(self, text, engine_type='google'):
        if engine_type in self.translators:
            time.sleep(self.delay)  # 在每次翻译请求前暂停
            return self.translators[engine_type].translate(text)
        else:
            raise AirflowException(f"Unsupported translation engine: {engine_type}")


class RequestMode(Enum):
    get = 0
    post = 1


class Translator:
    def __init__(self, src_lang, dest_lang, delay=0.5):
        self.api_key = self.get_api_key()
        self.url = self.get_url()
        self.src_lang = src_lang
        self.dest_lang = dest_lang
        self.delay = delay

    @abstractmethod
    def translate(self, text) -> str:
        """翻译文本，不需要再次指定语言"""
        raise NotImplementedError

    @staticmethod
    def _make_request(url, params, mode: RequestMode = RequestMode.post):
        retries = 3
        for _ in range(retries):  # 尝试最多三次
            if mode == RequestMode.post:
                response = requests.post(url, params=params)
            else:
                response = requests.get(url, params=params)
            if response.status_code == 200:
                return response  # 成功响应
            elif 500 <= response.status_code < 600:
                # 服务器错误，可能需要重试
                print(f"Server error ({response.status_code}), retrying...")
                time.sleep(1)  # 等待一秒再重试
            else:
                # 非重试类型的错误，直接抛出异常
                response.raise_for_status()
        raise AirflowException(f"Failed to request after {retries} attempts.")


def get_api_secret() -> str:
    return "IPzMrpPSule2XXQSY8MzCLl1xQOcLX0N"


class YouDaoTranslator(Translator):
    def __init__(self, source_language: str, target_language: str):
        super().__init__(source_language, target_language)
        self.api_secret = get_api_secret()

    def get_api_key(self) -> str:
        return "76e00c4e9466cfe1"

    def get_url(self) -> str:
        return "https://openapi.youdao.com/api"

    def translate(self, text: str, **kwargs) -> str:
        cur_time = str(int(time.time()))
        # sign and encrypt data
        salt = str(uuid.uuid1())
        sign_str = (self.api_key + truncate(text) + salt + cur_time + self.api_secret)
        sign = encrypt(sign_str)

        params = {
            'from': self.src_lang,
            'to': self.dest_lang,
            'q': text,
            'appKey': self.api_key,
            'curtime': cur_time,
            'signType': 'v3',
            'salt': salt,
            'sign': sign,
        }

        translate_retries = 3
        for _ in range(translate_retries):
            result = self._make_request(self.url, params)
            content_type = result.headers['Content-Type']
            if content_type is None or content_type == 'audio/mp3':
                continue
            json_data = result.json()
            if (json_data['errorCode'] != '0' or 'translation' not in json_data
                    or not isinstance(json_data['translation'], list)):
                continue
            output = json_data['translation']

            output = "".join([item for item in output if item is not None])
            if output == "":
                continue
            return output

        raise AirflowException(f"Failed to request after {translate_retries} attempts.")
