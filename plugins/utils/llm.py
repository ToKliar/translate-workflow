from abc import abstractmethod

from openai import OpenAI


class LLMTranslator:
    def __init__(self, source_language: str, target_language: str, model: str):
        self.source_language = source_language
        self.target_language = target_language
        self.model = model


    @abstractmethod
    def get_api_key(self) -> str:
        """子类需要实现这个方法来返回相应的 API 密钥"""
        raise NotImplementedError

    @abstractmethod
    def get_url(self) -> str:
        """子类需要实现这个方法来返回相应的 URL"""
        raise NotImplementedError

    @abstractmethod
    def translate(self, text: str) -> str:
        raise NotImplementedError

    def advanced_translate(self, text: str, prompt: str) -> str:
        pass


class QwenTranslator(LLMTranslator):
    def __init__(self, source_language: str, target_language: str, model: str = "qwen-turbo"):
        super().__init__(source_language, target_language, model)
        self.client = OpenAI(
            api_key=self.get_api_key(),
            base_url=self.get_url(),
        )

    def get_api_key(self) -> str:
        return "sk-fb5dccc912a74b0292804b24c00517fe"

    def get_url(self) -> str:
        return "https://dashscope.aliyuncs.com/compatible-mode/v1"

    def translate(self, text: str) -> str:
        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[{'role': 'system', 'content': 'You are a an experienced translator.'},
                      {'role': 'user',
                       'content': f'Please help me translate the following passage from {self.source_language} to {self.target_language}. {text}'}],
            stream=True,
            # 可选，配置以后会在流式输出的最后一行展示token使用信息
            stream_options={"include_usage": True}
        )
        translation = ""
        for chunk in completion:
            for item in chunk.choices:
                translation += item.delta.content
        return translation


