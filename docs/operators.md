# 数据加工节点
## 加工节点基础功能

1. 加工节点任务执行完毕后将生成数据的信息通过 xcom 传递给下游节点，公共信息需要添加到公共数据字典中
2. 加工节点需要对上游节点进行校验，校验是否来自同一个 pipeline
3. 每种加工节点能处理的数据类型有限，需要校验上游节点产生的数据类型当前节点是否能处理
4. 加工节点需要根据有无映射 + 上游节点的输出结果 + 输入指定的字段来校验数据
5. 加工节点根据上游节点传递的存储信息读取数据
6. 部分加工节点的上游节点的数量有限制，需要进行校验

## 各类加工节点

### 基础处理节点
上传文件节点

- 功能：提取文本，从指定路径读取文件，对文件的内容进行解析得到源文本，保存在文件中
    - pdf、epub 等带有格式的文件类型需要保存对应的格式文件，等待后续输出进行恢复

文本切割节点

- 功能：根据上游传递下来的源文本，将其划分为文档 - 段落 - 句子的三元结构并持久化
    - 此时已经能决定 pipeline 的前半部分
    - 提供内置的划分方法，支持用户传递自定义方法进行划分
    - 需要存储两种类型的多种数据，这里不需要将结果数据类型写入到 xcom，因为整条 pipeline 都可以从中受益

### 翻译单元处理节点

翻译单元切割节点

- 功能：根据上游的文本切割节点，根据文档-段落-句子将源文本划分为翻译单元
    - 此时已经能完全决定 pipeline
    - 提供内置的划分方法，支持用户传递自定义方法进行划分

翻译节点

- 功能：对词汇进行翻译
    - 翻译的种类包括
        - 机器翻译：直接调用机器翻译模型进行翻译
        - 大模型翻译：直接调用大模型进行翻译，翻译的 prompt + 流程可以做一些判断和识别
            - 直接进行翻译
            - 根据文本的类型选择不同的 prompt，选择不同的模型
            - 对于长难句进行改写后再翻译
            - 判断句子是否为中心句，进行翻译
    - 可以提供参考翻译标准：包括但不限于词汇表、语料库、翻译库

挑选节点

- 功能：对多个不同翻译节点的翻译结果的挑选（并给出挑选理由）
    - 工作方式
        - 大模型挑选 + 给出理由
        - 人工进行挑选
        - 大模型排序 + 给出理由后人工进行挑选
    - 包括翻译单元和词汇的翻译

润色节点

- 功能：对翻译内容进行润色和修改
    - 大模型进行润色
    - 人工润色

改写节点

- 功能：对翻译内容进行改写
    - 风格改写：将翻译转换为另外一个文风
    - 按需改写：按照某种要求（用户指定）进行改写

校正节点

- 功能：人工或者大模型对翻译结果的一致性和正确性进行校验和修改

拼接节点

- 功能：将传递的翻译单元按照文档-段落-句子拼接为完整的翻译内容并保存到用户指定路径
    - 存储方式为文件存储
    - 可选择人工校验 + 改写环节进行人工的改写和润色
    - 输出时需要恢复样式和文件类型

### 词汇处理节点

词典导入节点

- 功能：导入不同类型的词典作为知识库
    - 离线词典
    - 网络词典

词汇翻译节点

- 功能：对提取的词汇进行翻译
    - 机器翻译
    - 查找术语词典（指定上游节点）
    - 大模型翻译
    - 网络资源库查找
    - 知识库搜索

词汇抽取节点

- 功能：提取翻译单元或者句子中的各种类型的词语
    - 大模型提取
    - NLP 方法提取

词汇解释节点

- 功能：确定词语的含义和额外的附加信息
    - 词典查证
    - google 搜索
    - 知识库搜索

词汇表去重节点

- 功能：将多个来源的词汇进行去重并存储
    - 去重与合并方式
    - 存储格式是否需要做变化

## pipeline 定义

文本或者翻译单元的划分方式会影响整个操作，任何操作序列都以文本切割节点开始，绝大部分操作序列接下来是翻译单元切割节点。因此文本切割节点 + 翻译单元切割节点决定了一个 pipeline，pipeline 可以看成是一个二元组（文本切割节点 id，翻译单元切割节点 id），简写为 (A, B)

- 其中 B 可能为空，当 pipeline 中不包含翻译单元切割节点

整个 DAG 会包括多条 pipeline，pipeline 如果发生交叉需要进行校验

- 所有 pipeline 的 A 都相等，B 要么为空，要么为一个固定值。此时根据节点的语义进行判断
- 所有 pipeline 的 A 和 B 都相等，可以操作
- 当前节点为比较节点，则无需判断
- 其他情况需要报错