# 数据库

## PG 数据库设计

### 初步分割的句子

- 表名：sentence
- 主键：id
- 外键：paragraph_id
- 句子内容：content
- 句子在段落中的相对序号：no
- 是否为中心句：is_top，default null
- 是否为长难句：is_long_complex，default null
- 改写结果：transition，default null

### 初步分割的段落

- 表名：paragraph
- 主键：id
- 外键：doc_id
- 段落内容：content
- 段落在文档中的相对序号：no

### 初步分割的文档

- 表名：doc
- 主键：id
- 唯一 id：uuid
- 命名：name
- 存储路径（文件绝对路径）：path
- 类别：category
- 总结：summarize
- es 存储路径：es_index

### 翻译单元 TU

- 表名：translate_unit
- 主键：id
- 索引列，所属 task id：task_id
- 句子 id 列表：sentence_ids，Integer[]
- 段落 id：paragraph_id
- 内容：content
- 翻译结果：translation
- 改写结果：rewrite
- 翻译挑选理由：choose_reason

### 提取得到的单词

- 表名：word
- 主键：id
- 索引列，所属 task id：task_id
- 原文：content
- 译文：translation
- 解释：explanation
- 单词类型：type
- 所属句子：sentence_id
- 所属 tu：tu_id
- 所属文档 id：doc_id

## ES 数据库设计

### 固定翻译库

- 索引名称：fixed_translation
- 原文：text
- 译文：translation
- 类别：category
- 来源：source

### 文档库

- 索引：doc
- 原文：text
- 文档名 or 文档唯一 id
- 译文文档名：translation_name
- 译文：translation

### 句子库

- 索引：sentence
- 原文：text
- 原始译文：translation
- 改写译文：rewrite
- 最终译文：final
- 所属风格：style
- 句子 id：sentence_id
- 文档 id：doc_id

## 知识库

用来存储提取出的各种类型的单词和翻译，存储在 ES 中，包括以下类别

### 术语 term

- 索引：term
- 原文：text
- 译文：translation
- 类别：category
- 解释：explanation

### 俚语

- 索引：slang
- 原文：text
- 译文：translation
- 定义：definition
- 类别：category

### 谚语

- 索引：proverb
- 原文：text
- 译文：translation

### 习语成语

- 索引：idiom
- 原文：text
- 译文：translation
- 含义：meaning
- 例句：example

### 名字

- 索引：name
- 原文：text
- 译文：translation
- 国家：country

### 位置

- 索引：location
- 原文：text
- 译文：translation
- 国家：country

# 全局字典数据

- 存储路径
    - 数据库的连接信息：PG 和 ES
    - 文件存储的根路径
- 翻译引擎的调用信息
- LLM 的调用信息