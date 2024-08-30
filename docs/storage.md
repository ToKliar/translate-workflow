# 存储系统

## 数据产生

每个数据加工节点（对应 airflow 里的 operator）的结果数据会存储在数据槽 slot 中，下游数据节点可读取上游数据节点的数据槽，数据加工节点也可以读取自己的 slot 查看处理结果

## 数据流动

airflow 中数据通过 [xcom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html#xcoms) 的方式进行传递，本质上是一个 task_id - key - value 的 map。每个加工节点会将 slot 的 metadata 保存在 xcom 中，下游节点通过上游节点的 task_id 和 metadata 的 key 来获取需要读取的数据

注：xcom 无法保存太大的数据，也没有必要引入 S3 这种对象存储引入额外的系统维护复杂度

## 数据灵活性

用户对数据的物理存储无感，可以感知到的部分只有最直接的输出，如导入固定翻译，可以看到原文和译文，此时用户可以为原文和译文进行重命名，如译文命名为圣经译文。后续的加工节点可以指定使用圣经译文字段而非译文字段

## 数据存储

1. 对于绝大部分数据加工节点，slot 对应存储类型为 database
2. 对于最终的输出节点，通过文件（类型和格式与初始输入对齐）的方式存储

# 设计

目前的数据分为两种类型，具体的数据库表的格式见 [数据库设计](./database.md) 

- 关系型数据库数据，使用 PostgresSQL 进行存储
- 非关系型数据库数据，使用 ElasticSearch 进行索引

## 数据产生

每个数据加工节点有一个专属的 id，可以系统自动生成，或者由用户指定

数据节点有两种输出可能

1. 生成若干种数据类型，每种数据类型有自己的类型名称，如翻译单元的类型名称为 `translation_unit` ，生成翻译单元的数据加工节点的 id 为 `example` ，则对应的表名（table_name 或 index_name）为 `translation_unit_example`
2. 输出翻译结果文件，命名由系统根据初始输入文件名进行自动生成或者用户手动置顶

加工结果持久化之后，数据加工节点会将自身产生的数据类型和数据库类型传递给下游加工节点

如果用户指定了字段映射（如将译文指定为圣经）则将映射字典 dict 传递给下游加工节点（否则传递空 dict) 

## 数据使用

下游节点通过上游节点传递的数据类型和数据库类型读取需要使用的数据

此外还需要进行数据的校验

- 如果上游数据节点传递了字段映射，下游节点需要进行输入字段、物理表字段和字段映射的校验
- 通过不同的 doc、paragraph、sentence 划分方式，dag 可以划分为不同的 pipeline（节点序列），绝大部分类型的数据加工节点不能进行跨 pipeline 的数据使用，如上游节点不能来自多个 pipeline，具体校验逻辑，可以见 [数据加工节点](./operators.md)

## 单节点字典

每个数据加工节点必须和可能传递给下游的数据

1. 生成数据类型 `list[str]`
2. 数据库类型 `dict[str, db_type(enum)]`
3. (可能)字段映射 `dict[str, str]`
4. pipeline 编号 `tuple[str, str]`

## 全局字典

1. pg 和 es 的连接信息
2. 初始的输入文件路径

# 代码结构

存储系统相关的代码存在 data 文件夹下

数据类型和数据库操作在 `es_data.py` 和 `relational_data.py` 中

整体的思路为每种需要持久化的数据类型实现一种 DataHandler，handler 内部维护一个 map，dict[str, Any]，后续数据的修改仅仅需要修改这个 map

- 对于关系型数据库，value 为 (该列的 python 类型, 数据库存储类型），如 `(str, String)`
    - handler 会基于这个 map 生成加工处理节点使用的 dataclass 和数据库操作需要使用的 orm model
- 对于 es，value 为 (该列的 python 类型，es body mapping），如 `(str, {"type": "text", "analyzer": "ik_smart_analyzer"})`
    - handler 会基于这个 map 生成加工处理节点使用的 dataclass 和数据库建表需要的 index-body