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

import dataclasses
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Optional, Dict, Type

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from data.data import DataInterface

output_type_key = "output_type"
pipeline_key = "pipeline"
field_mapping_key = "field_mapping"


class BasicOperator(BaseOperator):
    input_type: set[str] = []
    output_type: set[str] = []
    upstream_count: int = 0

    def __init__(self, *, input_fields: dict[int, list[str]] = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pre_execute_hook = self.check_upstream
        self._post_execute_hook = self.finish_execute
        self.pipeline = ("", "")
        self.field_mapping: dict = dict()
        self.input_fields = input_fields
        self.actual_output_type = self.output_type
        self.output_data: dict[str, list[dataclass]] = {type_name: [] for type_name in self.output_type}

    def get_output_type(self, context: Context, task_id: str) -> set[str]:
        return self.xcom_pull(context, task_id, self.dag_id, output_type_key)

    def set_output_type(self, context: Context, output_type: set[str]):
        self.xcom_push(context, output_type_key, output_type)

    def get_pipeline(self, context: Context, task_id: str) -> tuple[str]:
        return self.xcom_pull(context, task_id, self.dag_id, pipeline_key)

    def set_pipeline(self, context: Context):
        self.xcom_push(context, pipeline_key, self.pipeline)

    def get_field_mapping(self, context: Context, task_id: str) -> dict:
        field_mapping = self.xcom_pull(context, task_id, self.dag_id, field_mapping_key)
        if field_mapping is None:
            return dict()
        return field_mapping

    def set_field_mapping(self, context: Context):
        self.xcom_push(context, field_mapping_key, self.field_mapping)

    @staticmethod
    def read_data(
            table_suffix: str,
            type_name: str,
            query_conditions: Dict[str, Any] = None,
            filter_conditions: Optional[Dict[str, Any]] = None) -> list[dataclass]:
        """
        read data with type {type_name} from upstream task {task_id}
        """
        return DataInterface.read_data(type_name, table_suffix, query_conditions, filter_conditions)

    @cached_property
    def get_table_suffix(self):
        return f"{self.dag_id}_{self.task_id}"

    def write_data(self, type_name: str, data: list[dataclass]):
        DataInterface.create_data(type_name, self.get_table_suffix)
        if len(data) == 0:
            return
        if len(data) == 1:
            DataInterface.write_data(type_name, self.get_table_suffix, data[0])
        else:
            DataInterface.write_batch(type_name, self.get_table_suffix, data)

    @staticmethod
    def check_upstream_pipelines(pipeline: list[tuple[str]]) -> bool:
        # rules of different operators may differ
        return True

    def check_field_mapping(self, context: Context, upstream_tasks: list[BaseOperator]):
        if self.input_fields is None:
            return True
        if len(self.input_fields) > len(upstream_tasks):
            raise AirflowException(f"count of specified input fields {len(self.input_fields)} is more than "
                                   f"upstream task {len(upstream_tasks)}")
        for [idx, fields] in self.input_fields.items():
            if idx >= len(upstream_tasks):
                raise AirflowException(f"input field {idx} out of range")
            upstream_task = upstream_tasks[idx]
            if not isinstance(upstream_task, BasicOperator):
                raise AirflowException(f"upstream task {upstream_task} is not operator for translate, "
                                       f"can't specify input data")
            upstream_field_mapping = self.get_field_mapping(context, upstream_task.task_id)
            if len(fields) == 0:
                continue
            fields = [upstream_field_mapping[field]
                      if field in upstream_field_mapping.keys() else field for field in fields]
            fields = set(fields)
            upstream_output_fields = set()
            for type_name in upstream_task.output_type:
                data_type: Type[dataclass] = DataInterface.get_data_type(type_name)
                for field in dataclasses.fields(data_type):
                    upstream_output_fields.add(field.name)

            if len(upstream_output_fields.intersection(fields)) != 0:
                raise AirflowException(f"specified input fields {fields} not "
                                       f"in upstream task output {upstream_task.output_type}")

    def check_upstream(self, context: Context):
        """
        check pipelines where upstream operators from
        check whether operator can handle output data type from upstream operators
        if there is fields mapping from upstream operators or fields specified by users,
            verify that the fields mapping matches the upstream output and fields specified by users
        check the count of upstream operators exceeds limit
        """

        if self.upstream_count > 0:
            upstream_tasks = self.get_direct_relatives(True)
            count = 0
            upstream_pipelines: list[tuple[str]] = []
            upstream_task_list: list[BaseOperator] = []
            for upstream_task in upstream_tasks:
                upstream_output_type = self.get_output_type(context, upstream_task.task_id)
                upstream_output_type = set(upstream_output_type)

                # check whether operator can handle output type from upstream task
                if upstream_output_type is None or len(upstream_output_type.intersection(self.input_type)) != 0:
                    raise AirflowException(f"operator can't consume any of {upstream_output_type}"
                                           f"from upstream operator {upstream_task} with input type {self.input_type}")

                upstream_pipelines.append(self.get_pipeline(context, upstream_task.task_id))

                count = count + 1

                upstream_task_list.append(upstream_task)

            self.check_field_mapping(context, upstream_task_list)

            # check whether count of upstream tasks exceeds limit
            if count > self.upstream_count:
                raise AirflowException(f"with {count} upstream operators, more than {self.upstream_count} limit")

            # check whether operator can handler upstream tasks from these pipelines
            if not self.check_upstream_pipelines(upstream_pipelines):
                raise AirflowException(f"can't handle upstream tasks from pipelines {upstream_pipelines}")

    def finish_execute(self, context: Context, result: Any) -> None:
        for type_name in self.actual_output_type:
            self.write_data(type_name, self.output_data[type_name])
        self.set_output_type(context, self.actual_output_type)
        self.set_pipeline(context)
