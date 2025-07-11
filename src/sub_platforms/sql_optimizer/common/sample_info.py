"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""

from dataclasses import dataclass
from typing import Optional

from sub_platforms.sql_optimizer.meta import Column, TableId


@dataclass
class SampleColumnInfo:
    table_id: TableId
    column_name: str
    data_type: Optional[str] = None
    # 大字段，进行前缀采样的长度，0 表示不进行前缀采样
    sample_length: Optional[int] = 0

    @property
    def db_name(self):
        return self.table_id.db_name

    @property
    def table_name(self):
        return self.table_id.table_name

    @classmethod
    def from_column(cls, column: Column, sample_length: int = 0):
        table_id = TableId(db_name=column.db, table_name=column.table)
        column_info = SampleColumnInfo(table_id, column.name)
        column_info.data_type = column.data_type
        column_info.sample_length = sample_length
        return column_info

    @classmethod
    def new_ins(cls, db_name, table_name, column_name: str, sample_length: int = 0, data_type: str = None):
        table_id = TableId(db_name=db_name, table_name=table_name)
        column_info = SampleColumnInfo(table_id, column_name)
        column_info.data_type = data_type
        column_info.sample_length = sample_length
        return column_info

    def __hash__(self):
        return hash((self.table_id, self.column_name))

    def __eq__(self, other):
        if not isinstance(other, SampleColumnInfo):
            return False
        return self.table_id == other.table_id and self.column_name == other.column_name
