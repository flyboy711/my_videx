"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""
import sqlglot.expressions

from sub_platforms.sql_optimizer.meta import Table, mysql_to_pandas_type
from sub_platforms.sql_optimizer.common.exceptions import UnsupportedException
from sqlglot.dialects.mysql import MySQL


def parse_from_expression(expression):
    ast = sqlglot.parse_one(expression, read=MySQL)
    for node in ast.dfs():
        if isinstance(node, sqlglot.expressions.Column):
            return node.name


def mapping_index_columns(table: Table):
    column_dict = {}
    for column in table.columns:
        column_dict[column.name] = column

    for index in table.indexes:
        for index_column in index.columns:
            column_name = index_column.name
            if column_name is None or column_name == "":
                if index_column.expression is not None:
                    # use replace to support "cast(json_extract(`owners`,_utf8mb4\\'$\\') as char(100) array)"
                    column_name = parse_from_expression(index_column.expression.replace("\\'", "\'"))
                    index_column.name = column_name
                else:
                    raise UnsupportedException(f"table [{table.name}] index[{index.name}] column name is empty")
            index_column.column_ref = column_dict[column_name]
