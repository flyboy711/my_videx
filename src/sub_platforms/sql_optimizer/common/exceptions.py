"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""


class TableNotFoundException(Exception):
    def __init__(self, message, table_name):
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self):
        return f"Table not found Exception: {self.message}, table : {self.table_name}"


class UnsupportedException(Exception):
    def __init__(self, message):
        super(UnsupportedException, self).__init__(message)


class TraceLoadException(ValueError):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"Failed load trace from OPTIMIZE_TRACE"
