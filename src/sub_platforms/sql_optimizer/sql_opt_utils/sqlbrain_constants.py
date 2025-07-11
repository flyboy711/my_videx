"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""

SYSTEM_DB_LIST = [
    'INFORMATION_SCHEMA',
    'PERFORMANCE_SCHEMA',
    'MYSQL',
    'SYS'
]

UNSUPPORTED_MYSQL_DATATYPE = [  # not support Spatial Data Types
    "GEOMETRY",
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
    "GEOMETRYCOLLECTION"
]

