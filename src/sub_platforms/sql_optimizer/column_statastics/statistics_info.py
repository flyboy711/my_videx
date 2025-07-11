"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, PrivateAttr, BeforeValidator
from typing_extensions import Annotated

from sub_platforms.sql_optimizer.common.pydantic_utils import PydanticDataClassJsonMixin
from sub_platforms.sql_optimizer.videx.videx_histogram import HistogramStats


def large_number_decoder(y):
    # 防止主键过大丢失精度：转换成字符串
    if isinstance(y, list):
        for item in y:
            if isinstance(item, dict) and "Value" in item:
                item['Value'] = str(item['Value'])
        return y
    else:
        res = [{"ColumnName": "id", "Value": str(y)}]
        return res


class TableStatisticsInfo(BaseModel, PydanticDataClassJsonMixin):
    db_name: str
    table_name: str
    # {col_name: col ndv}
    ndv_dict: Optional[Dict[str, float]] = Field(default_factory=dict)
    # {col_name: histogram}
    histogram_dict: Optional[Dict[str, HistogramStats]] = Field(default_factory=dict)
    # {col_name: not null ratio}
    not_null_ratio_dict: Optional[Dict[str, float]] = Field(default_factory=dict)

    # table rows
    num_of_rows: Optional[int] = Field(default=0)
    # 主键的最大值（经过 large_number_decoder 处理）
    max_pk: Annotated[Optional[List[Dict[str, str]]], BeforeValidator(large_number_decoder)] = Field(default=None)
    min_pk: Annotated[Optional[List[Dict[str, str]]], BeforeValidator(large_number_decoder)] = Field(default=None)

    # sample related info
    # 是否采样成功
    is_sample_success: Optional[bool] = Field(default=True)
    # 是否支持采样
    is_sample_supported: Optional[bool] = Field(default=True)
    # 不支持采样的原因
    unsupported_reason: Optional[str] = Field(default=None)
    # 采样行数
    sample_rows: Optional[int] = Field(default=0)
    # 本地采样文件路径前缀
    local_path_prefix: Optional[str] = Field(default=None)
    # TOS（对象存储）采样文件路径前缀
    tos_path_prefix: Optional[str] = Field(default=None)
    # 采样文件列表
    sample_file_list: Optional[List[str]] = Field(default_factory=list)
    # 采样文件块大小列表
    block_size_list: Optional[List[int]] = Field(default_factory=list)
    # 分片号
    shard_no: Optional[int] = Field(default=0)
    # {col_name: sample error}
    sample_error_dict: Optional[Dict[str, str]] = Field(default_factory=dict)
    # {col_name: histogram error}
    histogram_error_dict: Optional[Dict[str, float]] = Field(default_factory=dict)
    msg: Optional[str] = None
    extra_info: Optional[Dict[str, Any]] = Field(default_factory=dict)

    _version: Optional[str] = PrivateAttr(default='1.0.0')
