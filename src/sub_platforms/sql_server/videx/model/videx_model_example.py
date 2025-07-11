"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""
from typing import List

from sub_platforms.sql_server.videx.model.videx_model_innodb import VidexModelInnoDB
from sub_platforms.sql_server.videx.videx_metadata import VidexTableStats
from sub_platforms.sql_server.videx.model.videx_strategy import VidexStrategy
from sub_platforms.sql_server.videx.videx_utils import IndexRangeCond


class VidexModelExample(VidexModelInnoDB):
    """
    VidexModelExample estimates NDV, scan_time, and cardinality in a naive way.
    Unlike VidexModelInnoDB, VidexModelExample does not require statistics such as NDV, histograms, or where clauses,
    which can be costly to fetch.

    VidexModelExample inherits from VidexModelInnoDB regarding system variables, schema, and metadata information.
    Fetching this information is efficient since it only requires querying MySQL information tables once.

    The drawback of VidexModelExample is its inaccuracy in characterizing data distribution.
    However, we believe it's a good and simple demonstration for users to get started.

    References:
        MySQL, storage/example/ha_example.cc
    """

    def __init__(self, stats: VidexTableStats, **kwargs):
        super().__init__(stats, **kwargs)
        self.strategy = VidexStrategy.example

    def scan_time(self, req_json_item: dict) -> float:
        return (self.table_stats.records + self.table_stats.deleted) / 20.0 + 10

    def get_memory_buffer_size(self, req_json_item: dict) -> int:
        return -1

    def cardinality(self, idx_range_cond: IndexRangeCond) -> int:
        """
        corresponds to cardinality methods
        """
        return 10

    def ndv(self, index_name, field_list: List[str]) -> int:
        return 1




from typing import List
from sub_platforms.sql_server.videx.model.videx_model_innodb import VidexModelInnoDB
from sub_platforms.sql_server.videx.videx_metadata import VidexTableStats
from sub_platforms.sql_server.videx.model.videx_strategy import VidexStrategy
from sub_platforms.sql_server.videx.videx_utils import IndexRangeCond
from estndv import ndvEstimator

class ExtendedVidexModelExample(VidexModelInnoDB):
    """
    扩展 VidexModelExample 类，引入统计学习算法来估计 card 和 ndv
    """

    def __init__(self, stats: VidexTableStats, **kwargs):
        super().__init__(stats, **kwargs)
        self.strategy = VidexStrategy.example
        self.ndv_estimator = ndvEstimator()

    def scan_time(self, req_json_item: dict) -> float:
        return (self.table_stats.records + self.table_stats.deleted) / 20.0 + 10

    def get_memory_buffer_size(self, req_json_item: dict) -> int:
        return -1

    def cardinality(self, idx_range_cond: IndexRangeCond) -> int:
        """
        估算基数
        这里可以使用现有的统计数据和合适的统计学习算法来估算基数
        示例中我们使用简单的比例估算
        """
        ranges = idx_range_cond.get_valid_ranges(self.ignore_range_after_neq)
        total_rows = self.table_stats.records
        selectivity = 1.0

        for rc in ranges:
            col_hist = self.table_stats.get_col_hist(rc.col)
            if col_hist is not None and len(col_hist.buckets) > 0:
                min_freq = col_hist.find_nearest_key_pos(rc.min_value, rc.min_key_pos_side)
                max_freq = col_hist.find_nearest_key_pos(rc.max_value, rc.max_key_pos_side)
                col_selectivity = max_freq - min_freq
                selectivity *= col_selectivity

        estimated_cardinality = int(total_rows * selectivity)
        if estimated_cardinality == 0:
            estimated_cardinality = 1

        return estimated_cardinality

    def ndv(self, index_name, field_list: List[str]) -> int:
        """
        估算不同值数量
        使用 estndv 库来估算不同值数量
        """
        if self.table_stats.sample_file_info is not None:
            table_rows = self.table_stats.records
            df_sample_raw = self.load_sample_file(self.table_stats)

            for field in field_list:
                if field in df_sample_raw.columns:
                    col_data = df_sample_raw[field].dropna().tolist()
                    # 假设这里使用 sample_predict 方法
                    ndv = self.ndv_estimator.sample_predict(S=col_data, N=table_rows)
                    return ndv

        # 如果没有采样数据，使用独立分布假设估算
        return calc_mulcol_ndv_independent(field_list, self.table_stats.ndvs_single, self.table_stats.records)

    def load_sample_file(self, table_stats):
        # 这里假设 load_sample_file 函数已经定义
        return load_sample_file(table_stats)

def calc_mulcol_ndv_independent(col_names: List[str], ndvs_single: dict, table_rows: int) -> int:
    """
    基于多列 NDV 独立分布的假设，从单列 NDV 计算多列的 NDV
    """
    ndv_product = 1
    for col in col_names:
        if col in ndvs_single:
            ndv_product *= ndvs_single[col]
        else:
            ndv_product *= 1

    return min(ndv_product, table_rows)