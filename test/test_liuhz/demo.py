# 初始化模型
from sub_platforms.sql_server.videx.videx_metadata import VidexTableStats
from sub_platforms.sql_server.videx.model.videx_model_innodb import VidexModelInnoDB
from sub_platforms.sql_server.videx.videx_utils import IndexRangeCond, RangeCond, BTreeKeySide
from sub_platforms.sql_server.videx.videx_histogram import HistogramStats, HistogramBucket
import pandas as pd
import numpy as np
import json

from test.test_liuhz.test_ml_for_ndv_car import MLVidexModel

# 模拟表统计信息
table_stats = VidexTableStats(
    table_name="test_table",
    records=100000,
    clustered_index_size=500000,
    sum_of_other_index_sizes=300000,
    data_file_length=800000,
    index_file_length=600000,
    data_free_length=100000,
    pct_cached={"PRIMARY": {"pct_cached": 0.9}},
    ndvs_single={"col1": 1000, "col2": 500, "col3": 2000},
    gt_return=type('GTReturn', (), {
        'idx_gt_pair_dict': {
            'test_index': {
                'col1': (10, 20),
                'col2': (5, 15)
            }
        },
        'find': lambda self, idx_range_cond, ignore_range_after_neq: None
    })()
)

# 为列添加直方图数据
histogram = HistogramStats(
    buckets=[
        HistogramBucket(min_value=1, max_value=100, cum_freq=0.2, row_count=20000),
        HistogramBucket(min_value=101, max_value=200, cum_freq=0.5, row_count=30000),
        HistogramBucket(min_value=201, max_value=300, cum_freq=0.8, row_count=30000),
        HistogramBucket(min_value=301, max_value=1000, cum_freq=1.0, row_count=20000)
    ],
    data_type="int",
    null_values=0,
    histogram_type="equi_width",
    number_of_buckets_specified=4
)
table_stats.col_hists = {"col1": histogram, "col2": histogram}

# 初始化传统估算模型作为回退
fallback_model = VidexModelInnoDB(table_stats)

# 初始化机器学习模型
ml_model = MLVidexModel(
    stats=table_stats,
    model_path="ml_videx_model.pkl",
    fallback_model=fallback_model
)

# 准备训练数据（模拟真实查询和执行结果）
training_data = [
    {
        "column_stats": {
            "table_name": "test_table",
            "field_list": ["col1"],
            "ndv": 1000,
            "sample_size": 1000,
            "non_null_ratio": 1.0,
            "data_type": "int",
            "is_numeric": True,
            "min_value": 1,
            "max_value": 1000,
            "avg_value": 500.5,
            "std_dev": 288.68,
            "skewness": 0,
            "kurtosis": 1.2,
            "histogram": histogram.dict()
        },
        "query_condition": RangeCond(
            col="col1",
            type="range",
            range={
                "min": 101,
                "max": 200,
                "min_key_pos_side": BTreeKeySide.left,
                "max_key_pos_side": BTreeKeySide.right
            },
            selectivity=0.3
        ),
        "true_cardinality": 30000
    },
    {
        "column_stats": {
            "table_name": "test_table",
            "field_list": ["col2"],
            "ndv": 500,
            "sample_size": 1000,
            "non_null_ratio": 1.0,
            "data_type": "int",
            "is_numeric": True,
            "min_value": 1,
            "max_value": 500,
            "avg_value": 250.5,
            "std_dev": 144.34,
            "skewness": 0,
            "kurtosis": 1.2,
            "histogram": histogram.dict()
        },
        "query_condition": RangeCond(
            col="col2",
            type="range",
            range={
                "min": 51,
                "max": 150,
                "min_key_pos_side": BTreeKeySide.left,
                "max_key_pos_side": BTreeKeySide.right
            },
            selectivity=0.2
        ),
        "true_cardinality": 20000
    }
]

# 训练模型
ml_model.train(training_data)

# 构建查询条件
index_range_cond = IndexRangeCond(
    index_name="test_index",
    index_cols=["col1", "col2"],
    range_conds=[
        RangeCond(
            col="col1",
            type="range",
            range={
                "min": 201,
                "max": 300,
                "min_key_pos_side": BTreeKeySide.left,
                "max_key_pos_side": BTreeKeySide.right
            },
            selectivity=0.3
        ),
        RangeCond(
            col="col2",
            type="range",
            range={
                "min": 101,
                "max": 200,
                "min_key_pos_side": BTreeKeySide.left,
                "max_key_pos_side": BTreeKeySide.right
            },
            selectivity=0.2
        )
    ]
)

# 使用模型进行基数估算
estimated_cardinality = ml_model.cardinality(index_range_cond)
print(f"估算基数: {estimated_cardinality}")

# 获取估算解释
explanation = ml_model.explain_estimation(index_range_cond)
print("估算解释:")
print(json.dumps(explanation, indent=2, default=str))

# 使用执行反馈更新模型
ml_model.update_with_feedback(
    column_stats={
        "table_name": "test_table",
        "field_list": ["col1"],
        "ndv": 1000,
        "sample_size": 1000,
        "non_null_ratio": 1.0,
        "data_type": "int",
        "is_numeric": True,
        "min_value": 1,
        "max_value": 1000,
        "avg_value": 500.5,
        "std_dev": 288.68,
        "skewness": 0,
        "kurtosis": 1.2,
        "histogram": histogram.dict()
    },
    query_condition=RangeCond(
        col="col1",
        type="range",
        range={
            "min": 301,
            "max": 1000,
            "min_key_pos_side": BTreeKeySide.left,
            "max_key_pos_side": BTreeKeySide.right
        },
        selectivity=0.2
    ),
    true_cardinality=20000
)
