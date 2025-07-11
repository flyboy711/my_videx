import lightgbm as lgb
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
import joblib
from cachetools import TTLCache
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
from sub_platforms.sql_server.videx.videx_metadata import VidexTableStats
from src.sub_platforms.sql_server.videx.model.videx_strategy import VidexModelBase, VidexStrategy
from sub_platforms.sql_server.videx.videx_utils import IndexRangeCond, RangeCond, BTreeKeySide
from src.sub_platforms.sql_server.videx.model.videx_strategy import calc_mulcol_ndv_independent


class MLVidexModel(VidexModelBase):
    """基于机器学习的VIDEX基数估算模型，继承自VidexModelBase"""

    def __init__(self, stats: VidexTableStats,
                 model_path: str = "ml_videx_model.pkl",
                 fallback_model: VidexModelBase = None,
                 **kwargs):
        """
        初始化机器学习基数估算器

        Args:
            stats: 表统计信息
            model_path: 预训练模型路径
            fallback_model: 回退模型（当ML估算失败时使用）
        """
        super().__init__(stats, VidexStrategy.ml_innodb)
        self.model_path = model_path
        self.fallback_model = fallback_model or super()
        self.ndv_cache = TTLCache(maxsize=1000, ttl=1200)
        self.scaler = None
        self.feature_columns = None
        self.model = None
        self.logger = self._get_logger()

        # 加载或初始化模型
        self._load_or_init_model()

    def _load_or_init_model(self):
        """加载预训练模型，若不存在则初始化新模型"""
        try:
            model_data = joblib.load(self.model_path)
            self.model = model_data["model"]
            self.scaler = model_data["scaler"]
            self.feature_columns = model_data["feature_columns"]
            self.logger.info(f"成功加载ML基数估算模型: {self.model_path}")
        except Exception as e:
            self.logger.warning(f"加载ML模型失败，初始化新模型: {e}")
            self.model = None

    def _get_logger(self):
        """获取日志记录器"""
        import logging
        logger = logging.getLogger("ml_videx_model")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger

    def _extract_features(self, column_stats: Dict,
                          query_condition: RangeCond = None,
                          table_stats: Dict = None) -> pd.DataFrame:
        """
        提取用于基数估算的特征向量，包含列统计、直方图及查询条件特征

        Args:
            column_stats: 列统计信息
            query_condition: 查询条件
            table_stats: 表统计信息

        Returns:
            特征向量DataFrame
        """
        features = {}
        hist = column_stats.get("histogram", {})

        # 1. 基础统计特征
        features["sample_ndv"] = column_stats.get("ndv", 0)
        features["sample_size"] = column_stats.get("sample_size", 0)
        features["non_null_ratio"] = column_stats.get("non_null_ratio", 1.0)
        features["data_type"] = column_stats.get("data_type", "unknown")
        features["table_rows"] = table_stats.get("records", 0) if table_stats else 0

        # 2. 数值特征与直方图特征
        is_numeric = column_stats.get("is_numeric", False)
        features["is_numeric"] = is_numeric

        if is_numeric:
            features["min_value"] = column_stats.get("min_value", 0)
            features["max_value"] = column_stats.get("max_value", 0)
            features["avg_value"] = column_stats.get("avg_value", 0)
            features["std_dev"] = column_stats.get("std_dev", 0)
            features["skewness"] = column_stats.get("skewness", 0)
            features["kurtosis"] = column_stats.get("kurtosis", 0)

            # 直方图分布特征
            if hist and hist.get("buckets"):
                buckets = hist["buckets"]
                counts = [b["row_count"] for b in buckets]
                if len(buckets) > 1 and len(counts) > 1:
                    features["histogram_spread"] = np.std(counts) / (np.mean(counts) + 1e-6)
                    features["histogram_max_count"] = max(counts)
                    features["histogram_min_count"] = min(counts)
                    features["histogram_bucket_count"] = len(buckets)
                else:
                    features["histogram_spread"] = 0
                    features["histogram_max_count"] = 0
                    features["histogram_min_count"] = 0
                    features["histogram_bucket_count"] = 0
            else:
                features["histogram_spread"] = 0
                features["histogram_max_count"] = 0
                features["histogram_min_count"] = 0
                features["histogram_bucket_count"] = 0
        else:
            # 非数值类型默认值
            features["min_value"] = 0
            features["max_value"] = 0
            features["avg_value"] = 0
            features["std_dev"] = 0
            features["skewness"] = 0
            features["kurtosis"] = 0
            features["histogram_spread"] = 0
            features["histogram_max_count"] = 0
            features["histogram_min_count"] = 0
            features["histogram_bucket_count"] = 0

        # 3. 查询条件特征
        if query_condition:
            features["condition_type"] = query_condition.type or "unknown"
            features["condition_range"] = self._calculate_condition_range(query_condition, column_stats)
            features["condition_selectivity"] = query_condition.selectivity or 1.0

        # 4. 表级特征
        features["update_frequency"] = table_stats.get("update_frequency", 0) if table_stats else 0
        features["is_primary_key"] = column_stats.get("is_primary_key", False)
        features["has_index"] = column_stats.get("has_index", False)

        return pd.DataFrame([features])

    def _calculate_condition_range(self, condition: RangeCond,
                                   column_stats: Dict) -> float:
        """计算查询条件的范围比例，用于衡量条件选择性"""
        if not column_stats.get("is_numeric", False) or not condition.range:
            return 1.0

        min_val = column_stats.get("min_value", 0)
        max_val = column_stats.get("max_value", 0)
        if min_val >= max_val:
            return 1.0

        condition_min = condition.range.get("min", min_val)
        condition_max = condition.range.get("max", max_val)

        # 确保条件范围在列有效范围内
        condition_min = max(min_val, condition_min)
        condition_max = min(max_val, condition_max)

        if condition_min >= condition_max:
            return 0.0

        column_range = max_val - min_val
        condition_range = condition_max - condition_min
        return condition_range / column_range

    def _prepare_ml_input(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """准备机器学习模型输入，处理分类特征并标准化数值特征"""
        # 分类特征one-hot编码
        categorical_features = ["data_type", "condition_type", "is_numeric"]
        features_df = pd.get_dummies(features_df, columns=categorical_features)

        # 确保特征列与训练时一致
        if self.feature_columns:
            for col in self.feature_columns:
                if col not in features_df.columns:
                    features_df[col] = 0
            features_df = features_df[self.feature_columns]

        # 数值特征标准化
        numeric_cols = [col for col in features_df.columns if col.startswith(("sample_", "min_", "max_", "avg_"))]
        if self.scaler and numeric_cols:
            features_df[numeric_cols] = self.scaler.transform(features_df[numeric_cols])

        return features_df

    def ndv(self, column_stats: Dict) -> int:
        """
        估算列的NDV（唯一值数量），支持缓存加速

        Args:
            column_stats: 列统计信息

        Returns:
            估算的NDV值
        """
        # 构建缓存键（表名+字段列表）
        cache_key = (column_stats.get("table_name", ""), tuple(column_stats.get("field_list", [])))
        if cache_key in self.ndv_cache:
            return self.ndv_cache[cache_key]

        try:
            if not self.model:
                # 模型未加载时使用回退模型
                ndv = self.fallback_model.ndv(column_stats)
            else:
                # 提取特征并使用ML模型估算
                features = self._extract_features(column_stats, table_stats=self.table_stats)
                ml_input = self._prepare_ml_input(features)
                ndv_pred = self.model.predict(ml_input.values)[0]
                ndv = max(1, int(round(ndv_pred)))

            # 缓存结果
            self.ndv_cache[cache_key] = ndv
            return ndv

        except Exception as e:
            self.logger.warning(f"ML NDV估算失败: {e}，使用回退模型")
            ndv = self.fallback_model.ndv(column_stats)
            self.ndv_cache[cache_key] = ndv
            return ndv

    def cardinality(self, idx_range_cond: IndexRangeCond) -> int:
        """
        估算查询条件下的基数，支持多列范围查询

        Args:
            idx_range_cond: 索引范围条件

        Returns:
            估算的基数
        """
        try:
            if not self.model:
                return self.fallback_model.cardinality(idx_range_cond)

            ranges = idx_range_cond.get_valid_ranges(self.ignore_range_after_neq)
            if not ranges:
                return self.fallback_model.cardinality(idx_range_cond)

            # 对每个范围条件提取特征并预测
            predictions = []
            for rc in ranges:
                col_stats = self.table_stats.get_col_hist(rc.col)
                if not col_stats:
                    predictions.append(1.0)
                    continue

                features = self._extract_features(col_stats, rc, self.table_stats)
                ml_input = self._prepare_ml_input(features)
                pred = self.model.predict(ml_input.values)[0]
                predictions.append(pred / self.table_stats.records)  # 转换为比例

            # 合并多列预测结果（乘积法则）
            combined_selectivity = np.prod(predictions)
            cardinality = max(1, int(round(self.table_stats.records * combined_selectivity)))
            return cardinality

        except Exception as e:
            self.logger.warning(f"ML基数估算失败: {e}，使用回退模型")
            return self.fallback_model.cardinality(idx_range_cond)

    def mulcol_ndv(self, column_stats_list: List[Dict]) -> int:
        """
        估算多列组合的NDV，结合机器学习与独立性假设

        Args:
            column_stats_list: 列统计信息列表

        Returns:
            估算的多列NDV
        """
        if len(column_stats_list) <= 1:
            return self.ndv(column_stats_list[0]) if column_stats_list else 0

        try:
            # 提取各列NDV并基于独立性假设计算
            table_rows = column_stats_list[0].get("table_stats", {}).get("records", 1)
            ndvs = [self.ndv(col_stats) for col_stats in column_stats_list]

            # 多列NDV估算公式：table_rows * product(ndv/table_rows)
            combined_ndv = table_rows
            for ndv in ndvs:
                combined_ndv *= (ndv / table_rows)

            return max(1, int(round(combined_ndv)))

        except Exception as e:
            self.logger.warning(f"多列NDV估算失败: {e}，使用基础方法")
            return calc_mulcol_ndv_independent(
                [col["name"] for col in column_stats_list],
                {col["name"]: self.ndv({"field_list": [col["name"]]}) for col in column_stats_list},
                self.table_stats.records
            )

    def train(self, training_data: List[Dict], test_size: float = 0.2):
        """
        训练ML基数估算模型，支持NDV与基数多任务学习

        Args:
            training_data: 训练数据列表，包含列统计、查询条件及真实值
            test_size: 测试集比例
        """
        if not training_data:
            self.logger.error("无训练数据，无法训练模型")
            return

        X_ndv, y_ndv = [], []
        X_card, y_card = [], []

        for data in training_data:
            # 提取NDV训练数据
            features_ndv = self._extract_features(data["column_stats"], table_stats=self.table_stats).values[0]
            X_ndv.append(features_ndv)
            y_ndv.append(data["true_ndv"])

            # 提取基数训练数据
            features_card = self._extract_features(
                data["column_stats"],
                data["query_condition"],
                self.table_stats
            ).values[0]
            X_card.append(features_card)
            y_card.append(data["true_cardinality"])

        # 转换为DataFrame并处理特征
        feature_names = self._extract_features(training_data[0]["column_stats"]).columns
        X_ndv = pd.DataFrame(X_ndv, columns=feature_names)
        X_card = pd.DataFrame(X_card, columns=feature_names)

        # 分类特征编码
        categorical_features = ["data_type", "condition_type", "is_numeric"]
        X_ndv = pd.get_dummies(X_ndv, columns=categorical_features)
        X_card = pd.get_dummies(X_card, columns=categorical_features)

        # 保存特征列名
        self.feature_columns = list(X_ndv.columns.union(X_card.columns))

        # 划分训练集与测试集
        X_train_ndv, X_test_ndv, y_train_ndv, y_test_ndv = train_test_split(
            X_ndv, y_ndv, test_size=test_size, random_state=42
        )

        X_train_card, X_test_card, y_train_card, y_test_card = train_test_split(
            X_card, y_card, test_size=test_size, random_state=42
        )

        # 数值特征标准化
        numeric_cols = [col for col in X_train_ndv.columns if col.startswith(("sample_", "min_", "max_", "avg_"))]
        if numeric_cols:
            self.scaler = StandardScaler()
            X_train_ndv[numeric_cols] = self.scaler.fit_transform(X_train_ndv[numeric_cols])
            X_test_ndv[numeric_cols] = self.scaler.transform(X_test_ndv[numeric_cols])
            X_train_card[numeric_cols] = self.scaler.transform(X_train_card[numeric_cols])
            X_test_card[numeric_cols] = self.scaler.transform(X_test_card[numeric_cols])

        # 合并NDV与基数训练数据（多任务学习）
        X_train = pd.concat([X_train_ndv, X_train_card])
        y_train = y_train_ndv + y_train_card
        train_data = lgb.Dataset(X_train, label=y_train)
        valid_data = lgb.Dataset(pd.concat([X_test_ndv, X_test_card]),
                                 label=y_test_ndv + y_test_card,
                                 reference=train_data)

        # 训练LightGBM模型
        params = {
            "objective": "regression_l1",
            "metric": "mae",
            "num_leaves": 31,
            "learning_rate": 0.05,
            "n_estimators": 100,
            "lambda_l1": 0.1,
            "lambda_l2": 0.01,
            "verbosity": -1
        }

        self.model = lgb.train(
            params,
            train_data,
            valid_sets=[valid_data],
            early_stopping_rounds=10,
            verbose_eval=5
        )

        # 模型评估
        y_pred_ndv = self.model.predict(X_test_ndv)
        mae_ndv = mean_absolute_error(y_test_ndv, y_pred_ndv)
        mape_ndv = mean_absolute_percentage_error(y_test_ndv, y_pred_ndv)

        y_pred_card = self.model.predict(X_test_card)
        mae_card = mean_absolute_error(y_test_card, y_pred_card)
        mape_card = mean_absolute_percentage_error(y_test_card, y_pred_card)

        self.logger.info(f"模型训练完成 - NDV: MAE={mae_ndv:.2f}, MAPE={mape_ndv:.2%}")
        self.logger.info(f"模型训练完成 - 基数: MAE={mae_card:.2f}, MAPE={mape_card:.2%}")

        # 保存模型
        self.save_model()

    def save_model(self):
        """保存训练好的模型"""
        if self.model:
            model_data = {
                "model": self.model,
                "scaler": self.scaler,
                "feature_columns": self.feature_columns
            }
            joblib.dump(model_data, self.model_path)
            self.logger.info(f"模型已保存至: {self.model_path}")

    def update_with_feedback(self, column_stats: Dict,
                             query_condition: RangeCond,
                             true_cardinality: int):
        """
        使用实际执行反馈更新模型（在线学习）

        Args:
            column_stats: 列统计信息
            query_condition: 查询条件
            true_cardinality: 实际基数
        """
        if not self.model:
            self.logger.warning("模型未初始化，无法进行在线学习")
            return

        # 提取特征
        features = self._extract_features(column_stats, query_condition, self.table_stats)
        ml_input = self._prepare_ml_input(features)

        # 获取当前预测值
        current_pred = self.model.predict(ml_input.values)[0]

        # 计算误差
        error = abs(current_pred - true_cardinality) / (true_cardinality + 1e-6)

        # 如果误差超过阈值，进行在线学习
        if error > 0.2:  # 20%的误差阈值
            self.logger.info(f"检测到较大误差({error:.2%})，执行在线学习")

            # 准备训练数据
            X_update = ml_input
            y_update = [true_cardinality]

            # 在线训练
            update_data = lgb.Dataset(X_update, label=y_update)
            self.model = lgb.train(
                {
                    "objective": "regression_l1",
                    "metric": "mae",
                    "learning_rate": 0.01,  # 小学习率用于微调
                    "num_leaves": 31,
                    "lambda_l1": 0.1,
                    "lambda_l2": 0.01,
                    "verbosity": -1
                },
                update_data,
                init_model=self.model,
                num_boost_round=10,
                keep_training_booster=True
            )

            # 保存更新后的模型
            self.save_model()

    def info_low(self, req_json_item: dict) -> dict:
        """
        返回低级别统计信息，包含基数和NDV估算

        Args:
            req_json_item: 请求JSON项

        Returns:
            统计信息字典
        """
        res = super().info_low(req_json_item)

        # 增强统计信息，添加ML估算指标
        for i, key_json in enumerate(req_json_item['data']):
            key_name = key_json['properties']['name']
            first_fields = []

            for j, field_json in enumerate(key_json['data']):
                field_name = field_json['properties']['name']
                first_fields.append(field_name)

                ndv_key = (key_name, tuple(first_fields))
                if ndv_key in self.ndv_cache:
                    ndv = self.ndv_cache[ndv_key]
                else:
                    column_stats = {
                        "table_name": self.table_name,
                        "field_list": first_fields,
                        "histogram": self.table_stats.get_col_hist(field_name)
                    }
                    ndv = self.ndv(column_stats)
                    self.ndv_cache[ndv_key] = ndv

                # 更新rec_per_key信息
                concat_key = "rec_per_key" + " #@# " + key_name + " #@# " + field_name
                if ndv > 0:
                    res[concat_key] = self.table_stats.records / ndv
                else:
                    res[concat_key] = self.table_stats.records

        return res

    def explain_estimation(self, idx_range_cond: IndexRangeCond) -> dict:
        """
        解释基数估算过程，返回估算依据和特征重要性

        Args:
            idx_range_cond: 索引范围条件

        Returns:
            解释信息字典
        """
        explanation = {
            "method": "ml-based",
            "features": {},
            "feature_importance": {},
            "estimation_steps": [],
            "final_cardinality": 0
        }

        if not self.model:
            explanation["method"] = "fallback"
            explanation["reason"] = "ML model not available"
            explanation["final_cardinality"] = self.fallback_model.cardinality(idx_range_cond)
            return explanation

        ranges = idx_range_cond.get_valid_ranges(self.ignore_range_after_neq)
        if not ranges:
            explanation["final_cardinality"] = self.fallback_model.cardinality(idx_range_cond)
            return explanation

        # 对每个范围条件提取特征并解释
        for rc in ranges:
            col_stats = self.table_stats.get_col_hist(rc.col)
            if not col_stats:
                continue

            features = self._extract_features(col_stats, rc, self.table_stats)
            ml_input = self._prepare_ml_input(features)

            # 保存特征值
            for col in features.columns:
                explanation["features"][col] = features[col].values[0]

            # 获取特征重要性
            importance = self.model.feature_importance(importance_type='gain')
            feature_names = self.model.feature_name()

            for name, imp in zip(feature_names, importance):
                if imp > 0:
                    explanation["feature_importance"][name] = imp

            # 预测并记录步骤
            pred = self.model.predict(ml_input.values)[0]
            explanation["estimation_steps"].append({
                "column": rc.col,
                "condition": str(rc),
                "selectivity": pred / self.table_stats.records,
                "raw_prediction": pred
            })

        # 合并多列预测结果
        if explanation["estimation_steps"]:
            selectivities = [step["selectivity"] for step in explanation["estimation_steps"]]
            combined_selectivity = np.prod(selectivities)
            cardinality = max(1, int(round(self.table_stats.records * combined_selectivity)))

            explanation["final_cardinality"] = cardinality
            explanation["combined_selectivity"] = combined_selectivity

        return explanation
