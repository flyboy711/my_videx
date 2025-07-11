import time
import json
import pymysql
import random
import string
import re
from itertools import permutations
import warnings
import math

# 忽略警告
warnings.filterwarnings("ignore")


class IndexTuner:
    def __init__(self, db_config, table_name, sql_query):
        """初始化索引调优器"""
        self.db_config = db_config
        self.table_name = table_name
        self.sql_query = sql_query
        self.connection = None
        self.best_index = None
        self.best_score = float('inf')  # 使用综合评分代替单一成本
        self.best_cost = float('inf')
        self.best_cpu_cost = float('inf')
        self.best_io_cost = float('inf')
        self.best_rows = float('inf')
        self.best_extra = ""
        self.db_storage_engine = "Unknown"
        self.table_storage_engine = "Unknown"

    def connect(self):
        """建立数据库连接并获取存储引擎信息"""
        try:
            # 建立数据库连接
            self.connection = pymysql.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"],
                cursorclass=pymysql.cursors.DictCursor
            )
            print(f"成功连接到数据库: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")

            # 获取数据库默认存储引擎
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW VARIABLES LIKE 'default_storage_engine'")
                result = cursor.fetchone()
                if result:
                    self.db_storage_engine = result.get("Value", "Unknown")

            # 获取表的存储引擎
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.table_name}'")
                result = cursor.fetchone()
                if result:
                    self.table_storage_engine = result.get("Engine", "Unknown")

            print(f"数据库默认存储引擎: {self.db_storage_engine}")
            print(f"表 {self.table_name} 使用的存储引擎: {self.table_storage_engine}")

            return True
        except Exception as e:
            print(f"连接数据库失败: {e}")
            return False

    def generate_index_candidates(self, columns, max_columns=3):
        """生成所有可能的索引组合"""
        candidates = []
        print(f"为列 {columns} 生成索引候选项...")

        # 单列索引
        for col in columns:
            candidates.append([col])

        # 复合索引组合（多列排列）
        for col_count in range(2, min(len(columns), max_columns) + 1):
            for perm in permutations(columns, col_count):
                candidates.append(list(perm))

        # 覆盖索引（包含所有列）
        if len(columns) <= max_columns:
            for perm in permutations(columns):
                candidates.append(list(perm))

        print(f"生成了 {len(candidates)} 个索引候选项")
        return candidates

    def create_temp_index(self, index_columns):
        """创建临时索引用于测试"""
        if not index_columns:  # 无索引情况
            return None

        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        index_name = f"idx_temp_{random_suffix}"
        columns_str = ", ".join(index_columns)

        try:
            with self.connection.cursor() as cursor:
                # 创建新索引
                sql = f"CREATE INDEX {index_name} ON {self.table_name} ({columns_str})"
                cursor.execute(sql)
                self.connection.commit()
                return index_name
        except Exception as e:
            print(f"创建临时索引失败: {e}")
            return None

    def drop_temp_index(self, index_name):
        """删除临时索引"""
        if not index_name:
            return False

        try:
            with self.connection.cursor() as cursor:
                sql = f"DROP INDEX {index_name} ON {self.table_name}"
                cursor.execute(sql)
                self.connection.commit()
                return True
        except Exception as e:
            print(f"删除临时索引失败: {e}")
            return False

    def get_mysql_explain_plan(self):
        """获取当前SQL的执行计划"""
        try:
            with self.connection.cursor() as cursor:
                explain_sql = f"EXPLAIN FORMAT=JSON {self.sql_query}"
                cursor.execute(explain_sql)
                result = cursor.fetchone()
                return json.loads(result['EXPLAIN']) if result and 'EXPLAIN' in result else None
        except Exception as e:
            print(f"获取执行计划失败: {e}")
            return None

    def extract_execution_details(self, execution_plan):
        """从执行计划中提取详细信息"""
        if not execution_plan:
            return None

        try:
            details = {}

            # 1. 提取成本信息
            if 'query_block' in execution_plan:
                query_block = execution_plan['query_block']
                cost_info = query_block.get('cost_info') or {}
                table_info = query_block.get('table', {})

                # 如果query_block中有cost_info，优先使用
                if cost_info:
                    details.update({
                        "total_cost": cost_info.get('query_cost'),
                        "io_cost": cost_info.get('read_cost'),
                        "cpu_cost": cost_info.get('eval_cost')
                    })
                # 否则使用table中的cost_info
                elif 'cost_info' in table_info:
                    cost_info = table_info['cost_info']
                    details.update({
                        "total_cost": cost_info.get('cost_info'),
                        "io_cost": cost_info.get('read_cost'),
                        "cpu_cost": cost_info.get('eval_cost')
                    })

            # 尝试在顶层查找
            if not details and 'cost_info' in execution_plan:
                cost_info = execution_plan['cost_info']
                details.update({
                    "total_cost": cost_info.get('query_cost') or cost_info.get('cost_info'),
                    "io_cost": cost_info.get('read_cost'),
                    "cpu_cost": cost_info.get('eval_cost')
                })

            # 2. 提取行数和过滤信息
            rows_examined = table_info.get('rows_examined_per_scan') if 'query_block' in execution_plan else None
            rows = table_info.get('rows') if 'query_block' in execution_plan else None
            filtered = table_info.get('filtered_percent') if 'query_block' in execution_plan else None

            # 使用第一个可用值
            if rows_examined is None:
                rows_examined = rows
            if rows_examined is None:
                rows_examined = execution_plan.get('rows', 0)

            details['rows_examined'] = rows_examined
            details['filtered'] = filtered or 0

            # 3. 提取额外信息（Using filesort, Using temporary等）
            extra = table_info.get('attached_condition') or table_info.get('Extra') or ""
            extra_lower = extra.lower()

            # 标记是否有文件排序或临时表
            details['using_filesort'] = 'using filesort' in extra_lower
            details['using_temporary'] = 'using temporary' in extra_lower
            details['using_where'] = 'using where' in extra_lower
            details['using_index'] = 'using index' in extra_lower
            details['extra'] = extra

            # 4. 提取索引信息
            details['used_index'] = table_info.get('key')
            details['possible_keys'] = table_info.get('possible_keys') or []

            # 5. 访问类型
            details['access_type'] = table_info.get('access_type') or table_info.get('type') or ""

            return details
        except Exception as e:
            print(f"提取执行详情失败: {e}")
            return None

    def calculate_index_score(self, details):
        """基于多个因素计算索引的综合评分"""
        if not details:
            return float('inf')

        # 1. 基础成本（占主要权重）
        total_cost = float(details.get('total_cost', float('inf')))

        # 2. 扫描行数（对性能影响大）
        rows_examined = float(details.get('rows_examined', float('inf')))
        if rows_examined < 1:
            rows_examined = 1

        # 3. 额外操作（文件排序、临时表）的惩罚项
        penalty = 0
        if details.get('using_filesort'):
            penalty += 100  # 文件排序非常昂贵

        if details.get('using_temporary'):
            penalty += 500  # 创建临时表极其昂贵

        # 4. 访问类型加分项（索引访问比全表扫描好）
        access_bonus = 0
        access_type = details.get('access_type', '').lower()

        if access_type in ['eq_ref', 'const', 'system']:
            access_bonus = -500  # 最好的访问类型
        elif access_type in ['ref', 'range']:
            access_bonus = -200  # 很好的访问类型
        elif access_type == 'index':
            access_bonus = -50  # 索引扫描（比全表好）
        elif access_type == 'all':
            access_bonus = 0  # 全表扫描（最差）

        # 5. 索引覆盖加分项
        if details.get('using_index'):
            access_bonus -= 100  # 使用覆盖索引很好

        # 6. 成本稳定性调整
        # 对于InnoDB引擎，成本估算可能不准确，需要更加重视扫描行数
        weight = 1.0
        if 'innodb' in self.table_storage_engine.lower():
            weight = 0.7  # 降低成本的权重，提高行数的权重

        # 综合评分公式
        # 基础成本 * 权重 + 行数的对数 * 100 + 额外操作惩罚 + 访问类型奖励
        return (total_cost * weight) + (math.log10(rows_examined) * 100) + penalty + access_bonus

    def test_index(self, index_columns):
        """测试单个索引的性能"""
        # 创建临时索引（如果有列）
        index_name = self.create_temp_index(index_columns) if index_columns else None

        print(f"测试索引: {', '.join(index_columns) if index_columns else '无索引'}")

        # 获取执行计划
        plan = self.get_mysql_explain_plan()
        if not plan:
            print("无法获取执行计划")
            if index_name:
                self.drop_temp_index(index_name)
            return float('inf'), {}

        # 提取详细执行信息
        execution_details = self.extract_execution_details(plan)
        if not execution_details:
            print(f"无法提取执行详情 - 执行计划: {json.dumps(plan, indent=2)}")
            if index_name:
                self.drop_temp_index(index_name)
            return float('inf'), {}

        # 删除临时索引（如果创建了）
        if index_name:
            self.drop_temp_index(index_name)

        # 计算综合评分
        score = self.calculate_index_score(execution_details)

        # 记录最佳结果
        if score < self.best_score:
            self.best_index = index_columns
            self.best_score = score
            self.best_cost = float(execution_details.get('total_cost', float('inf')))
            self.best_rows = float(execution_details.get('rows_examined', float('inf')))
            self.best_cpu_cost = execution_details.get('cpu_cost', float('inf'))
            self.best_io_cost = execution_details.get('io_cost', float('inf'))
            self.best_extra = execution_details.get('extra', "")
            self.best_access_type = execution_details.get('access_type', "")

            # 打印当前最佳结果
            print(f"发现新最优索引:")
            print(f"  索引: {', '.join(index_columns) if index_columns else '无索引'}")
            print(f"  综合评分: {score:.2f}")
            print(f"  总成本: {self.best_cost}")
            print(f"  扫描行数: {self.best_rows}")
            print(f"  访问类型: {self.best_access_type}")
            if self.best_extra:
                print(f"  额外信息: {self.best_extra}")

        return score, execution_details

    def find_best_index(self, columns, max_columns=3):
        """找出给定列的最佳索引组合"""
        if not self.connect():
            print("无法连接数据库，退出调优")
            return None

        # 首先生成索引候选项
        candidates = self.generate_index_candidates(columns, max_columns)

        # 测试无索引情况
        print("\n=== 测试无索引情况 ===")
        score_no_index, details_no_index = self.test_index([])
        print(f"无索引评分: {score_no_index:.2f}")

        # 测试所有索引候选项
        print("\n=== 开始测试所有索引组合 ===")
        for index_columns in candidates:
            score, details = self.test_index(index_columns)
            print(
                f"索引 {'>'.join(index_columns)} 评分: {score:.2f}, 行数: {details.get('rows_examined', '?')}, 类型: {details.get('access_type', '?')}")

        # 关闭数据库连接
        self.connection.close()

        # 返回最终结果
        if self.best_index:
            print("\n=== 最终结果 ===")
            if self.best_index:
                print(f"最优索引: {', '.join(self.best_index)}")
            else:
                print(f"最优方案: 无索引")
            print(f"综合评分: {self.best_score:.2f}")
            print(f"总成本: {self.best_cost}")
            print(f"扫描行数: {self.best_rows}")
            print(f"访问类型: {self.best_access_type}")
            if self.best_extra:
                print(f"额外信息: {self.best_extra}")

            # 生成DDL
            if self.best_index:
                ddl = f"CREATE INDEX idx_{'_'.join(self.best_index)} ON {self.table_name} ({', '.join(self.best_index)})"
            else:
                ddl = "无索引（不需要创建索引）"

            return {
                "index": self.best_index,
                "score": self.best_score,
                "total_cost": self.best_cost,
                "rows_examined": self.best_rows,
                "access_type": self.best_access_type,
                "extra_info": self.best_extra,
                "ddl": ddl,
                "db_storage_engine": self.db_storage_engine,
                "table_storage_engine": self.table_storage_engine
            }

        print("未找到合适的索引")
        return None


# 使用示例
if __name__ == "__main__":
    # 数据库配置
    db_config = {
        "host": "127.0.0.1",
        "port": 13308,
        "user": "videx",
        "password": "password",
        "database": "videx_tpch_tiny"
    }

    # SQL查询
    sql_query = "SELECT * FROM t_users WHERE is_active = 1 AND city LIKE '北%' AND age > 20"

    # 表名
    table_name = "t_users"

    # 需要测试的列
    columns_to_test = ["is_active", "city", "age"]

    # 创建调优器并运行
    print("启动索引调优...")
    tuner = IndexTuner(db_config, table_name, sql_query)

    # 寻找最佳索引
    best_index = tuner.find_best_index(columns_to_test, max_columns=3)

    # 打印结果
    if best_index:
        print("\n===== 最优索引方案 =====")
        print(f"SQL: {sql_query}")
        print(f"表: {table_name}")
        print(f"数据库默认存储引擎: {best_index['db_storage_engine']}")
        print(f"表使用的存储引擎: {best_index['table_storage_engine']}")
        print(f"最优索引DDL: {best_index['ddl']}")
        print(f"综合评分: {best_index['score']:.2f}")
        print(f"总成本: {best_index['total_cost']}")
        print(f"扫描行数: {best_index['rows_examined']}")
        print(f"访问类型: {best_index['access_type']}")
        if best_index['extra_info']:
            print(f"额外信息: {best_index['extra_info']}")
    else:
        print("未找到最佳索引方案")
