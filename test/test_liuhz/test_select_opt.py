import time
import json
import pymysql
import random
import string
from itertools import permutations
import warnings

# 忽略警告
warnings.filterwarnings("ignore")


class IndexTuner:
    def __init__(self, db_config, table_name, sql_query):
        """初始化索引调优器"""
        self.indexes = None
        self.db_config = db_config
        self.table_name = table_name
        self.sql_query = sql_query
        self.connection = None
        self.best_index = None
        self.best_cost = float('inf')
        self.best_eval_cost = float('inf')
        self.best_read_cost = float('inf')
        self.table_storage_engine = "Unknown"

    def connect(self):
        """建立数据库连接并获取存储引擎信息和索引信息"""
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

            # 获取表的存储引擎
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.table_name}'")
                result = cursor.fetchone()
                if result:
                    self.table_storage_engine = result.get("Engine", "Unknown")
            print(f"表 {self.table_name} 使用的存储引擎: {self.table_storage_engine}")

            # 获取所有索引信息（排除主键）
            self.indexes = []
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW INDEX FROM {self.table_name}")
                for row in cursor.fetchall():
                    if row['Key_name'] == 'PRIMARY':
                        continue  # 跳过主键
                    self.indexes.append({
                        'name': row['Key_name'],
                        'unique': row['Non_unique'] == 0,
                        'columns': []  # 初始化列列表
                    })

            # 按索引名分组，处理多列索引
            index_map = {}
            for idx in self.indexes:
                index_map.setdefault(idx['name'], idx)

            # 重新获取完整索引信息（包含多列）
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW INDEX FROM {self.table_name}")
                for row in cursor.fetchall():
                    if row['Key_name'] == 'PRIMARY':
                        continue
                    if row['Key_name'] in index_map:
                        index_map[row['Key_name']]['columns'].append(row['Column_name'])

            print(f"获取到 {len(self.indexes)} 个非主键索引")
            # 连接后,删除表中存在的索引结构
            self.drop_indexes()
            return True
        except Exception as e:
            print(f"连接数据库失败: {e}")
            return False

    def drop_indexes(self):
        """删除所有非主键索引"""
        try:
            with self.connection.cursor() as cursor:
                for idx in self.indexes:
                    # 避免重复删除同一个索引（多列索引只需删除一次）
                    if 'dropped' not in idx:
                        drop_sql = f"ALTER TABLE {self.table_name} DROP INDEX `{idx['name']}`"
                        cursor.execute(drop_sql)
                        idx['dropped'] = True  # 标记已删除
                        print(f"已删除索引: {idx['name']}")
            self.connection.commit()
            return True
        except Exception as e:
            print(f"删除索引失败: {e}")
            self.connection.rollback()
            return False

    def recreate_indexes(self):
        """重新创建所有索引"""
        try:
            with self.connection.cursor() as cursor:
                for idx in self.indexes:
                    # 构建列列表
                    cols = ', '.join([f"`{col}`" for col in idx['columns']])
                    unique = "UNIQUE" if idx['unique'] else ""
                    create_sql = f"ALTER TABLE {self.table_name} ADD {unique} INDEX `{idx['name']}` ({cols})"
                    cursor.execute(create_sql)
                    print(f"已重建索引: {idx['name']} ({cols})")
            self.connection.commit()
            return True
        except Exception as e:
            print(f"重建索引失败: {e}")
            self.connection.rollback()
            return False

    def disconnect(self):
        """断开数据库连接（在断开前重建索引）"""
        if self.connection:
            self.recreate_indexes()  # 关键步骤：重建索引
            self.connection.close()
            print("数据库连接已关闭")

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

    def extract_cost_details(self, execution_plan):
        """从执行计划中提取成本详情"""
        if not execution_plan:
            return None

        try:
            # 从执行计划中提取成本信息
            if 'query_block' in execution_plan:
                cost_info = execution_plan['query_block'].get('cost_info') or {}
                table_info = execution_plan['query_block'].get('table', {})

                # 如果query_block中有cost_info，优先使用
                if cost_info and table_info:
                    return {
                        "total_cost": cost_info.get('query_cost'),
                        "read_cost": table_info.get('cost_info').get('read_cost'),
                        "eval_cost": table_info.get('cost_info').get('eval_cost'),
                        "used_index": table_info.get('key')
                    }

        except Exception as e:
            print(f"提取成本详情失败: {e}")
            return None

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
            return float('inf')

        # 提取成本详情
        cost_info = self.extract_cost_details(plan)
        if not cost_info or not cost_info.get('total_cost'):
            print(f"无法提取成本详情 - 执行计划: {json.dumps(plan, indent=2)}")
            if index_name:
                self.drop_temp_index(index_name)
            return float('inf')

        # 删除临时索引（如果创建了）
        if index_name:
            self.drop_temp_index(index_name)

        total_cost = float(cost_info['total_cost'])

        # 记录最佳结果
        if total_cost < self.best_cost:
            self.best_index = index_columns
            self.best_cost = total_cost
            self.best_eval_cost = cost_info.get('eval_cost', float('inf'))
            self.best_read_cost = cost_info.get('read_cost', float('inf'))

            # 打印当前最佳结果
            print(f"发现新最优索引:")
            print(f"索引: {', '.join(index_columns) if index_columns else '无索引'}")
            print(f"总成本: {total_cost}")
            if 'eval_cost' in cost_info:
                print(f"CPU成本: {cost_info['eval_cost']}")
            if 'read_cost' in cost_info:
                print(f"IO成本: {cost_info['read_cost']}")
            if cost_info.get('used_index'):
                print(f"实际使用索引: {cost_info['used_index']}")

        return total_cost

    def find_best_index(self, columns, max_columns=3):
        """找出给定列的最佳索引组合"""
        if not self.connect():
            print("无法连接数据库，退出调优")
            return None

        # 首先生成索引候选项
        candidates = self.generate_index_candidates(columns, max_columns)

        # 测试无索引情况
        print("\n============== 测试无索引情况 =================")
        cost_no_index = self.test_index([])
        print(f"无索引成本: {cost_no_index}")

        # 测试所有索引候选项
        print("\n========== 开始测试所有索引组合 ============")
        for index_columns in candidates:
            cost = self.test_index(index_columns)
            print(f"索引 {'_'.join(index_columns)} 成本: {cost}")

        # 关闭数据库连接并恢复起初索引结构
        self.disconnect()

        # 返回最终结果
        if self.best_index:
            print("\n=== 最终结果 ===")
            if self.best_index:
                print(f"最优索引: {', '.join(self.best_index)}")
            else:
                print(f"最优方案: 无索引")
            print(f"总成本: {self.best_cost}")

            if self.best_eval_cost != float('inf'):
                print(f"CPU成本: {self.best_eval_cost}")
            if self.best_read_cost != float('inf'):
                print(f"IO成本: {self.best_read_cost}")

            # 生成DDL
            if self.best_index:
                ddl = f"CREATE INDEX idx_{'_'.join(self.best_index)} ON {self.table_name} ({', '.join(self.best_index)})"
            else:
                ddl = "无索引（不需要创建索引）"

            return {
                "index": self.best_index,
                "total_cost": self.best_cost,
                "eval_cost": self.best_eval_cost if self.best_eval_cost != float('inf') else None,
                "read_cost": self.best_read_cost if self.best_read_cost != float('inf') else None,
                "ddl": ddl,
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
        "database": "videx_1"
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
        print(f"表使用的存储引擎: {best_index['table_storage_engine']}")
        print(f"最优索引DDL: {best_index['ddl']}")
        print(f"总成本: {best_index['total_cost']}")

        if best_index['eval_cost']:
            print(f"CPU成本: {best_index['eval_cost']}")
        if best_index['read_cost']:
            print(f"IO成本: {best_index['read_cost']}")
