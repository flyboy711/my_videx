import requests
import json
from typing import Dict, Any, List, Optional
import mysql.connector
from mysql.connector import Error


class VidexStatsCollector:
    def __init__(self, server_address: str = "http://127.0.0.1:5001",
                 db_config: Dict[str, Any] = None):
        """初始化Videx统计数据收集器"""
        self.server_address = server_address
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.db_config = db_config  # 数据库连接配置

    def get_all_stats(self) -> Dict[str, Any]:
        """获取服务器上的所有统计数据"""
        try:
            response = self.session.get(f"{self.server_address}/videx/visualization/get_stats")
            response.raise_for_status()
            return response.json()["data"]["stats"]
        except requests.exceptions.RequestException as e:
            print(f"从缓存获取统计数据失败: {e}")
            print("尝试从原始数据源获取数据...")
            return self._fetch_stats_from_source()

    def _fetch_stats_from_source(self) -> Dict[str, Any]:
        """从原始数据源获取统计信息"""
        if not self.db_config:
            print("错误: 未配置数据库连接信息，无法从源头获取数据")
            return {}

        try:
            # 连接数据库
            connection = mysql.connector.connect(
                host=self.db_config["host"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"],
                port=self.db_config.get("port", 3306)
            )

            if connection.is_connected():
                cursor = connection.cursor(dictionary=True)
                stats = {}

                # 获取所有表的统计信息
                cursor.execute("SHOW TABLE STATUS")
                table_status = cursor.fetchall()

                for table in table_status:
                    table_name = table["Name"]
                    stats[table_name] = {
                        "cardinality": table.get("Rows", "N/A"),
                        "data_size": table.get("Data_length", "N/A"),
                        "index_size": table.get("Index_length", "N/A"),
                        "create_time": table.get("Create_time", "N/A"),
                        "update_time": table.get("Update_time", "N/A"),
                    }

                    # 获取列统计信息
                    columns = self._fetch_column_stats(connection, table_name)
                    stats[table_name]["columns"] = columns

                return {"stats_dict": {self.db_config["database"]: stats}}

        except Error as e:
            print(f"从数据库获取统计信息失败: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

        return {}

    def _fetch_column_stats(self, connection, table_name: str) -> Dict[str, Any]:
        """获取特定表的列统计信息"""
        cursor = connection.cursor(dictionary=True)
        columns = {}

        try:
            # 获取列元数据
            cursor.execute(f"SHOW FULL COLUMNS FROM {table_name}")
            column_meta = cursor.fetchall()

            for column in column_meta:
                column_name = column["Field"]
                columns[column_name] = {
                    "type": column["Type"],
                    "nullable": column["Null"],
                    "key": column["Key"],
                    "default": column["Default"],
                    "extra": column["Extra"]
                }

                # 尝试获取列基数（对于InnoDB可能不可用）
                try:
                    cursor.execute(f"SELECT COUNT(DISTINCT {column_name}) as ndv FROM {table_name}")
                    result = cursor.fetchone()
                    columns[column_name]["ndv"] = result["ndv"] if result else "N/A"
                except Error:
                    columns[column_name]["ndv"] = "N/A"

            # 尝试获取直方图数据（如果支持）
            histograms = self._fetch_histogram_data(connection, table_name, list(columns.keys()))
            if histograms:
                columns["histograms"] = histograms

        except Error as e:
            print(f"获取表 {table_name} 的列统计信息失败: {e}")

        return columns

    def _fetch_histogram_data(self, connection, table_name: str, columns: List[str]) -> Dict[str, Any]:
        """尝试从数据库获取直方图数据（如果支持）"""
        histograms = {}

        # 检查是否支持information_schema统计信息
        try:
            cursor = connection.cursor(dictionary=True)

            # 对于MySQL 8.0，可以尝试从information_schema.COLUMN_STATISTICS获取直方图
            for column in columns:
                try:
                    cursor.execute(f"""
                        SELECT histogram 
                        FROM information_schema.COLUMN_STATISTICS 
                        WHERE table_schema = %s AND table_name = %s AND column_name = %s
                    """, (self.db_config["database"], table_name, column))

                    result = cursor.fetchone()
                    if result and result["histogram"]:
                        # 解析JSON格式的直方图数据
                        try:
                            histograms[column] = json.parse(result["histogram"])
                        except json.JSONDecodeError:
                            histograms[column] = {"raw_data": result["histogram"]}

                except Error:
                    continue  # 继续尝试下一列

        except Error:
            # 如果不支持，返回空
            pass

        return histograms

    def get_table_stats(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """获取特定表的完整统计数据"""
        all_stats = self.get_all_stats()
        if not all_stats:
            return {}

        table_stats = all_stats.get("stats_dict", {}).get(db_name, {})
        if not table_stats:
            print(f"数据库 {db_name} 中找不到表 {table_name} 的统计数据")
            return {}

        return table_stats.get(table_name, {})

    def print_table_statistics(self, db_name: str, table_name: str) -> None:
        """打印特定表的统计信息，包括直方图、基数和NDV"""
        table_stats = self.get_table_stats(db_name, table_name)
        if not table_stats:
            print(f"表 {db_name}.{table_name} 没有统计数据")
            return

        print(f"\n=== 表 {db_name}.{table_name} 的统计信息 ===")

        # 打印表级统计信息
        self._print_table_level_stats(table_stats)

        # 打印列级统计信息
        self._print_column_level_stats(table_stats)

    def _print_table_level_stats(self, table_stats: Dict[str, Any]) -> None:
        """打印表级统计信息"""
        # 提取基本表信息
        table_meta = table_stats.get("meta_dict", {})
        table_cardinality = table_meta.get("cardinality", "N/A")

        print(f"\n表级统计信息:")
        print(f"  - 基数 (Cardinality): {table_cardinality}")

        # 打印其他可能的表级统计信息
        additional_stats = {
            "行数": table_meta.get("rows", "N/A"),
            "数据大小": table_meta.get("data_size", "N/A"),
            "索引大小": table_meta.get("index_size", "N/A"),
            "平均行大小": table_meta.get("avg_row_length", "N/A"),
        }

        for key, value in additional_stats.items():
            if value != "N/A":
                print(f"  - {key}: {value}")

    def _print_column_level_stats(self, table_stats: Dict[str, Any]) -> None:
        """打印列级统计信息，包括直方图和NDV"""
        histograms = table_stats.get("histogram_dict", {})
        ndv_data = table_stats.get("ndv_dict", {})
        column_meta = table_stats.get("meta_dict", {}).get("columns", {})

        if not histograms and not ndv_data:
            print("\n列级统计信息: 未找到直方图或NDV数据")
            return

        print(f"\n列级统计信息:")

        # 获取所有列名
        column_names = set()
        for data_source in [histograms, ndv_data, column_meta]:
            if isinstance(data_source, dict):
                column_names.update(data_source.keys())

        for column in sorted(column_names):
            print(f"\n- 列 '{column}':")

            # 打印基数和NDV
            column_info = column_meta.get(column, {})
            column_cardinality = column_info.get("cardinality", "N/A")
            ndv = ndv_data.get(column, column_info.get("ndv", "N/A"))

            print(f"  - 基数: {column_cardinality}")
            print(f"  - NDV (唯一值数量): {ndv}")

            # 计算选择性 (Selectivity)
            if column_cardinality != "N/A" and ndv != "N/A" and column_cardinality > 0:
                selectivity = ndv / column_cardinality
                print(f"  - 选择性: {selectivity:.4f}")

            # 打印列类型
            column_type = column_info.get("type", "N/A")
            print(f"  - 数据类型: {column_type}")

            # 打印直方图信息
            hist_data = histograms.get(column, {})
            if hist_data:
                print(f"  - 直方图:")

                # 直方图基本信息
                bucket_count = len(hist_data.get("buckets", []))
                print(f"    - 桶数量: {bucket_count}")

                # 打印直方图数据样本
                self._print_histogram_sample(hist_data)
            else:
                print(f"  - 直方图: 无数据")

    def _print_histogram_sample(self, hist_data: Dict[str, Any]) -> None:
        """打印直方图数据样本"""
        # 提取直方图数据
        buckets = hist_data.get("buckets", [])
        counts = hist_data.get("counts", [])
        ndvs = hist_data.get("ndvs", [])

        # 打印样本数据
        max_samples = 5
        print(f"    - 数据样本 (最多显示{max_samples}个):")

        display_count = min(max_samples, len(buckets))
        for i in range(display_count):
            bucket = buckets[i] if i < len(buckets) else "N/A"
            count = counts[i] if i < len(counts) else "N/A"
            ndv = ndvs[i] if i < len(ndvs) else "N/A"

            print(f"      桶 {i + 1}: 值={bucket}, 频率={count}, NDV={ndv}")

        if len(buckets) > max_samples:
            print(f"      ... 共 {len(buckets)} 个桶")


# 使用示例
if __name__ == "__main__":
    # 数据库连接配置
    db_config = {
        "host": "localhost",
        "user": "videx",
        "password": "password",
        "database": "videx_2",
        "port": 13308
    }

    # 初始化收集器，指定VIDEX统计服务器地址和数据库配置
    collector = VidexStatsCollector(
        server_address="http://127.0.0.1:5001",
        db_config=db_config
    )

    # 数据库和表名
    db_name = "videx_2"
    table_name = "orders"

    # 获取并打印所有统计信息
    all_stats = collector.get_all_stats()
    print(f"获取到 {len(all_stats)} 个数据库的统计信息")

    # 打印特定表的详细统计信息
    collector.print_table_statistics(db_name, table_name)
