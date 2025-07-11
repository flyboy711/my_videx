"""
简单测试结构的准确性以及CPU、内存和IO等的变化
"""

import json
import time
import concurrent.futures
import psutil
import requests

# 配置参数
URL = "http://127.0.0.1:5001/ask_videx"  # 替换为实际的 ask_videx 接口地址
CONCURRENCY = 1000  # 并发数
REQUESTS_COUNT = 2000  # 请求总数

# curl 请求中的 JSON 数据
json_requests = [
    {
        "item_type": "videx_root",
        "properties": {
            "dbname": "videx_1",
            "function": "virtual double ha_videx::get_memory_buffer_size()",
            "table_name": "orders",
            "target_storage_engine": "VIDEX"
        },
        "data": []
    },
    # {
    #     "item_type": "videx_root",
    #     "properties": {
    #         "dbname": "videx_remote",
    #         "function": "virtual double ha_videx::scan_time()",
    #         "table_name": "timeout_record_31",
    #         "target_storage_engine": "VIDEX"
    #     },
    #     "data": []
    # }

    # {
    #     "item_type": "videx_request",
    #     "properties": {
    #         "dbname": "videx_tpch_tiny",
    #         "function": "virtual ha_rows ha_innobase::records_in_range(uint, key_range*, key_range*)",
    #         "table_name": "orders",
    #         "target_storage_engine": "VIDEX"
    #     },
    #     "data": [
    #         {
    #             "item_type": "min_key",
    #             "properties": {
    #                 "index_name": "ORDERS_FK1",
    #                 "length": "4",
    #                 "operator": ">"
    #             },
    #             "data": [
    #                 {
    #                     "item_type": "column_and_bound",
    #                     "properties": {
    #                         "column": "O_CUSTKEY",
    #                         "value": "3"
    #                     },
    #                     "data": []
    #                 }
    #             ]
    #         },
    #         {
    #             "item_type": "max_key",
    #             "properties": {
    #                 "index_name": "ORDERS_FK1",
    #                 "length": "4",
    #                 "operator": ">"
    #             },
    #             "data": [
    #                 {
    #                     "item_type": "column_and_bound",
    #                     "properties": {
    #                         "column": "O_CUSTKEY",
    #                         "value": "3"
    #                     },
    #                     "data": []
    #                 }
    #             ]
    #         }
    #     ]
    # },

    {
        "item_type": "videx_request",
        "properties": {
            "dbname": "videx_1",
            "function": "virtual int ha_videx::info_low(uint, bool)",
            "table_name": "orders",
            "target_storage_engine": "VIDEX",
            # "videx_options": "{\"task_id\": \"127_0_0_1_13308@@@demo_tpch\", \"use_gt\": true}"
        },
        "data": [
            {
                "item_type": "key",
                "properties": {
                    "key_length": "4",
                    "name": "PRIMARY"
                },
                "data": [
                    {
                        "item_type": "field",
                        "properties": {
                            "name": "O_ORDERKEY",
                            "store_length": "4"
                        },
                        "data": []
                    }
                ]
            },
            {
                "item_type": "key",
                "properties": {
                    "key_length": "4",
                    "name": "ORDERS_FK1"
                },
                "data": [
                    {
                        "item_type": "field",
                        "properties": {
                            "name": "O_CUSTKEY",
                            "store_length": "4"
                        },
                        "data": []
                    },
                    {
                        "item_type": "field",
                        "properties": {
                            "name": "O_ORDERKEY",
                            "store_length": "4"
                        },
                        "data": []
                    }
                ]
            }
        ]
    }
]


# 发送单个请求并记录响应时间和内容
def send_request(url, request_data):
    start_time = time.time()
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        end_time = time.time()
        # 解析并打印响应内容
        response_content = response.json()
        print(f"响应内容: {response_content}")
        return end_time - start_time, response_content
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None, None


# 并发发送请求
def run_concurrent_requests(url, json_requests, concurrency, requests_count):
    response_times = []
    responses = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for _ in range(requests_count):
            for request_data in json_requests:
                future = executor.submit(send_request, url, request_data)
                futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            response_time, response_content = future.result()
            if response_time is not None:
                response_times.append(response_time)
                responses.append(response_content)

    return response_times, responses


# 收集系统指标
def collect_system_metrics():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory_percent = psutil.virtual_memory().percent
    disk_io_counters = psutil.disk_io_counters()
    return cpu_percent, memory_percent, disk_io_counters


# 主函数
def main():
    # 收集开始时的系统指标
    start_cpu, start_memory, start_disk_io = collect_system_metrics()

    # 记录开始时间
    start_time = time.time()

    # 并发发送请求
    response_times, responses = run_concurrent_requests(URL, json_requests, CONCURRENCY, REQUESTS_COUNT)

    # 记录结束时间
    end_time = time.time()

    # 收集结束时的系统指标
    end_cpu, end_memory, end_disk_io = collect_system_metrics()

    # 计算总时间
    total_time = end_time - start_time

    # 计算 QPS
    qps = len(response_times) / total_time if total_time > 0 else 0

    # 计算平均响应时间
    avg_response_time = sum(response_times) / len(response_times) if response_times else 0

    # 计算磁盘 I/O 变化
    disk_read_bytes = end_disk_io.read_bytes - start_disk_io.read_bytes
    disk_write_bytes = end_disk_io.write_bytes - start_disk_io.write_bytes

    # 输出测试结果
    print("\n===== 压测结果汇总 =====")
    print(f"并发数: {CONCURRENCY}")
    print(f"请求总数: {REQUESTS_COUNT * len(json_requests)}")
    print(f"总时间: {total_time:.2f} 秒")
    print(f"QPS: {qps:.2f}")
    print(f"平均响应时间: {avg_response_time:.2f} 秒")
    print(f"CPU 使用率: 开始 {start_cpu:.2f}% 结束 {end_cpu:.2f}%")
    print(f"内存使用率: 开始 {start_memory:.2f}% 结束 {end_memory:.2f}%")
    print(f"磁盘读取字节数: {disk_read_bytes}")
    print(f"磁盘写入字节数: {disk_write_bytes}")


if __name__ == "__main__":
    main()
