"""
固定线程数的情况下，可视化测试结果
"""

import json
import time
import concurrent.futures
import psutil
import requests
import numpy as np
import threading
import matplotlib.pyplot as plt
from datetime import datetime
import os

# 配置参数
URL = "http://127.0.0.1:5001/ask_videx"  # 替换为实际的 ask_videx 接口地址
CONCURRENCY = 100  # 并发数
REQUESTS_COUNT = 2000  # 请求总数
SAMPLE_INTERVAL = 0.1  # 系统监控采样间隔(秒)
REPORT_FILE = "result/load_test_report.txt"  # 报告文件名
PLOT_FILE = "result/load_test_metrics.png"  # 图表文件名

# 确保图表显示英文，避免中文字体问题
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['DejaVu Sans']  # 使用更兼容的英文字体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# curl 请求中的 JSON 数据（保持不变）
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
    {
        "item_type": "videx_root",
        "properties": {
            "dbname": "videx_1",
            "function": "virtual double ha_videx::scan_time()",
            "table_name": "orders",
            "target_storage_engine": "VIDEX"
        },
        "data": []
    },
    {
        "item_type": "videx_request",
        "properties": {
            "dbname": "videx_1",
            "function": "virtual ha_rows ha_innobase::records_in_range(uint, key_range*, key_range*)",
            "table_name": "orders",
            "target_storage_engine": "VIDEX"
        },
        "data": [
            {
                "item_type": "min_key",
                "properties": {
                    "index_name": "ORDERS_FK1",
                    "length": "4",
                    "operator": ">"
                },
                "data": [
                    {
                        "item_type": "column_and_bound",
                        "properties": {
                            "column": "O_CUSTKEY",
                            "value": "3"
                        },
                        "data": []
                    }
                ]
            },
            {
                "item_type": "max_key",
                "properties": {
                    "index_name": "ORDERS_FK1",
                    "length": "4",
                    "operator": ">"
                },
                "data": [
                    {
                        "item_type": "column_and_bound",
                        "properties": {
                            "column": "O_CUSTKEY",
                            "value": "3"
                        },
                        "data": []
                    }
                ]
            }
        ]
    },

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


# 定义监控数据结构
class SystemMetrics:
    def __init__(self):
        self.cpu = []
        self.mem = []
        self.cpu_min = float('inf')
        self.cpu_max = 0
        self.cpu_avg = 0
        self.mem_min = float('inf')
        self.mem_max = 0
        self.mem_avg = 0
        self.start_time = time.time()


class PerformanceMonitor(threading.Thread):
    """后台线程，定期收集系统指标"""

    def __init__(self, interval):
        super().__init__()
        self.interval = interval
        self.metrics = SystemMetrics()
        self.running = True
        self.daemon = True

    def run(self):
        while self.running:
            # 获取CPU和内存使用率
            cpu_percent = psutil.cpu_percent(interval=self.interval)
            mem_percent = psutil.virtual_memory().percent

            # 记录当前值
            self.metrics.cpu.append(cpu_percent)
            self.metrics.mem.append(mem_percent)

            # 更新最小值
            if cpu_percent < self.metrics.cpu_min:
                self.metrics.cpu_min = cpu_percent
            if mem_percent < self.metrics.mem_min:
                self.metrics.mem_min = mem_percent

            # 更新最大值
            if cpu_percent > self.metrics.cpu_max:
                self.metrics.cpu_max = cpu_percent
            if mem_percent > self.metrics.mem_max:
                self.metrics.mem_max = mem_percent

            # 提前退出检查
            if not self.running:
                break

    def stop(self):
        self.running = False
        # 计算平均值
        if self.metrics.cpu:
            self.metrics.cpu_avg = sum(self.metrics.cpu) / len(self.metrics.cpu)
        if self.metrics.mem:
            self.metrics.mem_avg = sum(self.metrics.mem) / len(self.metrics.mem)
        self.metrics.end_time = time.time()
        return self.metrics


# 发送单个请求并记录响应时间和内容
def send_request(url, request_data):
    start_time = time.time()
    success = False
    status_code = None

    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, json=request_data, timeout=10)
        status_code = response.status_code
        response.raise_for_status()
        end_time = time.time()
        response_time = end_time - start_time
        success = True

        # 仅在DEBUG模式下打印响应内容
        if os.getenv('DEBUG') == '1':
            print(f"成功响应: {response.json()} | 耗时: {response_time:.4f}s")

        return success, status_code, response_time, response.json()

    except requests.Timeout:
        return False, 504, time.time() - start_time, "请求超时"
    except requests.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            status_code = e.response.status_code
        else:
            status_code = 500
        return False, status_code, time.time() - start_time, f"请求失败: {str(e)}"


# 并发发送请求
def run_concurrent_requests(url, json_requests, concurrency, requests_count):
    # 启动性能监控
    monitor = PerformanceMonitor(SAMPLE_INTERVAL)
    monitor.start()

    # 初始化统计数据结构
    response_times = []
    status_code_distribution = {}
    all_responses = []
    successful_count = 0
    errors = []

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for _ in range(requests_count):
            for request_data in json_requests:
                future = executor.submit(send_request, url, request_data)
                futures.append(future)

        # 收集每个请求的结果
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            success, status_code, response_time, response = future.result()

            # 记录统计信息
            response_times.append(response_time)
            all_responses.append(response)

            # 更新状态码分布
            status_code_distribution[status_code] = status_code_distribution.get(status_code, 0) + 1

            if success:
                successful_count += 1
            else:
                errors.append(f"请求 {i + 1} 失败 [状态码: {status_code}]: {response}")

    end_time = time.time()
    total_duration = end_time - start_time

    # 停止性能监控并获取结果
    metrics = monitor.stop()

    # 计算性能指标
    min_time = min(response_times) if response_times else 0
    max_time = max(response_times) if response_times else 0
    avg_time = sum(response_times) / len(response_times) if response_times else 0

    # 分位数计算
    quantiles = {
        '50% (median)': 0,
        '90%': 0,
        '95%': 0,
        '99%': 0
    }

    if response_times:
        sorted_times = sorted(response_times)
        quantiles['50% (median)'] = np.percentile(sorted_times, 50)
        quantiles['90%'] = np.percentile(sorted_times, 90)
        quantiles['95%'] = np.percentile(sorted_times, 95)
        quantiles['99%'] = np.percentile(sorted_times, 99)

    # 吞吐量计算
    rps = len(response_times) / total_duration if total_duration > 0 else 0
    error_rate = (len(response_times) - successful_count) / len(response_times) * 100 if response_times else 0

    return {
        'total_requests': len(response_times),
        'successful_count': successful_count,
        'error_rate': error_rate,
        'errors': errors,
        'start_time': start_time,
        'end_time': end_time,
        'total_duration': total_duration,
        'min_response_time': min_time,
        'max_response_time': max_time,
        'avg_response_time': avg_time,
        'response_time_quantiles': quantiles,
        'rps': rps,
        'status_code_distribution': status_code_distribution,
        'response_times': response_times,
        'all_responses': all_responses,
        'system_metrics': metrics
    }


def analyze_disk_io():
    """收集磁盘I/O统计信息"""
    start_io = psutil.disk_io_counters()
    time.sleep(1)  # 短暂的等待以获取有意义的变化
    end_io = psutil.disk_io_counters()

    disk_io = {
        'read_bytes': end_io.read_bytes - start_io.read_bytes,
        'write_bytes': end_io.write_bytes - start_io.write_bytes,
        'read_count': end_io.read_count - start_io.read_count,
        'write_count': end_io.write_count - start_io.write_count,
        'read_time': end_io.read_time - start_io.read_time,
        'write_time': end_io.write_time - start_io.write_time
    }

    return disk_io


def generate_performance_report(results):
    """生成详细的性能报告"""
    # 获取系统指标
    sys_metrics = results['system_metrics']

    report_lines = [
        "\n===== Performance Test Report =====",
        f"Test Time: {datetime.fromtimestamp(results['start_time']).strftime('%Y-%m-%d %H:%M:%S')}",
        f"API Endpoint: {URL}",
        f"Concurrency: {CONCURRENCY}",
        f"Total Requests: {results['total_requests']}",
        f"Successful Requests: {results['successful_count']}",
        f"Error Rate: {results['error_rate']:.2f}%",
        f"Test Duration: {results['total_duration']:.2f} seconds",
        f"QPS: {results['rps']:.2f}",
        "",
        "===== System Resource Usage =====",
        f"CPU Usage: Min {sys_metrics.cpu_min:.2f}%, Max {sys_metrics.cpu_max:.2f}%, Avg {sys_metrics.cpu_avg:.2f}%",
        f"Memory Usage: Min {sys_metrics.mem_min:.2f}%, Max {sys_metrics.mem_max:.2f}%, Avg {sys_metrics.mem_avg:.2f}%",
        "",
        "===== Response Time Statistics =====",
        f"Min Response Time: {results['min_response_time']:.4f} seconds",
        f"Max Response Time: {results['max_response_time']:.4f} seconds",
        f"Avg Response Time: {results['avg_response_time']:.4f} seconds",
    ]

    # 添加分位数统计
    report_lines.append("\n----- Response Time Percentiles -----")
    for percentile, value in results['response_time_quantiles'].items():
        report_lines.append(f"{percentile}: {value:.4f} seconds")

    # 添加状态码分布
    report_lines.append("\n===== HTTP Status Code Distribution =====")
    for code, count in results['status_code_distribution'].items():
        report_lines.append(f"Status Code {code}: {count} requests ({count / results['total_requests'] * 100:.2f}%)")

    # 添加错误详情
    if results['errors']:
        report_lines.append("\n===== Error Details =====")
        for error in results['errors'][:10]:  # 仅显示前10个错误
            report_lines.append(error)
        if len(results['errors']) > 10:
            report_lines.append(f"... and {len(results['errors']) - 10} more errors")

    return "\n".join(report_lines)


def plot_performance_metrics(results):
    """生成性能图表"""
    plt.figure(figsize=(16, 14))

    # System Resource Usage
    plt.subplot(3, 2, 1)
    cpu_data = results['system_metrics'].cpu
    mem_data = results['system_metrics'].mem
    x = np.linspace(0, results['total_duration'], len(cpu_data))

    plt.plot(x, cpu_data, 'r-', label='CPU (%)')
    plt.plot(x, mem_data, 'b-', label='Memory (%)')

    plt.xlabel('Time (seconds)')
    plt.ylabel('Usage (%)')
    plt.title('System Resource Usage')
    plt.legend()
    plt.grid(True)

    # Response Time Distribution
    plt.subplot(3, 2, 2)
    n, bins, patches = plt.hist(results['response_times'], bins=50, alpha=0.7, color='skyblue')

    plt.xlabel('Response Time (seconds)')
    plt.ylabel('Number of Requests')
    plt.title('Response Time Distribution')
    plt.grid(True)

    # 在分布图上添加均值线
    if results['response_times']:
        avg = results['avg_response_time']
        plt.axvline(avg, color='red', linestyle='dashed', linewidth=1)
        plt.text(avg * 1.05, max(n) * 0.9, f'Avg: {avg:.4f}s', color='red')

    # Response Time Percentiles
    plt.subplot(3, 2, 3)
    quantiles = results['response_time_quantiles']
    labels = ['Min', '50%', '90%', '95%', '99%', 'Max']
    values = [
        results['min_response_time'],
        quantiles['50% (median)'],
        quantiles['90%'],
        quantiles['95%'],
        quantiles['99%'],
        results['max_response_time']
    ]

    # 使用不同颜色
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    plt.bar(labels, values, color=colors)

    plt.xlabel('Percentile')
    plt.ylabel('Response Time (seconds)')
    plt.title('Response Time Percentiles')
    plt.grid(True)

    # 添加数值标签
    for i, v in enumerate(values):
        plt.text(i, v + (max(values) * 0.01), f'{v:.4f}', ha='center', fontsize=9)

    # Status Code Distribution
    plt.subplot(3, 2, 4)
    codes = list(results['status_code_distribution'].keys())
    counts = list(results['status_code_distribution'].values())

    plt.bar([str(c) for c in codes], counts, color='green')

    plt.xlabel('HTTP Status Code')
    plt.ylabel('Number of Requests')
    plt.title('Status Code Distribution')
    plt.grid(True)

    # 添加数值标签
    for i, v in enumerate(counts):
        plt.text(i, v + 0.5, str(v), ha='center')

    # System Usage Histogram
    plt.subplot(3, 2, 5)
    plt.hist(results['system_metrics'].cpu, bins=20, alpha=0.7, color='red', label='CPU')
    plt.xlabel('CPU Usage (%)')
    plt.ylabel('Count')
    plt.title('CPU Usage Distribution')
    plt.grid(True)

    plt.subplot(3, 2, 6)
    plt.hist(results['system_metrics'].mem, bins=20, alpha=0.7, color='blue', label='Memory')
    plt.xlabel('Memory Usage (%)')
    plt.ylabel('Count')
    plt.title('Memory Usage Distribution')
    plt.grid(True)

    plt.tight_layout()

    # 保存高质量图片
    plt.savefig(PLOT_FILE, dpi=300)
    print(f"Performance chart saved to: {PLOT_FILE}")


# 主函数
def main():
    print(f"Starting load test: {URL}")
    print(f"Configuration: {CONCURRENCY} concurrency, {REQUESTS_COUNT} total requests")

    # 收集磁盘I/O信息
    disk_io = analyze_disk_io()

    # 执行压测
    results = run_concurrent_requests(URL, json_requests, CONCURRENCY, REQUESTS_COUNT)

    # 生成报告
    report = generate_performance_report(results)
    print(report)

    # 保存报告到文件
    with open(REPORT_FILE, 'w') as f:
        f.write(report)
    print(f"Detailed report saved to: {REPORT_FILE}")

    # 添加磁盘I/O到报告
    disk_io_report = [
        "\n===== Disk I/O Statistics =====",
        f"Read Bytes: {disk_io['read_bytes']}",
        f"Write Bytes: {disk_io['write_bytes']}",
        f"Read Count: {disk_io['read_count']}",
        f"Write Count: {disk_io['write_count']}",
        f"Read Time (ms): {disk_io['read_time']}",
        f"Write Time (ms): {disk_io['write_time']}"
    ]

    with open(REPORT_FILE, 'a') as f:
        f.write("\n")
        f.write("\n".join(disk_io_report))

    # 生成可视化图表
    if results['response_times'] and results['system_metrics'].cpu:
        plot_performance_metrics(results)
    else:
        print("Warning: No response time or system metrics data available, skipping chart generation")


if __name__ == "__main__":
    main()
