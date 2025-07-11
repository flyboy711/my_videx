import json
import time
import concurrent.futures
import psutil
import requests
import numpy as np
import threading
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import json

# 全局设置
plt.rcParams['font.family'] = 'DejaVu Sans'  # 兼容性更好的字体
plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号

# 配置参数
URL = "http://127.0.0.1:5001/ask_videx"  # 替换为实际的接口地址
CONCURRENCY_LEVELS = [10, 50, 100, 150, 200]
REQUESTS_PER_LEVEL = 2000  # 每个并发级别的请求总数
SAMPLE_INTERVAL = 0.05  # 系统监控采样间隔(秒)


class SystemMetrics:
    """系统性能指标收集器"""

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
    """后台性能监控线程"""

    def __init__(self, interval):
        super().__init__()
        self.interval = interval
        self.metrics = SystemMetrics()
        self.running = True
        self.daemon = True

    def run(self):
        while self.running:
            cpu_percent = psutil.cpu_percent(interval=self.interval)
            mem_percent = psutil.virtual_memory().percent

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


def send_request(url, request_data):
    """发送单个请求并记录指标"""
    start_time = time.time()
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, json=request_data, timeout=10)
        response.raise_for_status()
        return True, response.status_code, time.time() - start_time
    except requests.Timeout:
        return False, 504, time.time() - start_time
    except requests.RequestException as e:
        status_code = e.response.status_code if hasattr(e, 'response') and e.response else 500
        return False, status_code, time.time() - start_time


def run_load_test(concurrency, total_requests):
    """执行指定并发级别的负载测试"""
    print(f"\n🚀 Starting load test with concurrency: {concurrency}, requests: {total_requests}")

    # 启动性能监控
    monitor = PerformanceMonitor(SAMPLE_INTERVAL)
    monitor.start()

    # 初始化统计数据结构
    response_times = []
    successful_count = 0
    error_count = 0
    status_codes = {}

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for _ in range(total_requests):
            for request_data in json_requests:
                future = executor.submit(send_request, URL, request_data)
                futures.append(future)

        # 收集每个请求的结果
        for future in concurrent.futures.as_completed(futures):
            success, status_code, response_time = future.result()

            response_times.append(response_time)
            status_codes[status_code] = status_codes.get(status_code, 0) + 1

            if success:
                successful_count += 1
            else:
                error_count += 1

    end_time = time.time()
    total_duration = end_time - start_time

    # 停止性能监控并获取结果
    metrics = monitor.stop()

    # 计算性能指标
    min_time = min(response_times) if response_times else 0
    max_time = max(response_times) if response_times else 0
    avg_time = sum(response_times) / len(response_times) if response_times else 0
    qps = total_requests / total_duration if total_duration > 0 else 0
    error_rate = (error_count / total_requests * 100) if total_requests > 0 else 0

    # 分位数计算
    quantiles = {'p50': 0, 'p90': 0, 'p95': 0, 'p99': 0}
    if response_times:
        sorted_times = sorted(response_times)
        quantiles['p50'] = np.percentile(sorted_times, 50)
        quantiles['p90'] = np.percentile(sorted_times, 90)
        quantiles['p95'] = np.percentile(sorted_times, 95)
        quantiles['p99'] = np.percentile(sorted_times, 99)

    # 返回结果
    return {
        'concurrency': concurrency,
        'total_requests': total_requests,
        'successful_count': successful_count,
        'error_count': error_count,
        'error_rate': error_rate,
        'start_time': start_time,
        'end_time': end_time,
        'total_duration': total_duration,
        'min_response_time': min_time,
        'max_response_time': max_time,
        'avg_response_time': avg_time,
        'qps': qps,
        'response_time_quantiles': quantiles,
        'cpu_min': metrics.cpu_min,
        'cpu_max': metrics.cpu_max,
        'cpu_avg': metrics.cpu_avg,
        'mem_min': metrics.mem_min,
        'mem_max': metrics.mem_max,
        'mem_avg': metrics.mem_avg,
        'status_codes': status_codes
    }


def plot_comparison_results(results):
    """绘制不同并发级别的对比图表"""
    plt.figure(figsize=(15, 18))

    # 准备数据
    concurrency = [r['concurrency'] for r in results]
    qps = [r['qps'] for r in results]
    avg_response = [r['avg_response_time'] * 1000 for r in results]  # 转换为毫秒
    cpu_avg = [r['cpu_avg'] for r in results]
    mem_avg = [r['mem_avg'] for r in results]
    error_rates = [r['error_rate'] for r in results]

    # 1. QPS和错误率对比
    plt.subplot(4, 1, 1)
    fig1 = plt.gca()
    fig1.set_title('QPS and Error Rate by Concurrency Level', fontsize=14)

    # QPS - 左轴
    fig1.set_xlabel('Concurrent Threads')
    fig1.set_ylabel('QPS (Requests/Sec)', color='blue')
    fig1.plot(concurrency, qps, 'o-', markersize=8, linewidth=2, color='blue')
    fig1.tick_params(axis='y', labelcolor='blue')
    fig1.grid(True, linestyle='--', alpha=0.7)

    # 错误率 - 右轴
    fig2 = fig1.twinx()
    fig2.set_ylabel('Error Rate (%)', color='red')
    fig2.plot(concurrency, error_rates, 's-', markersize=8, linewidth=2, color='red')
    fig2.tick_params(axis='y', labelcolor='red')

    # 添加数据标签
    for i, v in enumerate(qps):
        fig1.text(concurrency[i], v, f'{v:.1f}', ha='center', va='bottom', fontsize=9)

    for i, v in enumerate(error_rates):
        fig2.text(concurrency[i], v + 0.2, f'{v:.1f}%', ha='center', va='bottom', fontsize=9, color='red')

    # 2. 响应时间对比
    plt.subplot(4, 1, 2)
    avg_response_patch = mpatches.Patch(color='blue', label='Avg Response Time')
    p95_patch = mpatches.Patch(color='green', label='P95 Response Time')
    p99_patch = mpatches.Patch(color='red', label='P99 Response Time')
    plt.legend(handles=[avg_response_patch, p95_patch, p99_patch], loc='upper left')

    # 平均响应时间
    plt.plot(concurrency, avg_response, 'o-', markersize=8, linewidth=2, color='blue')

    # P95响应时间
    p95_responses = [r['response_time_quantiles']['p95'] * 1000 for r in results]
    plt.plot(concurrency, p95_responses, 's-', markersize=8, linewidth=2, color='green')

    # P99响应时间
    p99_responses = [r['response_time_quantiles']['p99'] * 1000 for r in results]
    plt.plot(concurrency, p99_responses, 'D-', markersize=8, linewidth=2, color='red')

    plt.title('Response Time Metrics by Concurrency Level', fontsize=14)
    plt.xlabel('Concurrent Threads')
    plt.ylabel('Response Time (ms)')
    plt.grid(True, linestyle='--', alpha=0.7)

    # 添加数据标签
    for i, v in enumerate(avg_response):
        plt.text(concurrency[i], v, f'{v:.1f}ms', ha='center', va='bottom', fontsize=8)

    for i, v in enumerate(p95_responses):
        plt.text(concurrency[i], v + 10, f'P95:{v:.1f}ms', ha='center', va='bottom', fontsize=8, color='green')

    for i, v in enumerate(p99_responses):
        plt.text(concurrency[i], v + 20, f'P99:{v:.1f}ms', ha='center', va='bottom', fontsize=8, color='red')

    # 3. 资源使用率对比
    plt.subplot(4, 1, 3)
    fig3 = plt.gca()
    fig3.set_title('System Resource Usage by Concurrency Level', fontsize=14)

    # CPU使用率 - 左轴
    fig3.set_xlabel('Concurrent Threads')
    fig3.set_ylabel('CPU Usage (%)', color='purple')
    fig3.plot(concurrency, cpu_avg, 'o-', markersize=8, linewidth=2, color='purple')
    fig3.tick_params(axis='y', labelcolor='purple')
    fig3.grid(True, linestyle='--', alpha=0.7)

    # 内存使用率 - 右轴
    fig4 = fig3.twinx()
    fig4.set_ylabel('Memory Usage (%)', color='orange')
    fig4.plot(concurrency, mem_avg, 's-', markersize=8, linewidth=2, color='orange')
    fig4.tick_params(axis='y', labelcolor='orange')

    # 添加数据标签
    for i, v in enumerate(cpu_avg):
        fig3.text(concurrency[i], v, f'{v:.1f}%', ha='center', va='bottom', fontsize=9, color='purple')

    for i, v in enumerate(mem_avg):
        fig4.text(concurrency[i], v, f'{v:.1f}%', ha='center', va='bottom', fontsize=9, color='orange')

    # 4. 状态码分布
    plt.subplot(4, 1, 4)

    # 创建状态码堆叠图
    status_categories = {
        200: '#4CAF50',  # 成功 - 绿色
        201: '#8BC34A',  # 创建成功 - 浅绿
        204: '#CDDC39',  # 无内容 - 黄绿
        400: '#FFC107',  # 错误请求 - 黄色
        401: '#FF9800',  # 未授权 - 橙色
        403: '#FF5722',  # 禁止访问 - 深橙
        404: '#795548',  # 未找到 - 棕色
        500: '#F44336',  # 服务器错误 - 红色
        502: '#E91E63',  # 网关错误 - 粉色
        503: '#9C27B0',  # 服务不可用 - 紫色
        504: '#673AB7'  # 网关超时 - 深紫
    }

    # 获取所有状态码并按频率排序
    all_codes = set()
    for result in results:
        for code in result['status_codes']:
            all_codes.add(code)
    sorted_codes = sorted(all_codes)

    # 为每个并发级别创建状态码计数列表
    bar_width = 70  # 条形宽度
    x_positions = np.arange(len(concurrency)) * bar_width * 1.5

    bottoms = np.zeros(len(concurrency))

    for code in sorted_codes:
        counts = []
        for result in results:
            counts.append(result['status_codes'].get(code, 0))

        color = status_categories.get(code, '#CCCCCC')  # 默认灰色
        label = f'{code}'

        plt.bar(x_positions, counts, bar_width, bottom=bottoms, color=color, label=label)
        bottoms += counts

    plt.title('Status Code Distribution by Concurrency Level', fontsize=14)
    plt.xlabel('Concurrent Threads')
    plt.ylabel('Number of Responses')
    plt.xticks(x_positions, concurrency)
    plt.grid(True, linestyle='--', alpha=0.7, axis='y')

    # 添加图例
    plt.legend(title='Status Codes', bbox_to_anchor=(1.05, 1), loc='upper left')

    # 添加总数标签
    for i, total in enumerate(bottoms):
        plt.text(x_positions[i], total + 5, f'Total: {total:.0f}',
                 ha='center', fontsize=9)

    plt.tight_layout()

    # 保存结果
    plot_file = "result/concurrency_comparison.png"
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Comparison plot saved to: {plot_file}")

    return plot_file


def save_results_to_excel(results, filename="执行结果记录/concurrency_test_results.xlsx"):
    """将测试结果保存到Excel文件"""
    data = []

    for r in results:
        # 格式化状态码分布
        status_distribution = json.dumps(r['status_codes'])

        # 添加统计
        success_rate = (r['successful_count'] / r['total_requests'] * 100) if r['total_requests'] > 0 else 0
        failure_rate = 100 - success_rate

        data.append({
            'Concurrency Level': r['concurrency'],
            'Total Requests': r['total_requests'],
            'Successful Requests': r['successful_count'],
            'Failed Requests': r['error_count'],
            'Success Rate (%)': f"{success_rate:.2f}",
            'Error Rate (%)': f"{r['error_rate']:.2f}",
            'Test Duration (s)': f"{r['total_duration']:.2f}",
            'QPS': f"{r['qps']:.2f}",
            'Min Response Time (ms)': f"{r['min_response_time'] * 1000:.2f}",
            'Avg Response Time (ms)': f"{r['avg_response_time'] * 1000:.2f}",
            'Max Response Time (ms)': f"{r['max_response_time'] * 1000:.2f}",
            'P50 (ms)': f"{r['response_time_quantiles']['p50'] * 1000:.2f}",
            'P90 (ms)': f"{r['response_time_quantiles']['p90'] * 1000:.2f}",
            'P95 (ms)': f"{r['response_time_quantiles']['p95'] * 1000:.2f}",
            'P99 (ms)': f"{r['response_time_quantiles']['p99'] * 1000:.2f}",
            'Min CPU (%)': f"{r['cpu_min']:.1f}",
            'Avg CPU (%)': f"{r['cpu_avg']:.1f}",
            'Max CPU (%)': f"{r['cpu_max']:.1f}",
            'Min Memory (%)': f"{r['mem_min']:.1f}",
            'Avg Memory (%)': f"{r['mem_avg']:.1f}",
            'Max Memory (%)': f"{r['mem_max']:.1f}",
            'Status Code Distribution': status_distribution
        })

    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    print(f"\n📊 Results saved to Excel file: {filename}")
    return filename


def main():
    print(f"Starting performance comparison test")

    all_results = []

    # 运行所有并发级别测试
    for concurrency in CONCURRENCY_LEVELS:
        result = run_load_test(concurrency, REQUESTS_PER_LEVEL)
        all_results.append(result)

    # 绘制对比图表
    plot_file = plot_comparison_results(all_results)

    # 保存详细结果到Excel
    # excel_file = save_results_to_excel(all_results)

    print("\n==================================")
    print("🚀 Performance comparison complete!")
    print("==================================")


if __name__ == "__main__":
    # 定义测试请求数据（根据实际情况修改）
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

    main()
