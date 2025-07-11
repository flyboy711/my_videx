"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""

import argparse
import json
import logging
import os
import requests

from sub_platforms.sql_optimizer.env.rds_env import OpenMySQLEnv
from sub_platforms.sql_optimizer.videx import videx_logging
from sub_platforms.sql_optimizer.videx.videx_metadata import construct_videx_task_meta_from_local_files, \
    fetch_all_meta_with_one_file
from sub_platforms.sql_optimizer.videx.videx_utils import VIDEX_IP_WHITE_LIST


def get_usage_message(args, videx_ip, videx_port, videx_db, videx_user, videx_pwd, videx_server_ip_port):
    base_msg = f"Build env finished. Your VIDEX server is {videx_server_ip_port}."

    mysql57_msg = ("-- Note, if your MySQL version is 5.x, please setup/clear the environment "
                   "before and after your connecting as follows:\n"
                   f"mysql -h{videx_ip} -P{videx_port} -u{videx_user} -p{videx_pwd} < setup_mysql57_env.sql\n"
                   f"mysql -h{videx_ip} -P{videx_port} -u{videx_user} -p{videx_pwd} < clear_mysql57_env.sql\n")

    if args.task_id:
        videx_options = json.dumps({"task_id": args.task_id})
        return (f"{base_msg}\n"
                f"To use VIDEX, please set the following variable before explaining your SQL:\n" + "-" * 20 +
                "\n"
                f"-- Connect VIDEX-MySQL: mysql -h{videx_ip} -P{videx_port} -u{videx_user} -p{videx_pwd} -D{videx_db}\n"
                f"USE {videx_db};\n"
                f"SET @VIDEX_SERVER='{videx_server_ip_port}';\n"
                f"SET @VIDEX_OPTIONS='{videx_options}';\n"
                f"-- EXPLAIN YOUR_SQL;\n"
                f"{mysql57_msg}")
    else:
        return (f"{base_msg}\n"
                f"You are running in non-task mode.\n"
                f"To use VIDEX, please set the following variable before explaining your SQL:\n" + "-" * 20 +
                "\n"
                f"-- Connect VIDEX-MySQL: mysql -h{videx_ip} -P{videx_port} -u{videx_user} -p{videx_pwd} -D{videx_db}\n"
                f"USE {videx_db};\n"
                f"SET @VIDEX_SERVER='{videx_server_ip_port}';\n"
                f"-- EXPLAIN YOUR_SQL;\n"
                f"{mysql57_msg}")


def parse_connection_info(info):
    target_ip, target_port, target_db, target_user, target_pwd = info.split(':')
    return target_ip, int(target_port), target_db, target_user, target_pwd


def parse_connection_file_info(info):
    files_ip, files_port, db_name = info.split(':')
    return files_ip, int(files_port), db_name


def save_metadata_to_file(meta_request, db_name: str, files_server_ip_port: str):
    """
    将元数据保存到指定的文件系统路径
    通过POST请求调用元数据保存服务

    参数:
        meta_request: 需要保存的元数据（字典格式）
        db_name: 数据库名称
        files_server_ip_port: 文件服务IP地址和端口（格式为"ip:port"）

    返回:
        True: 保存成功
        False: 保存失败
    """
    # 构建完整的服务URL
    url = f"http://{files_server_ip_port}/save_metadata"

    # 准备请求参数
    params = {
        "db_name": db_name,
        "files_server_ip_port": files_server_ip_port
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        # 发起POST请求，包含URL参数和JSON数据
        response = requests.post(
            url,
            params=params,
            json=meta_request,  # 自动序列化为JSON
            headers=headers
        )

        # 检查响应状态
        if response.status_code == 200:
            print(f"Successfully saved metadata for {files_server_ip_port},{db_name}")
            return True
        else:
            print(f"Failed to save metadata. Status code: {response.status_code}, Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        # 处理网络错误
        print(f"Network error while saving metadata: {str(e)}")
        return False
    except Exception as e:
        # 处理其他意外错误
        print(f"Unexpected error while saving metadata: {str(e)}")
        return False


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Collect data from target_ins and create videx environment.')
    parser.add_argument('--target', type=str, required=True,
                        help='Connection info for raw instance, in the format of "ip:port:db:user:password"')
    parser.add_argument('--videx', type=str, required=True,
                        help='Connection info for videx instance, in the format of "ip:port:db:user:password"')
    parser.add_argument('--files', type=str, required=True,
                        help='Connection info for file system, in the format of "ip:port:db_name:table_name"')
    parser.add_argument('--tables', type=str, default=None,
                        help='Comma-separated list of table names to fetch. If not provided, fetching all tables. '
                             'e.g. customer,nation')
    parser.add_argument('--meta_path', type=str, default=None,
                        help='meta filepath to save pulled metadata.')
    parser.add_argument('--fetch_method', type=str, default='fetch', help='fetch, partial_fetch, sampling')
    parser.add_argument('--task_id', type=str, default=None,
                        help='task id is to distinguish different videx tasks, if they have same database names.')

    videx_logging.initial_config()
    args = parser.parse_args()

    # step 1: parse arguments
    target_ip, target_port, target_db, target_user, target_pwd = parse_connection_info(args.target)
    target_env = OpenMySQLEnv(ip=target_ip, port=target_port, usr=target_user, pwd=target_pwd, db_name=target_db,
                              read_timeout=300, write_timeout=300, connect_timeout=10)

    if args.videx:
        videx_ip, videx_port, videx_db, videx_user, videx_pwd = parse_connection_info(args.videx)
    else:
        # if no videx, the videx is located to the same instance with target mysql
        videx_ip, videx_port, videx_user, videx_pwd = target_ip, target_port, target_user, target_pwd
        videx_db = f'videx_{target_db}'

    if args.files:
        files_ip, files_port, db_name = parse_connection_file_info(args.files)
        files_server_ip_port = f"{files_ip}:{files_port}"

    if args.tables:
        all_table_names = args.tables.split(',')
    else:
        all_table_names = None  # No restriction, fetch all tables from target database

    if args.meta_path:
        meta_path = args.meta_path
        if os.path.dirname(meta_path):
            os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    else:
        # Load the existing meta file or save it to a file only when the meta_path is explicitly defined.
        meta_path = None
    logging.info(f"metadata file is {meta_path}")

    # step 2: fetch or read metadata and statistics
    task_id = f"task_id_videx_on_{db_name}"

    if args.fetch_method in ['fetch', 'partial_fetch']:
        # N.B.: Fetching NDV and histogram data can be costly. Ensure the target IP is offline or permitted.
        VIDEX_IP_WHITE_LIST.append(target_ip)
        # TODO fetch ndv and histogram may be costly, if partial_fetch, we skip fetching ndv and histogram
        if args.fetch_method == 'partial_fetch':
            pass  # more tests are required before supporting it
        files = fetch_all_meta_with_one_file(meta_path=meta_path,
                                             env=target_env, target_db=target_db, all_table_names=all_table_names,
                                             n_buckets=16, hist_force=True,
                                             hist_mem_size=200000000, drop_hist_after_fetch=True)
        stats_file_dict, hist_file_dict, ndv_single_file_dict, ndv_mulcol_file_dict = files
        meta_request = construct_videx_task_meta_from_local_files(task_id=args.task_id,
                                                                  videx_db=videx_db,
                                                                  stats_file=stats_file_dict,
                                                                  hist_file=hist_file_dict,
                                                                  ndv_single_file=ndv_single_file_dict,
                                                                  ndv_mulcol_file=ndv_mulcol_file_dict,
                                                                  gt_rec_in_ranges_file=None,
                                                                  gt_req_resp_file=None,
                                                                  raise_error=True)

        save_metadata_to_file(meta_request.to_json(), db_name, files_server_ip_port)

    elif args.fetch_method == 'sampling':
        # We will introduce the sampling-based method soon.
        # This method will generate metadata from the sample data.
        # Additionally, the sample data will be employed to estimate the ndv (the number of distinct values) and cardinality.
        raise NotImplementedError
    else:
        raise NotImplementedError(f"Fetching method `{args.fetch_method}` not implemented, "
                                  f"only support `analyze`, `sampling`.")
