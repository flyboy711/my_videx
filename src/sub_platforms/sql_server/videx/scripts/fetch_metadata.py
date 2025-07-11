"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""

import argparse
import json
import logging
import re
import requests
import gzip
from pymysql import InternalError

from sub_platforms.sql_optimizer.env.rds_env import Env
from sub_platforms.sql_optimizer.videx.videx_metadata import VidexDBTaskStats
from sub_platforms.sql_optimizer.env.rds_env import OpenMySQLEnv
from sub_platforms.sql_optimizer.videx import videx_logging


def create_videx_env_multi_db(videx_env: Env,
                              meta_dict: dict,
                              new_engine: str = 'VIDEX',
                              ):
    """
    Specify a target database (`target_db`), retrieve metadata, and create it on the `videx_db` within the `videx_env`.

     Args:
        meta_dict: Dictionary containing metadata.
        videx_env: Integrated environment for Videx MySQL and Parse MySQL.
        // meta_dir: Metadata already fetched. Tuple of four elements: info_stats, hist, ndv_single, ndv_multi
        // gt_rec_in_ranges: Used to return gt results for `rec_in_ranges`.
            Each element contains range conditions and gt rows.
            Data is collected from executing explain on innodb, tracing range queries and (gt) rows information.
        // gt_req_resp: Used to return gt for any request.
            Element is a tuple of three: request json, response json, turn_on (whether to enable).
            If a request matches an enabled element, it returns directly.
        new_engine: Name of the engine to be created.
    Returns:

    """
    # Create a test database named after `target_db` in videx-db and save the table schema.
    for target_db, table_dict in meta_dict.items():
        videx_env.execute(f"DROP DATABASE IF EXISTS `{target_db}`")
        videx_env.execute(f"CREATE DATABASE `{target_db}`")

        # `videx_env` might need to create tables in multiple databases;
        # since DDL does not include db_name, switching is required.
        # However, it should revert to the default_db after execution.
        videx_default_db = videx_env.default_db
        try:
            videx_env.set_default_db(target_db)
            for table in table_dict.values():
                create_table_ddl = re.sub(r"ENGINE=\w+", "ENGINE={}".format(new_engine), table.ddl)
                # remove secondary index
                match = re.search(r'SECONDARY_ENGINE=(\w+)', create_table_ddl)
                if match:
                    value = match.group(1)
                    logging.warning(f"find SECONDARY_ENGINE={value}, remove it from CREATE TABLE DDL")
                    create_table_ddl = re.sub(r'SECONDARY_ENGINE=\w+', '', create_table_ddl)
                videx_env.execute(create_table_ddl)
        finally:
            videx_env.set_default_db(videx_default_db)


def post_add_videx_meta(req: VidexDBTaskStats, videx_server_ip_port: str, use_gzip: bool):
    # 1. 将 src_meta 导入videx-py
    json_data = req.to_json().encode('utf-8')
    if use_gzip:
        # 转换 JSON 数据为字符串，并用 UTF-8 编码为 bytes
        json_data = gzip.compress(json_data)  # 使用 gzip 进行压缩
        headers = {'Content-Encoding': 'gzip', 'Content-Type': 'application/json'}
    else:
        headers = {'Content-Type': 'application/json'}
    # send request
    logging.info(f"post videx metadata to {videx_server_ip_port}")
    return requests.post(f'http://{videx_server_ip_port}/create_task_meta', data=json_data, headers=headers)


def load_metadata_from_file(db_name: str, files_server_ip_port: str):
    """
    从文件系统服务加载指定表的元数据信息
    通过HTTP GET请求调用元数据服务

    参数:
        db_name: 数据库名称
        files_server_ip_port: 请求地址

    返回:
        包含元数据的字典 (成功时)
        None (失败时)
    """
    # 元数据服务的URL - 请根据实际情况替换videx_server_ip_port
    url = f'http://{files_server_ip_port}/get_metadata'

    # 准备查询参数
    params = {
        'db_name': db_name,
        'files_server_ip_port': files_server_ip_port
    }

    try:
        # 发起GET请求
        response = requests.get(url, params=params)

        # 检查响应状态
        if response.status_code == 200:
            # 成功返回元数据
            return response.json()
        elif response.status_code == 404:
            # 元数据不存在
            print(f"No metadata found for {files_server_ip_port}_{db_name}")
            return None
        else:
            # 其他错误
            print(f"Metadata request failed: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        # 网络或连接错误
        print(f"Network error while loading metadata: {str(e)}")
        return None


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


if __name__ == "__main__":
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>> >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    parser = argparse.ArgumentParser(description='Collect data from target_ins and create videx environment.')
    parser.add_argument('--videx', type=str, required=True,
                        help='Connection info for videx instance, in the format of "ip:port:db:user:password"')
    parser.add_argument('--files', type=str, required=True,
                        help='Connection info for file system, in the format of "ip:port:db_name:table_name"')
    parser.add_argument('--videx_server', type=str, default="5001",
                        help='Connection info for videx server, in the format of "[ip:]port". '
                             'If not provided, access "{videx_ip}:5001".')
    parser.add_argument('--task_id', type=str, default=None,
                        help='task id is to distinguish different videx tasks, if they have same database names.')

    """
    videx_server_ip_port: IP and port information, Videx MySQL will inform Videx Python about this address and send Videx queries to it.
    task_id: Task ID used to differentiate metadata of different tasks or even different versions of the same task.
    """

    videx_logging.initial_config()
    args = parser.parse_args()

    if args.videx:
        videx_ip, videx_port, videx_db, videx_user, videx_pwd = parse_connection_info(args.videx)

    if args.files:
        files_ip, files_port, db_name = parse_connection_file_info(args.files)
        files_server_ip_port = f"{files_ip}:{files_port}"

    if ':' in args.videx_server:
        videx_server_ip_port = args.videx_server
    else:
        videx_server_ip_port = f"{videx_ip}:{args.videx_server}"

    try:
        videx_env = OpenMySQLEnv(ip=videx_ip, port=videx_port, usr=videx_user, pwd=videx_pwd, db_name=videx_db,
                                 read_timeout=300, write_timeout=300, connect_timeout=10)
    except InternalError as e:
        if f"Unknown database '{videx_db}'" in str(e):
            videx_env = OpenMySQLEnv(ip=videx_ip, port=videx_port, usr=videx_user, pwd=videx_pwd, db_name=None,
                                     read_timeout=300, write_timeout=300, connect_timeout=10)
            videx_env.execute(f"CREATE DATABASE IF NOT EXISTS `{videx_db}`")
            videx_env.set_default_db(videx_db)
        else:
            raise

    # step 2: 统计服务器层从文件系统拉取元数据，创建虚拟表并导入元数据
    task_id = f"task_id_videx_on_{db_name}"
    meta_request = VidexDBTaskStats.from_json(load_metadata_from_file(db_name, files_server_ip_port))

    # 向 VIDEX-MySQL 中建表
    create_videx_env_multi_db(videx_env, meta_dict=meta_request.meta_dict, )
    # 向 VIDEX-Server 中导入数据
    response = post_add_videx_meta(meta_request, videx_server_ip_port=videx_server_ip_port, use_gzip=True)
    assert response.status_code == 200

    logging.info(get_usage_message(args, videx_ip, videx_port, videx_db, videx_user, videx_pwd, videx_server_ip_port))
