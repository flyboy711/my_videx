import argparse

from flask import Flask, request
import json
import os

app = Flask(__name__)


@app.route('/save_metadata', methods=['POST'])
def save_metadata():
    try:
        db_name = request.args.get('db_name')
        files_server_ip_port = request.args.get('files_server_ip_port')

        # 验证参数
        if not db_name or not files_server_ip_port:
            return "Missing db_name or files_server_ip_port parameters", 400

        # 检查参数有效性（防止路径遍历攻击）
        if not db_name.isidentifier() and not files_server_ip_port.isidentifier():
            return "Invalid db_name or files_server_ip_port", 400

        # 从POST请求体中获取元数据JSON
        metadata = request.get_json()
        if metadata is None:
            return "Invalid JSON data in request body", 400

        # 使用参数拼接文件名
        file_path = f"metadata_{files_server_ip_port}_{db_name}.json"

        # 保存/更新元数据文件
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                existing_metadata = json.load(f)
            existing_metadata.update(metadata)
        else:
            existing_metadata = metadata

        with open(file_path, 'w') as f:
            json.dump(existing_metadata, f, indent=4)

        return f"Metadata for {db_name} saved successfully.", 200
    except Exception as e:
        return f"Error saving metadata: {str(e)}", 500


@app.route('/get_metadata', methods=['GET'])
def get_metadata():
    try:
        # 从查询参数获取 db_name
        db_name = request.args.get('db_name')
        files_server_ip_port = request.args.get('files_server_ip_port')

        # 检查参数是否存在
        if not db_name or not files_server_ip_port:
            return "Missing db_name or files_server_ip_port parameters", 400

        # 检查参数有效性（防止路径遍历攻击）
        if not db_name.isidentifier() and files_server_ip_port.isidentifier():
            return "Invalid db_name or files_server_ip_port", 400

        # 安全拼接文件名
        file_path = f"metadata_{files_server_ip_port}_{db_name}.json"

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                metadata = json.load(f)
            return json.dumps(metadata), 200
        else:
            return f"No metadata available for {db_name}", 404
    except Exception as e:
        return f"Error getting metadata: {str(e)}", 500


if __name__ == "__main__":
    """
    Examples:
        python start_videx_files.py --port 5002
    """
    parser = argparse.ArgumentParser(description='Start the Videx Files server.')
    parser.add_argument('--server_ip', type=str, default='0.0.0.0', help='The IP address to bind the server to.')
    parser.add_argument('--debug', action='store_true', help='Run the server in debug mode.')
    parser.add_argument('--port', type=int, default=5002, help='The port number to run the server on.')

    args = parser.parse_args()

    app.run(debug=args.debug, threaded=True, host=args.server_ip, port=args.port, use_reloader=False)
