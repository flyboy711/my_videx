## Docker 部署方式
👇下面是官方提供的基于Docker部署安装Videx的执行指令。

Ubuntu系统构建指令：
- 构建基础环境镜像
  ```bash
   # cd videx_server
    docker build -t videx_build:latest -f build/Dockerfile.build_u .

  ```

- 构建 Videx 服务镜像
  ```bash
    docker build --memory=8g -t videx_ubuntu:latest -f build/Dockerfile.videx ..
  
  ```

- 启动 Videx 容器服务
  ```bash
    docker run -d --name Videx_U -p 13308:13308 -p 5001:5001 -p 5002:5002 videx_ubuntu:latest
  
  ```

- 验证是否正确
  ```bash
    mysql -h127.0.0.1 -P13308 -uvidex -ppassword

    show engines;
  ```

  ⚠️：镜像文件可以先本地构建，然后，通过如下指令导出再上传到测试服务器中：
  ```bash
    docker save -o videx_ubuntu.tar videx_ubuntu:latest

    docker load -i /tmp/videx_ubuntu.tar
  ```