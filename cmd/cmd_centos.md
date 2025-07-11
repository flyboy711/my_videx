## Docker 构建指令

CentOS平台构建指令：
- 构建基础环境镜像
  ```bash
   # cd videx_server
    docker build --network=host --platform linux/amd64 -t videx_build_c:latest -f build/Dockerfile.build_c .
  
  ```

- 构建 Videx 服务镜像
  ```bash
    docker build --network=host --platform linux/amd64 --memory=8g -t videx_centos:latest -f build/Dockerfile.videx_c ..
 
   ```

- 启动 Videx 容器服务
  ```bash
    docker run -d --name Videx_CentOS -p 13308:13308 -p 5001:5001 -p 5002:5002 videx_centos:latest
  
  ```

- 验证是否正确
  ```bash
    mysql -h127.0.0.1 -P13308 -uvidex -ppassword

    show engines;
  ```

  ⚠️：镜像文件可以先本地构建，然后，通过如下指令导出再上传到测试服务器中：
  ```bash
    docker save -o videx_centos.tar videx_centos:latest

    docker load -i /tmp/videx_centos.tar
  ```