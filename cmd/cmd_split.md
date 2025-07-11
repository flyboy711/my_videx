## Docker 构建指令

CentOS 平台构建指令：

- 构建基础环境镜像
  ```bash
   # cd videx_server
    docker build --network=host --platform linux/amd64 -t videx_build_c:latest -f build/Dockerfile.build_c .

  ```
- 构建 Videx 插件镜像
  ```bash
    docker build --network=host --platform linux/amd64 --memory=8g -t videx_opt_centos:latest -f build/Dockerfile.videx_optimizer ..
    
  ```
- 构建 Videx 服务镜像
  ```bash
    docker build --network=host --platform linux/amd64 --memory=8g -t videx_ser_centos:latest -f build/Dockerfile.videx_server ..

  ```

- 启动 Videx 容器服务
  ```bash
    docker run -d --name Videx_Ser_CentOS7 -p 13308:13308 -p 5001:5001 -p 5002:5002 videx_ser_centos7:latest
    
  ```

- 验证是否正确
  ```bash
    mysql -h127.0.0.1 -P13308 -uvidex -ppassword

    show engines;
  ```

  ⚠️：镜像文件可以先本地构建，然后，通过如下指令导出再上传到测试服务器中：
  ```bash
    docker save -o videx_ser_centos7.tar videx_ser_centos:latest

    docker load -i /tmp/videx_ser_centos7.tar
  ```

- 拉取元数据
```python
cd data/tpch_tiny

tar xf tpch_tiny.sql.tar.gz

# 创建数据库
mysql -h127.0.0.1 -P13308 -uvidex -ppassword -e "create database tpch_tiny;"

mysql -h127.0.0.1 -P13308 -uvidex -ppassword -Dtpch_tiny < tpch_tiny.sql


python src/sub_platforms/sql_optimizer/videx/scripts/videx_build_env.py \
 --target 127.0.0.1:13308:tpch_tiny:videx:password \
 --videx 127.0.0.1:13308:videx_2:videx:password \
 --files 127.0.0.1:5002:videx_2


python src/sub_platforms/sql_server/videx/scripts/fetch_metadata.py \
       --files 127.0.0.1:5002:videx_2 \
       --videx 127.0.0.1:13308:videx_2:videx:password
        

```