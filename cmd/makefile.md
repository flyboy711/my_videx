## 通过 Make 指令自适应平台构建
在本地终端中安装make，如mac中安装：`brew install make`.

- 完整构建指令
```bash
# Ubuntu 默认（一体式构建）
make all_old    # 或直接 make（默认使用 new，但 Ubuntu 会转为 old）

# CentOS 传统一体式构建
make all_old    # build_base → build_videx → run_old

# CentOS 模块化构建
make all_new    # build_base → build_optimizer → build_server → run_new
```

- 分步构建指令：
```bash
# 仅构建基础镜像
make build_base

# 构建一体式服务镜像（Ubuntu/CentOS通用）
make build_videx

# 构建优化器模块（仅CentOS）
make build_optimizer

# 构建服务模块
make build_server

# 运行一体式容器
make run_old

# 运行模块化容器
make run_new
```

- 额外功能 ：
```bash
# 清理相应系统的容器和镜像
make clean    
```

- 注意事项 ：
确保在项目根目录下运行（cd videx_server）


## 测试

- 验证是否正确
  ```bash
    mysql -h127.0.0.1 -P13308 -uvidex -ppassword

    show engines;
  ```
  
导入数据进行测试：
- 拉取元数据
```python
cd data/tpch_tiny

# 创建数据库
mysql -h127.0.0.1 -P13308 -uvidex -ppassword -e "create database tpch_tiny;"

mysql -h127.0.0.1 -P13308 -uvidex -ppassword -Dtpch_tiny < tpch_tiny.sql


python src/sub_platforms/sql_optimizer/videx/scripts/videx_build_env.py \
 --target 127.0.0.1:13308:tpch_tiny:videx:password \
 --videx 127.0.0.1:13308:videx_1:videx:password \
 --files 127.0.0.1:5002:videx_1


python src/sub_platforms/sql_server/videx/scripts/fetch_metadata.py \
       --files 127.0.0.1:5002:videx_1 \
       --videx 127.0.0.1:13308:videx_1:videx:password
        

```