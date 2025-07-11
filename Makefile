# 获取操作系统类型（兼容不同系统）
DISTRO := $(shell (source /etc/os-release 2>/dev/null; echo $$ID) || echo unknown | tr '[:upper:]' '[:lower:]')

# 目标定义
.PHONY: all all_old all_new build_base build_videx build_optimizer build_server run run_old run_new clean

# 默认行为：Ubuntu 使用一体式，CentOS 使用模块化
all: all_new

# 传统一体式构建路径（适用于所有平台）
all_old: build_base build_videx run_old

# 模块化构建路径（适用于CentOS）
all_new: build_base build_optimizer build_server run_new

# 基础镜像构建
build_base:
ifeq ($(findstring ubuntu,${DISTRO}),ubuntu)
	@echo "Building Ubuntu base image..."
	docker build -t videx_build:latest -f build/Dockerfile.build_env .
else
	@echo "Building CentOS base image..."
	docker build --network=host --platform linux/amd64 -t videx_build_c:latest -f build/Dockerfile.build_c .
endif

# 一体式Videx镜像构建
build_videx:
ifeq ($(findstring ubuntu,${DISTRO}),ubuntu)
	@echo "Building Ubuntu service image..."
	docker build --memory=8g -t videx_ubuntu:latest -f build/Dockerfile.videx ..
else
	@echo "Building CentOS unified service image..."
	docker build --network=host --platform linux/amd64 --memory=8g -t videx_centos:latest -f build/Dockerfile.videx_c ..
endif

# 优化器模块构建
build_optimizer:
ifeq ($(findstring ubuntu,${DISTRO}),ubuntu)
	@echo "Warning: Ubuntu doesn't support optimizer module"
else
	@echo "Building CentOS optimizer plugin..."
	docker build --network=host --platform linux/amd64 --memory=8g -t videx_opt_centos:latest -f build/Dockerfile.videx_optimizer ..
endif

# 服务模块构建
build_server:
ifeq ($(findstring ubuntu,${DISTRO}),ubuntu)
	@echo "Building Ubuntu service image..."
	docker build --memory=8g -t videx_ubuntu:latest -f build/Dockerfile.videx ..
else
	@echo "Building CentOS server module..."
	docker build --network=host --platform linux/amd64 --memory=8g -t videx_ser_centos:latest -f build/Dockerfile.videx_server ..
endif

# 运行一体式构建容器
run_old:
ifeq ($(findstring ubuntu,${DISTRO}),ubuntu)
	@echo "Starting Ubuntu container..."
	docker run -d --name Videx_U -p 13308:13308 -p 5001:5001 -p 5002:5002 videx_ubuntu:latest
else
	@echo "Starting CentOS unified container..."
	docker run -d --name Videx_CentOS -p 13308:13308 -p 5001:5001 -p 5002:5002 videx_centos:latest
endif

# 运行模块化构建容器
run_new:
ifeq ($(findstring ubuntu,${DISTRO}),ubuntu)
	@echo "Starting Ubuntu container..."
	docker run -d --name Videx_U -p 13308:13308 -p 5001:5001 videx_ubuntu:latest
else
	@echo "Starting CentOS modular container..."
	docker run -d --name Videx_Ser_CentOS7 -p 13308:13308 -p 5001:5001 -p 5002:5002 videx_ser_centos:latest
endif

# 通用运行（默认新路径）
run: run_new

# 清理所有资源
clean:
# Ubuntu清理
ifeq ($(findstring ubuntu,${DISTRO}),ubuntu)
	@echo "Cleaning Ubuntu resources..."
	-docker rm -f Videx_U
#	-docker rmi videx_ubuntu:latest
#	-docker rmi videx_build:latest
else
# CentOS清理
	@echo "Cleaning CentOS resources..."
	-docker rm -f Videx_CentOS
	-docker rm -f Videx_Ser_CentOS7
#	-docker rmi videx_centos:latest
#	-docker rmi videx_ser_centos:latest
#	-docker rmi videx_opt_centos:latest
#	-docker rmi videx_build_c:latest
endif