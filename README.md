# Docker 环境配置指南

本文档记录了项目所需的 Docker 容器配置和常用命令。

## MySQL 容器

### 拉取 MySQL 镜像
```bash
docker pull mysql:8.0
```

### 运行 MySQL 容器
```bash
docker run --name quanburn_mysql\
  -e MYSQL_ROOT_PASSWORD=quanburn \
  -e MYSQL_DATABASE=quanburn \
  -p 63306:3306 \
  -d mysql:8.0
```

### MySQL 容器管理命令
```bash
# 查看运行中的容器
docker ps

# 停止容器
docker stop mysql-container

# 启动容器
docker start mysql-container

# 删除容器
docker rm mysql-container

# 查看容器日志
docker logs mysql-container
```

## 常用 Docker 命令

### 镜像管理
```bash
# 列出本地镜像
docker images

# 删除镜像
docker rmi image_name

# 构建镜像
docker build -t image_name .
```

### 容器管理
```bash
# 查看所有容器（包括已停止的）
docker ps -a

# 进入容器内部
docker exec -it container_name bash

# 查看容器详细信息
docker inspect container_name
```

### 网络管理
```bash
# 创建网络
docker network create network_name

# 查看网络列表
docker network ls

# 将容器连接到网络
docker network connect network_name container_name
```

## 注意事项

1. 请确保在运行容器前已安装 Docker
2. 生产环境中请使用强密码
3. 建议使用 Docker Compose 来管理多个容器
4. 定期备份重要数据
5. 注意容器端口映射，避免端口冲突

## 环境变量说明

- MYSQL_ROOT_PASSWORD: MySQL root 用户密码
- MYSQL_DATABASE: 初始创建的数据库名称
- MYSQL_USER: 可选，创建新用户
- MYSQL_PASSWORD: 可选，新用户密码 