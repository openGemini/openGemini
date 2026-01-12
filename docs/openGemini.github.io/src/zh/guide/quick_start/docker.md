---
title: Docker
order: 2
---

本节首先介绍如何通过 Docker 快速体验 openGemini，然后介绍如何在 Docker 环境下体验 openGemini 的写入和查询功能。如果你不熟悉 Docker，请使用[手动安装](./get_started.md)的方式快速体验。如果您希望为 openGemini 贡献代码或对内部技术实现感兴趣，请参考 [openGemini GitHub](https://github.com/openGemini/openGemini) 主页下载源码构建和安装。

## 使用 docker 体验

1. 安装 [Docker](https://www.docker.com/products/docker-desktop/) 环境。

2. 使用最新的 openGemini 容器镜像：

   ```shell
   docker run -d --name opengemini opengeminidb/opengemini-server:latest
   ```

   或者指定版本的容器镜像：

   ```shell
   docker run -d --name opengemini opengeminidb/opengemini-server:v1.0.1
   ```

3. 使用openGemini cli 连接：

   ```shell
   docker exec -it opengemini ts-cli
   ```

4. 基本操作

   可以参考[安装部署章节的基本操作](./get_started.md#基本操作-ts-cli)

5. 停止/删除容器

   ```shell
   docker stop opengemini
   docker rm opengemini
   ```

6. 更多用法请参考：

   [docker hub官网](https://hub.docker.com/r/opengeminidb/opengemini-server)
