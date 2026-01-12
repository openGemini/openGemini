---
title: Deploying on Docker
order: 2
---

This section introduces how to quickly experience openGemini through Docker at first, and then explains how to use openGemini for writing and querying in a Docker environment. If you are not familiar with Docker, you can use the manual installation method to experience it quickly. If you are interested in contributing code to openGemini or are interested in its internal technical implementation, you can download the source code from the [GitHub](https://github.com/openGemini) for building and installation.

## Run with Docker

1. Install the [Docker](https://www.docker.com/products/docker-desktop/) environment

2. Use the latest openGemini container image:

   ```shell
   > docker run -d --name opengemini opengeminidb/opengemini-server:latest
   ```

   Or specify a version of the container image:

   ```shell
   > docker run -d --name opengemini opengeminidb/opengemini-server:v1.0.1
   ```

3. Connect to openGemini cli:

   ```shell
   > docker exec -it opengemini ts-cli
   ```

4. Basic operations

   You can refer to the [basic operations in the manual installation section](./get_started.md#basic-operations)

5. Stop/Delete container

   ```shell
   > docker stop opengemini
   > docker rm opengemini
   ```

6. For more usage, please refer to:

   [Docker Hub Website](https://hub.docker.com/r/opengeminidb/opengemini-server)
