# openGemini Server Docker Image

## How to use this image


### start server instance


```shell
docker run -d --name opengemini-server-example opengeminidb/opengemini-server
```

By default, starting above server instance will be run as the default user without password.

### connect to it from gemini cli

```shell
docker exec -it opengemini-server-example ts-cli
```


### stopping / removing the container

```shell
docker stop opengemini-server-example
docker rm opengemini-server-example
```

### networking

You can expose your openGemini server running in docker by mapping a particular port from inside the container using host ports:

```shell
docker run -d -p 8086:8086 --name opengemini-server-example opengeminidb/opengemini-server
```

## Volumes

Typically, you may want to mount the following folders inside your container to achieve persistence:

- /opt/openGemini/ - main folder where openGemini stores the data
- /var/log/openGemini/ - logs

```shell
docker run -d \
    -v $(realpath_data):/opt/openGemini/ \
    -v $(realpath_logs):/var/log/openGemini \
    --name opengemini-server-example opengeminidb/opengemini-server
```
