---
title: 启用HTTPS
order: 3
---


# 启用HTTPS

以下是 openGemini 开启 HTTPS 的步骤：

## 生成证书和密钥

首先，您需要生成 SSL 证书和密钥。您可以使用 openssl 命令生成。

```bash
openssl req -x509 -nodes -newkey rsa:2048 -keyout /etc/ssl/openGemini-selfsigned.key -out /etc/ssl/openGemini-selfsigned.crt -days 365 -subj "/C=US/ST=CA/L=San Francisco/O=openGemini/OU=openGemini/CN=localhost"
```

以上命令会生成自签名的证书和密钥，并指定了有效期为 365 天。请根据您的需要修改参数。

## 修改 openGemini 配置文件

接下来，您需要修改 openGemini 的配置文件。在配置文件中，找到 `[http]` 部分，并添加以下内容：

```toml
[http]
bind-address = "127.0.0.1:8086"
https-enabled = true
https-certificate = "/etc/ssl/openGemini-selfsigned.crt"
https-private-key = "/etc/ssl/openGemini-selfsigned.key"
```

以上配置会开启 HTTPS，并指定证书和密钥的路径。请根据您的实际情况修改证书和密钥的路径。

## 重启 openGemini

修改配置文件后，您需要重启`ts-sql`进程或者`ts-server`进程以使更改生效。

## 验证 HTTPS 是否已启用

您可以使用以下命令来验证 openGemini 是否已启用 HTTPS：

```bash
$ curl -i -k https://localhost:8086/ping
HTTP/1.1 200 Connection established

HTTP/1.1 204 No Content
Content-Type: application/json
Request-Id: 5073446b-e2b7-11ed-8002-72ef6a841b9c
X-Request-Id: 5073446b-e2b7-11ed-8002-72ef6a841b9c
Date: Mon, 24 Apr 2023 15:47:27 GMT
```

以上命令会发送一个 HTTPS 请求到 openGemini，并返回一个 `204` 响应。如果您得到了响应，则表示 HTTPS 已成功启用。请注意，由于我们使用的是自签名证书，因此您需要使用 `-k` 参数来跳过证书验证。

**也可通过使用CLI工具连接到openGemini来验证HTTPS是否正常工作**：

```bash
ts-cli -ssl -host <domain_name>.com
```


以上就是在 openGemini 上启用 HTTPS 的步骤。如果您遇到任何问题，请随时与我们联系。
