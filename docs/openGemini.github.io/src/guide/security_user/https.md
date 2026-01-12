---
title: Use HTTPS
order: 3
---

The following are the steps to enable HTTPS in openGemini:

## Generate certificate and key

First, you need to generate an SSL certificate and key. You can generate it using openssl command

```bash
openssl req -x509 -nodes -newkey rsa:2048 -keyout /etc/ssl/openGemini-selfsigned.key -out /etc/ssl/openGemini-selfsigned.crt -days 365 -subj "/C=US/ST=CA/L=San Francisco/O=openGemini/OU=openGemini/CN=localhost"
```

The above command generates a self-signed certificate and key with a specified validity period of 365 days. Please modify the parameters according to your needs.

## Modify the openGemini configuration file

Next, you need to modify openGemini's configuration file. In the configuration file, find the `[http]` section and add the following:

```toml
[http]
...
https-enabled = true
https-certificate = "/etc/ssl/openGemini-selfsigned.crt"
https-private-key = "/etc/ssl/openGemini-selfsigned.key"
```

The path of the certificate and key can be modified.

## Restart openGemini

After modifying the configuration file, you need to restart the `ts-sql` process or `ts-server` process for the changes to take effect.

## Check that HTTPS is working

You can use the following command to check whether https is in effect

```bash
$ curl -i -k https://localhost:8086/ping
HTTP/1.1 200 Connection established

HTTP/1.1 204 No Content
Content-Type: application/json
Request-Id: 5073446b-e2b7-11ed-8002-72ef6a841b9c
X-Request-Id: 5073446b-e2b7-11ed-8002-72ef6a841b9c
Date: Mon, 24 Apr 2023 15:47:27 GMT
```

The above command will send an HTTPS request to openGemini and return a response with status 204. If you get a right response, HTTPS has been successfully enabled. Note that since we are using a self-signed certificate, you need to use the `-k` parameter to skip certificate verification.

**Using the CLI to check if HTTPS is working**：

```bash
ts-cli -ssl -host 127.0.0.1 -port 8086
```
You can use ```-unsafeSsl``` to skip certificate verification.
