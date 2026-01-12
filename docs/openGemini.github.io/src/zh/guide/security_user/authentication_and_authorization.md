---
title: 身份认证和授权
order: 2
---


# 身份认证和授权

## 认证方式
openGemini API和openGemini CLI包含身份验证功能，启用身份验证后，openGemini仅执行验证通过的HTTP请求。

1. 创建至少一个管理员用户，有关如何创建管理员用户，请参见[用户管理](./user_manage.md)。
2. 默认情况下，配置文件中禁用身份验证，通过在配置文件中将`auth-enabled`选项设为`true`来开启身份验证。
    ```toml
    [http]
    auth-enabled = true # ✨
    ```
3. 重新启动进程，openGemini将检查每个请求的用户信息，并将仅处理通过验证的用户请求。

## 使用API进行验证
如果同时使用基本身份验证和URL查询参数进行身份验证，则查询参数中指定的用户凭据优先，以下示例中查询假定该用户是admin用户，有关不同用户类型，其特权以及有关用户管理的更多信息，请参见[用户管理-GRANT]((./user_manage.md))的部分。
* Basic Authentication 验证

    ```bash
    curl -G http://localhost:8086/query -u user0:${YOUR_PWD} --data-urlencode "q=SHOW DATABASES"
    ```

* 在URL中使用查询参数
    ```bash
    curl -G "http://localhost:8086/query?u=user0&p=${YOUR_PWD}" --data-urlencode "q=SHOW DATABASES"
    ```

* 在URL中使用请求正文
    ```bash
    curl -G http://localhost:8086/query --data-urlencode "u=user0" --data-urlencode "p=${YOUR_PWD}" --data-urlencode "q=SHOW DATABASES"
    ```
## 使用CLI进行验证
* 在启动CLI时通过username和password进行身份验证
    ```bash
    ts-cli -username user0 -password ${YOUR_PWD}
    ```
* 启动CLI后使用auth命令进行验证
    ```sql
    >>> auth
    username: user0
    password:
    > show databases
    name: databases
    +---------------------+
    | name                |
    +---------------------+
    | NOAA_water_database |
    +---------------------+
    1 columns, 1 rows in set
    ```
