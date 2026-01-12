---
title: Authentication
order: 2
---

## authentication
openGemini supports security verification of user name and password

1. Create at least one admin user, see [User Management](./user_manage.md) for how to create an admin user
2. By default, authentication is disabled in the configuration file, enable authentication by setting the `auth-enabled` option to `true` in the configuration file.
    ```toml
    [http]
    auth-enabled = true
    ```
3. After restarting the process, openGemini will check the user information of each request and will only process authenticated user requests.

## Authenticate using the API
* Basic Authentication

    ```bash
    curl -G http://localhost:8086/query -u user0:${YOUR_PWD} --data-urlencode "q=SHOW DATABASES"
    ```

* Using username and password as parameters in the URL
    ```bash
    curl -G "http://localhost:8086/query?u=user0&p=${YOUR_PWD}" --data-urlencode "q=SHOW DATABASES"
    ```

* put username and password in request body
    ```bash
    curl -G http://localhost:8086/query --data-urlencode "u=user0" --data-urlencode "p=${YOUR_PWD}" --data-urlencode "q=SHOW DATABASES"
    ```
**relate entries** [User Management](./user_manage.md#grant)

## Authenticate using the CLI
* Authenticate by username and password when starting the CLI
    ```bash
    ts-cli -username user0 -password ${YOUR_PWD}
    ```
* Use the auth command to authenticate after starting the CLI
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
