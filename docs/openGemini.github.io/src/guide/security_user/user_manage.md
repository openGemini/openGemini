---
title: User management
order: 3
---

This chapter mainly introduces how to create users, delete users, authorize, etc. in openGemini.

openemini does not have a built-in account. you need to actively create a unique administrator account for the system. The accounts of openGemini are divided into administrator accounts and ordinary user accounts, and the corresponding roles are admin and user respectively. Administrator can only be created once in the system, cannot be deleted, and cannot be renamed. Administrator can create more ordinary user accounts, and must be authorized to access the database.

The permissions of ordinary users are associated with the DB. The permissions are divided into three types: READ, WRITE, and ALL, which represent readable, writable, and readable and writable respectively. If an ordinary user is granted the READ permission on the DB, the ordinary user can only query in the DB.

:::tip
Create an administrator account first, then enable https and identity authentication
:::

## Create an administrator account
Start openGemini stand-alone or cluster without enable https and auth.
```sql
> curl -i -XPOST "http://ip:8086/query" --data-urlencode "q=CREATE USER admin WITH PASSWORD 'admin-passwd' WITH ALL PRIVILEGES"
```
| creates an administrator account named "admin"

:::tip
1. You need to replace the IP address and port in the command with the ip and port of ts-sql, and set the administrator account name and password.

2. The password must be composed of uppercase and lowercase letters, numbers, and special characters, and the length is limited to 8-256 characters.

3. config/weakpasswd.properties is a weak password configuration file, which supports weak password verification by default. If the password is exist in the configuration file, it will be regarded as a weak password and is not allowed to be used.

4. The password must be in single quotes.
5. It is **not recommended** to use single quote (') and backslash (\) characters in passwords, for passwords containing these characters, use '\' to escape special characters when creating users and submitting authentication requests.
:::

You can also use ts-cli and create administrator.
```shell
> ts-cli -host xxx -port xxx
openGemini CLI 0.1.0 (rev-revision)
Please use `quit`, `exit` or `Ctrl-D` to exit this program.
> CREATE USER admin WITH PASSWORD 'nJa@w7f@12' WITH ALL PRIVILEGES
> SHOW USERS
+-------+-------+
| user  | admin |
+-------+-------+
| admin | true  |
+-------+-------+
2 columns, 1 rows in set
```
::: danger
For security reasons, the administrator account of openGemini can only be created once in the system, cannot be deleted, and cannot be renamed. Before creating an administrator account, please carefully save the user name and password.
:::

## Enable authentication and HTTPS

Please refer to [Authentication and Authorization](./authentication_and_authorization.md) and [Enable HTTPS](./https.md)

1. If you use an openGemini cluster, modify the configuration file ' openGemini.conf '
```toml
[http]
 ...
 auth-enabled = true
 https-enabled = true
 https-certificate = "path/to/certificate.crt"
 https-private-key = "path/to/certificate.key"
```
2. If you use an single openGemini, modify the configuration file ' openGemini.singlenode.conf ', <font color=red>**add**</font> the following content under [http] section.
```toml
[http]
 auth-enabled = true
 https-enabled = true
 https-certificate = "path/to/certificate.crt"
 https-private-key = "path/to/certificate.key"
```
If you combine the crt file and key file into a pem file, you can only configure the https-certificate option.

**relate entries** [generate a self-signed certificate and key](./https.md#generate-certificate-and-key)

3. **<font color=red>Restart openGemini</font>**
```shell
~$ ts-cli -host xx -port xx -ssl -unsafeSsl
openGemini CLI 0.1.0 (rev-revision)
Please use `quit`, `exit` or `Ctrl-D` to exit this program.
> show databases
ERR: unable to parse authentication credentials
```
The error message "ERR: unable to parse authentication credentials" indicates that https and identity authentication have taken effect

## Connect to openGemini

```sql
~$ ts-cli -host xx -port xx -ssl -unsafeSsl
openGemini CLI 0.1.0 (rev-revision)
Please use `quit`, `exit` or `Ctrl-D` to exit this program.
> auth
username: admin
password:
> show databases
name: databases
+------+
| name |
+------+

1 columns, 0 rows in set
```
If the above command is executed correctly, it means that the user has taken effect, and then you can use the administrator to create a ordinary user or execute other commands.

## Create ordinary user
use the administrator to create ordinary users

**Create a ordinary user 'user0' without any permissions**
```sql
> auth
username: admin
password:
> CREATE USER user0 WITH PASSWORD 'your_pwd'
```

## SHOW USERS
View all existing users

```sql
> SHOW USERS
+-------+-------+
| user  | admin |
+-------+-------+
| admin | true  |
| user0 | false |
+-------+-------+
2 columns, 2 rows in set
```

## DROP USER
delete exist users

```sql
DROP USER <username>
```
### Examples

```sql
DROP USER "user0"
```

## GRANT
Grant user `READ`, `WRITE` or `ALL` permission on DB

```sql
GRANT [READ,WRITE,ALL] ON <database_name> TO <username>
```

### Examples

Grant `user0` read access to `NOAA_water_database`:

```sql
GRANT READ ON "NOAA_water_database" TO "user0"
```

Grant `user0` to have all permissions on `NOAA_water_database`:

```sql
GRANT ALL ON "NOAA_water_database" TO "user0"
```

## SHOW GRANTS
View specified user permissions

```sql
> SHOW GRANTS FOR <username>
```

### Examples

```sql
> SHOW GRANTS FOR "user0"
+----------------------+-----------+
| database             | privilege |
+----------------------+-----------+
| NOAA_water_database  | READ      |
+----------------------+-----------+
2 columns, 1 rows in set
```

## REVOKE
Revoke user permissions

```sql
> REVOKE [READ,WRITE,ALL] ON <database_name> FROM <username>
```

### Examples

Revoke the write permission of `user0` to `NOAA_water_database`:

```sql
> REVOKE WRITE ON "NOAA_water_database" FROM "user0"
```

Revoke the all permission of `user0` to `NOAA_water_database`:

```sql
> REVOKE ALL ON "NOAA_water_database" FROM "user0"
```

## SET PASSWORD
reset Password

```sql
> SET PASSWORD FOR <username> = '<password>'
```

### Examples

```sql
> SET PASSWORD FOR "user0" = 'your_pwd'
```
