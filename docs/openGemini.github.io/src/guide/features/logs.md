---
title: Log search
order: 3
---
:::tip
relate entries [create measurement](../schema/measurement.md#text-search)
:::
## MATCH
An exact match means that only the entire word matches the keyword. For example, in log retrieval, all relevant log information is retrieved according to a specific error code.
```sql
> SELECT server_name，message FROM mst WHERE MATCH(message, "504") AND server_name="server0" AND time > now() - 1d
```
Query all logs of "server0" service which contained error code '504' in the past day

## MATCHPHRASE
Like MATCH query, phrase matching is one of the most commonly used query methods in standard full-text search. Both MATCH and MATCHPHRASE are exact matches. The difference is that MATCHPHRASE is a query by phrase, while MATCH is a single word.
```sql
> SELECT COUNT(message) FROM mst WHERE MATCHPHRASE(message, 'GET images backnews.gif') AND time > now() - 1h
```
Query the total number of logs containing the content of 'GET images backnews.gif' in the past 1 hour

## LIKE
Fuzzy matching is another common full-text search query method, which can return a type of string with a certain prefix or suffix. For example, in service logs, some errors have fixed prefixes, but the error codes are different. , but all indicate a certain type of error message, then you will think of using fuzzy matching
```sql
> SELECT * FROM mst WHERE message LIKE 'NET%'
```
Query all log data containing keywords starting with 'NET'
