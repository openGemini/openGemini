---
title: 数学表达式
order: 4
---

数学运算符遵循[标准的运算顺序](https://golang.org/ref/spec#Operator_precedence)。也就是说，括号优先于除法和乘法，除法和乘法优先于加法和减法，例如，`5 / 2 + 3 * 2 =  (5 / 2) + (3 * 2)` and `5 + 2 * 3 - 2 = 5 + (2 * 3) - 2`。

## 数学运算符

### 加法

与常数相加

```sql
SELECT "A" + 5 FROM "add"
```
```sql
SELECT * FROM "add" WHERE "A" + 5 > 10
```

两个field相加

```sql
SELECT "A" + "B" FROM "add"
```
```sql
SELECT * FROM "add" WHERE "A" + "B" >= 10
```

### 减法

与常数相减

```sql
SELECT 1 - "A" FROM "sub"
```
```sql
SELECT * FROM "sub" WHERE 1 - "A" <= 3
```

两个field相减

```sql
SELECT "A" - "B" FROM "sub"
```
```sql
SELECT * FROM "sub" WHERE "A" - "B" <= 1
```

### 乘法

与常数相乘

```sql
SELECT 10 * "A" FROM "mult"
```
```sql
SELECT * FROM "mult" WHERE "A" * 10 >= 20
```

两个field相乘

```sql
SELECT "A" * "B" * "C" FROM "mult"
```
```sql
SELECT * FROM "mult" WHERE "A" * "B" <= 80
```

乘法分布在其它运算符上

```sql
SELECT 10 * ("A" + "B" + "C") FROM "mult"
```

```sql
SELECT 10 * ("A" - "B" - "C") FROM "mult"
```

```sql
SELECT 10 * ("A" + "B" - "C") FROM "mult"
```

### 除法

与常数相除

```sql
SELECT 10 / "A" FROM "div"
```
```sql
SELECT * FROM "div" WHERE "A" / 10 <= 2
```

两个field相除

```sql
SELECT "A" / "B" FROM "div"
```
```sql
SELECT * FROM "div" WHERE "A" / "B" >= 10
```

除法分布在其它运算符上

```sql
SELECT 10 / ("A" + "B" + "C") FROM "mult"
```

### 模

与常数进行模运算

```sql
SELECT "B" % 2 FROM "modulo"
```
```sql
SELECT "B" FROM "modulo" WHERE "B" % 2 = 0
```

对两个field进行模运算

```sql
SELECT "A" % "B" FROM "modulo"
```
```sql
SELECT "A" FROM "modulo" WHERE "A" % "B" = 0
```

### 按位AND

您可以将这个运算符与任何整数或布尔值一起使用，无论它们是field还是常数。这个运算符不适用于浮点数或字符串，并且您也不能将整数和布尔值混合在一起计算。

```sql
SELECT "A" & 255 FROM "bitfields"
```

```sql
SELECT "A" & "B" FROM "bitfields"
```

```sql
SELECT * FROM "data" WHERE "bitfield" & 15 > 0
```

```sql
SELECT "A" & "B" FROM "booleans"
```

```sql
SELECT ("A" ^ true) & "B" FROM "booleans"
```


### 按位OR

您可以将这个运算符与任何整数或布尔值一起使用，无论它们是field还是常数。这个运算符不适用于浮点数或字符串，并且您也不能将整数和布尔值混合在一起计算。

```sql
SELECT "A" | 5 FROM "bitfields"
```

```sql
SELECT "A" | "B" FROM "bitfields"
```

```sql
SELECT * FROM "data" WHERE "bitfield" | 12 = 12
```

### 按位异或

您可以将这个运算符与任何整数或布尔值一起使用，无论它们是field还是常数。这个运算符不适用于浮点数或字符串，并且您也不能将整数和布尔值混合在一起计算。

```sql
SELECT "A" ^ 255 FROM "bitfields"
```

```sql
SELECT "A" ^ "B" FROM "bitfields"
```

```sql
SELECT * FROM "data" WHERE "bitfield" ^ 6 > 0
```

### 数学运算符的常见问题

**问题一：数学运算符与通配符或正则表达式同时使用**
openGemini不支持在SELECT子句中将数学运算与通配符(`*`)或正则表达式结合使用。以下查询是无效的，系统会返回错误：

对通配符执行数学运算
```sql
> SELECT * + 2 FROM "nope"
ERR: error parsing query: syntax error: unexpected ADD, expecting FROM
```

对函数中的通配符执行数学运算
```sql
> SELECT COUNT(*) / 2 FROM "nope"
ERR: unsupported expression with wildcard: count()
```

对正则表达式执行数学运算
```sql
> SELECT /A/ + 2 FROM "nope"
ERR: cannot perform a binary expression on two literals
```

对函数中的正则表达式执行数学运算
```sql
> SELECT COUNT(/A/) + 2 FROM "nope"
ERR: unsupported expression with regex field: count()
```

**问题二：数学运算符与函数同时使用**

目前不支持在函数内使用数学运算。请注意，openGemini只允许在`SELECT`子句中使用函数。

例如

```sql
SELECT 10 * mean("value") FROM "cpu"
```
语句没有问题，但会产生一个解析错误。
```sql
> SELECT mean(10 * "value") FROM "cpu"
ERR: expected field argument in mean()
```

## 不支持的运算符

### 逻辑运算符

使用`!|`、`NAND`、`XOR`或`NOR`会产生解析错误。

另外，在查询的`SELECT`子句中使用`AND`或者`OR`并不会跟使用数学运算符的效果一样，只会产生空的结果，因为`AND`和`OR`都是GeminiQL中的标记(tokens)。但是，您可以对布尔类型的数据使用按位操作符：`&`、`|`和`^`

### Bitwise Not

没有bitwise not运算符，因为您期望的结果依赖您的bitfield的宽度。GeminiQL不知道您的bitfield的宽度，所以无法实现一个合适的bitwise not运算符。

例如，如果您的bitfield的宽是8比特(bit)，那么整数`1`表示比特`0000 0001`，bitwise-not操作后应该返回比特`1111 1110`，也就是整数254。

但是，如果您的bitfield的宽是16比特(bit)，那么整数`1`表示比特`0000 0000 0000 0001`，bitwise-not操作后应该返回比特`1111 1111 1111 1110`，也就是整数65534。

**解决方法**

您可以通过使用`^`(bitwise xor)运算符和全部比特位都为`1`的数字（比特`1`的个数等于您的bitfield的宽度）来实现bitwise not运算：

对于8比特的数据(8-bit data)：

```sql
SELECT "A" ^ 255 FROM "data"
```

对于16比特的数据(16-bit data)：

```sql
SELECT "A" ^ 65535 FROM "data"
```

对于32比特的数据(32-bit data)：

```sql
SELECT "A" ^ 4294967295 FROM "data"
```

在每种情况下，您需要的常数可以这样计算：`(2 ** width) - 1`，即2的`width`次方减去1。
