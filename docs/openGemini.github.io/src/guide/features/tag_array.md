---
title: TAG array
order: 8
---

This chapter mainly introduces the ```tag array``` function.

## BACKGROUND

If there are multiple services in a machine, in order to save bandwidth, the uploader will package and report the various component indicators of the same machine. In this case, the public fields will be merged and reported. If the database supports the ```TAG array``` function, it will greatly save bandwidth in this case.
:::tip

```ts-cli``` currently does not support ```tag array``` syntax and can be used using the ```Influx``` client.

:::

## FEATURES AND BENEFITS

```openGemini``` database supports array type tags, and when grouping and filtering, the database automatically recognizes array tags, disassembles them, and then processes them according to normal tags. ```openGemini``` supports selecting tag arrays for query insertion and is compatible with normal query insertion. Supporting array tags will reduce storage costs and write traffic.

## WRITE PROCESS

#### Enable tag array function

```sql
>create database db0 tag attribute array
```

By specifying support for ```tag array``` when creating a database, it can only be specified at the beginning. Modifying an existing database to support ```tag array``` is not currently supported. If the database is created in the following way, the ```tag array``` function is not supported by default.

```sql
>create database db0
```

#### Actual writing to the model

```sql
insert cpu,tk1=value1,tk2=value2,tk3=[value3,value33,value333] value=3 1670491328000000000

insert cpu,tk1=value1,tk2=[value2,value22,value222],tk3=[value3,value33,value333] value=2 1670491329000000000
```

#### Logical writing model

```sql
insert cpu,tk1=value1,tk2=value2,tk3=value3 value=3 1670491328000000000
insert cpu,tk1=value1,tk2=value2,tk3=value33 value=3 1670491328000000000
insert cpu,tk1=value1,tk2=value2,tk3=value333 value=3 1670491328000000000
insert cpu,tk1=value1,tk2=value2,tk3=value3 value=2 1670491329000000000
insert cpu,tk1=value1,tk2=value22,tk3=value33 value=2 1670491329000000000
insert cpu,tk1=value1,tk2=value222,tk3=value333 value=2 1670491329000000000
```

It can be found that if there are several members in the array, they will be converted into several statements. The lengths of different arrays are the same, and the data at the same position in different arrays form a tuple for writing.

## WRITE REQUEST

1. ```tagvalue``` may not be an array
2. The length of all ```tagvalue``` arrays must be consistent
3. ```serieskey``` is arranged according to the array subscript


## WRITING EXAMPLE

```sql
insert cpu,tk1=value1,tk2=value2,tk3=[value3,value33,value333] value=3
insert cpu,tk1=value1,tk2=[value2,value22,value222],tk3=[value3,value33,value333] value=2
```
