---
title: 端口矩阵
order: 2
---


# 端口矩阵

![communication](https://user-images.githubusercontent.com/49023462/200800239-63c53229-b7df-41eb-9504-ec1f4b558173.png)



<table>
  <tr>
    <th>组件</th>
    <th>端口</th>
    <th>说明</th>
  </tr>
  <tr>
    <td rowspan="2">ts-sql</td>
    <td>8086</td>
    <td>端口可变更，openGemini对外提供服务的统一入口</td>
  </tr>
  <tr>
    <td>6061</td>
    <td>不可变更，若被其他程序占用，则pprof功能不可用</td>
  </tr>
  <tr>
    <td rowspan="4">ts-meta</td>
    <td>8092</td>
    <td>端口可变更，ts-meta与ts-sql、ts-store之间正常业务交互使用的端口</td>
  </tr>
  <tr>
    <td>8091</td>
    <td>端口可变更，ts-meta的运维接口</td>
  </tr>
  <tr>
    <td>8088</td>
    <td>端口可变更，选举通信使用，三个ts-meta组成一个复制集，复制集之间通过raft协议进行选举</td>
  </tr>
  <tr>
    <td>8010</td>
    <td>端口可变更，ts-store（新）加入集群时使用</td>
  </tr>
  <tr>
    <td rowspan="4">ts-store</td>
    <td>8400</td>
    <td>端口可变更，ts-sql通过该端口将数据写入ts-store</td>
  </tr>
  <tr>
    <td>8401</td>
    <td>端口可变更，ts-sql通过该端口查询ts-store的数据</td>
  </tr>
  <tr>
    <td>8011</td>
    <td>端口可变更，ts-meta监测ts-store心跳使用</td>
  </tr>
  <tr>
    <td>6060</td>
    <td>不可变更，若被其他程序占用，则pprof功能不可用</td>
  </tr>
</table>
