---
title: 如何参与文档贡献
order: 4
---

## 文档仓库
openGemini的文档仓库是
```
https://github.com/openGemini/openGemini.github.io
```

## 文档目录介绍
### 中文文档路径
```
openGemini.github.io/src/zh/guide
```
### 目录解析
|目录名称|说明|
|:----|:----|
|introduction|介绍|
|quick_start|快速上手|
|write_data|写数据|
|query_data|查询数据|
|schema|元数据|
|develop|应用开发|
|features|关键特性|
|functions|系统函数|
|security_user|用户与安全|
|data_migrate|数据迁移|
|maintenance|数据库运维|
|kernel|技术内幕|
|contribution|参与社区|
|reference|参考指南|
|versions|版本发布历史|
|troubleshoot|常见问题|
### 英文文档路径
```
openGemini.github.io/src/guide
```
目录说明与中文文档一致

### 图片保存目录
```
openGemini.github.io/static
```
## 参与贡献
### 下载源码
```shell
> git clone https://github.com/openGemini/openGemini.github.io.git
```
### 编译(非必须，编译的目的是要在本地查看文档效果）
参考仓库[README](https://github.com/openGemini/openGemini.github.io)
简单来讲分为三步
- 安装[node.js](https://nodejs.org/en),推荐最新LTS版本
- 安装pnpm，版本推荐 v8.6.11
- 安装依赖 ```pnpm install --frozen-lockfile```或```pnpm install --nofrozen-lockfile```
- 本地运行```pnpm docs:dev```，浏览器访问```http://localhost:8080```

运行成功示例
```
~$ pnpm docs:dev

> openGemini docs@1.0.0 docs:dev /home/opengemini-1/Documents/openGemini.github.io
> vuepress dev src

VuePress version mismatch: @vuepress/plugin-register-components is using 2.0.0-beta.66 while the main VuePress is using 2.0.0-beta.61
✔ Initializing and preparing data - done in 1.15s

  vite v4.1.4 dev server running at:

  ➜  Local:   http://localhost:8080/
  ➜  Network: http://192.168.0.14:8080/

```
### 文档开发
以开发openGemini函数功能的中文文档为例

1. 进入```functions```目录
2. 了解文件格式，打开任何一个MarkDown文件，例如：aggregate.md，格式如下：
```
---
title: 聚合函数
order: 1
---

内容略...
```
**title**: 表示在文档标题，会在官网文档左侧导航栏上显示

**order**: 文档的顺序，为1表示排最前面，2表示排第二位，以此类推

**其余部分**: 文档内容, 按markdown格式编写

[markdown语法参考](https://markdown.com.cn/basic-syntax/)

3. 增加或在修改文档内容，保存

4. 浏览器查看实际效果

### 文档提交
Git提交命令
```
> git add *
> git commit -s -m "chore: update aggregate.md"
> git push HEAD:newbranch
```
提交PR略

### 文档合并
提交PR后，后台自动会进行格式验证和项目构建，成功后便可合入，这使在PR下方留言，@committer，请求检视和合入

Committer

|xiangyu5632|shilinlee|
|-----------|---------|

:::tip
英文文档亦是如此，可参考上述步骤
:::
