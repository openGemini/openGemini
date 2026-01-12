---
order: 1
---

# 安装 Golang

要从源代码构建 openGemini，您需要先在您的开发环境中安装 Go。 如果还没有安装Go，您可以按照本文档中的说明进行安装。

## 安装 Go 1.18

目前，openGemini 使用 Go 1.18+ 编译代码。 要安装 Go 1.18，请转到 [Go 的下载页面](https://golang.org/dl/)，选择版本 1.18，然后按照 [安装说明](https://golang.org/doc/install) .

## 使用 gvm 管理 Go 工具链

如果您使用的是 Linux 或 MacOS，您可以使用 [Go 版本管理器 (gvm)](https://github.com/moovweb/gvm) 轻松管理 Go 版本。

要安装 gvm，请运行以下命令：

```bash
curl -s -S -L https://raw.githubusercontent.com/moovweb/gvm/master/binscripts/gvm-installer | sh
```


安装 gvm 后，您可以使用它来管理多个不同版本的不同 Go 编译器。 让我们安装 Go 1.18 并将其设置为默认值：

```bash
gvm install go1.18
gvm use go1.18 --default
```


现在，您可以在 shell 中输入 go version 来验证安装：

```bash
go version
# OUTPUT:
# go 版本 go1.18 linux/amd64
```

在下一章中，您将学习如何获取 openGemini 源代码以及如何构建它。
