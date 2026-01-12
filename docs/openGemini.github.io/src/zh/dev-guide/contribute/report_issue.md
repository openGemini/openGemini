---
order: 1
---

# 提交Issue

在报告Issue之前，请[搜索现有Issue](https://github.com/openGemini/openGemini/issues)以检查该问题是否已被报告，甚至是否已修复。 如果您选择报告问题，请在报告中包含以下内容：

- 您的操作系统（或发行版）的完整详细信息 - 例如`64 位 Ubuntu 18.04`。 要获取操作系统详细信息，请在终端中运行以下命令并将输出复制粘贴到报告中：

   ```bash
   uname -srm
   ```

- 你如何安装 openGemini。 您使用了预构建的包还是从源代码构建的？

- 您正在运行的 openGemini 版本。 如果您使用预构建的软件包安装了 openGemini，请在终端中运行以下命令，然后将输出复制粘贴到报告中：

   ```bash
   ts-server version # or ts-sql version / ts-store version / ts-meta version e.t.
   ```

有很多Issue模板。

[新问题](https://github.com/openGemini/openGemini/issues/new/choose)

回答这些问题会提供有关您的问题的详细信息，以便其他贡献者或 openGemini 用户可以更轻松地解决您的问题。

## 提出好的Issue (TODO)

除了一个好的标题和详细的问题消息之外，您还可以通过[/label]()为您的问题添加合适的标签，特别是该问题属于哪个组件以及该问题影响哪些版本。 许多提交者和贡献者只关注 openGemini 的某些子系统。 设置适当的组件对于引起他们的注意很重要。

深入的思考可以帮助问题更快地得到解决，并有助于在社区中建立自己的声誉。

## 了解Issue的进展和状态

一旦你的问题被创建，其他贡献者可能会参与进来。你需要与他们讨论，提供他们可能想知道的更多信息，回应他们的意见以达成共识并推动进展。 但请注意，悬而未决的问题总是超出贡献者能够处理的范围，尤其是 openGemini 社区是一个全球性的社区，贡献者居住在世界各地，他们可能已经忙于自己的工作和生活。 请耐心等待！ 如果您的问题在一段时间内变得过时，可以向其他参与者发出更多关注。

## 报告安全漏洞

openGemini 非常重视安全性和用户的信任。 如果您认为您在我们的任何开源项目中发现了安全问题，请负责任地联系 [security@openGemini.org](mailto:security@openGemini.org) 进行披露。 有关安全漏洞报告的更多详细信息，[可以在此处找到](https://github.com/openGemini/openGemini/security/policy)。
