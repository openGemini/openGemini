---
order: 2
---

# 贡献代码

openGemini 通过代码贡献来维护、改进和扩展。 我们欢迎向 openGemini 贡献代码。 openGemini 是基于[pull requests](https://docs.github.com/en/github/collaborating-with-pull-requests/producing-changes-to-your-work-with-pull-requests/about) 来运作的。

## 贡献之前

为 openGemini 做出贡献并*不*从创建pull request开始。 我们希望贡献者首先与我们联系，共同讨论总体方法。 如果没有与 openGemini 提交者达成共识，贡献可能需要大量返工或不会被审查。 因此，请[创建 GitHub 问题](report_issue.md)，在现有问题下进行讨论并到达共识。

对于新人，您可以查看[starter issues](https://github.com/openGemini/openGemini/issues?q=is%3Aissue+label%3A%22good+first+issue%22)，这些问题标有“good first issues”标签。 这些问题适合新贡献者处理，并且不需要很长时间就能解决。 但由于该标签通常是在分类时添加的，因此可能会变得不准确，因此，如果您认为该分类不再适用，请随时发表评论。

## 贡献过程

在issue达成共识后，就可以开始代码贡献过程了：

1. 通过 [/assign](http://prow.openGemini.org/command-help?repo=openGemini%2FopenGemini#assign) 将issue分配给自己。 这可以让其他贡献者知道您正在解决该issue，这样他们就不会重复工作。
2. 按照 [GitHub 工作流程](https://guides.github.com/introduction/flow/)，在您自己的 git 存储库分支中提交代码更改，并打开pull requst以进行代码审查。
3. 确保对拉取请求的持续集成检查为绿色（即成功）。
4. 审核并处理 [对你的拉取请求的评论](https://docs.github.com/en/github/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/commenting-on-a-pull-request)。
5. 当您的拉取请求获得足够的批准（默认数量为1）并且满足所有其他要求时，它将被合并。

清晰而友善的沟通是此过程的关键。

## 引用issue

openGemini 社区中的代码存储库需要**所有**拉取请求引用其相应的问题。在拉取请求正文中，**必须**有一行以`Issue Number:`开头，并通过[关键字](https://docs.github.com/zh/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue)链接相应的问题，例如：

如果拉取请求解决了相关问题，并且您希望 GitHub 在合并到默认分支后自动关闭这些问题，您可以使用如下语法 (`KEYWORD #ISSUE-NUMBER`)：

```
Issue Number: close #123
```

如果拉取请求链接了一个问题但没有关闭它，您可以使用关键字`ref`，如下所示：

```
Issue Number: ref #456
```

多个问题应该对每个问题使用完整的语法并用逗号分隔，例如：

```
Issue Number: close #123, ref #456
```

对于尝试关闭不同存储库中问题的拉取请求，贡献者需要首先在同一存储库中创建问题并使用此问题进行跟踪。

如果拉取请求正文未提供所需的内容，机器人将在拉取请求中添加`do-not-merge/needs-linked-issue`标签以防止其被合并。

## 编写测试

当您向 openGemini 贡献代码时，一件重要的事情就是测试。 测试应始终被视为变更的一部分。 任何导致 openGemini 语义改变或新功能添加的代码更改都应该有相应的测试用例。 当然，如果任何现有测试用例仍然有效，您就不能破坏它们。 建议首先在本地环境上[运行测试](../get_started/test_tutorials.md)，以发现明显的问题并在打开拉取请求之前修复它们。

如果您的拉取请求仅包含测试用例以增加 openGemini 的测试覆盖率，我们也会非常感激。 为现有模块补充测试用例是熟悉现有代码的一种很好且简单的方法。

## 创建良好的拉取请求

创建拉取请求以供提交时，您应该考虑以下几点以帮助确保您的拉取请求被接受：

* 该贡献是否会改变功能或组件的行为，从而可能破坏以前用户的程序和设置？ 如果是，则需要进行讨论并同意这种改变是可取的。
* 该贡献在概念上是否适合 openGemini？ 它是否属于特殊情况，导致常见情况变得更加复杂，或者使抽象/API 变得臃肿？
* 该贡献对 openGemini 的构建时间有很大影响吗？
* 您的贡献是否会影响任何文档？ 如果是，您应该添加/更改适当的文档。
* 如果有任何新的依赖项，它们是否正在积极维护？ 他们的许可证是什么？

## 编写良好的commits

每个功能或错误修复都应该通过单个拉取请求来解决，并且对于每个拉取请求可能有多个提交。 尤其：

* *不要*在同一次提交中修复多个问题（当然，除非一项代码更改修复了所有问题）。
* *不要*在与某些**功能/错误修复**相同的提交中对不相关的代码进行外观更改。

## 等待审核

首先，请耐心等待！ 提交拉取请求的人比能够审查您的拉取请求的人多得多。 审核您的拉取请求需要审核者有空闲时间和动力来查看您的拉取请求。 如果您的拉取请求在一段时间内没有收到审阅者的任何通知（即没有发表评论），您可以 ping reviewers 和 assignees 以获得更多关注。

当有人确实抽出时间查看您的拉取请求时，他们很可能会就如何改进它发表评论（不用担心，即使提交者/维护者也会将其拉取请求发送回给他们进行更改）。 然后，预计您会更新拉取请求以解决这些评论，并且审查过程将不断迭代，直到出现令人满意的解决方案。
