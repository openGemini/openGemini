# 贡献指南

欢迎来到openGemini！

## 1. 体验openGemini

- 从社区下载安装体验，见[用户指南-->Getting Started](http://opengemini.org/docs)

- [华为云沙箱实验]()（待上线）

## 2. 社区账号及DCO协议签署

### 2.1 申请注册GitHub社区账号

登录Github并使用常用邮箱进行注册，该邮箱需要用于签署DCO，以及用于配置SSH公钥。

### 2.2 签署DCO

签署openGemini [DCO]([Developer Certificate of Origin](https://developercertificate.org/))（Developer Certificate of Origin，开发者原创声明），并遵守原创契约。每次提交PR时，都需使用邮箱进行签署。

## 3. 参与openGemini社区

### 3.1 参与社区活动

您可以了解并参与丰富多彩的社区活动：

- [Meeting](http://opengemini.org/events)
- [Meetups](http://opengemini.org/events)
- [直播](https://space.bilibili.com/1560037308)
- [峰会](https://space.bilibili.com/1560037308)

### 3.2 找到您想参与的SIG

SIG就是Special Interest Group的缩写，openGemini社区按照不同的SIG来组织，以便于更好的管理和改善工作流程。因此参与社区事务正确的起始姿势是先找到您感兴趣的SIG， SIG组均是开放的，欢迎任何人来参与。

目前，openGemini社区刚成立，SIG正在筹备中。

如果您对openGemini的某个方向有浓厚的兴趣，希望在社区成立一个新的相关SIG进行维护和发展，那您可以参考 [申请新SIG流程指南]() 来申请创建新的SIG.

### 3.3 参与社区贡献

#### 提交Issue/处理issue任务

- 找到Issue列表：
  在您感兴趣的SIG项目仓内，点击“Issues”，您可以找到其Issue列表
- 提交Issue
  如果您准备向社区上报Bug或者提交需求，或者为openGemini社区贡献自己的意见或建议，请在openGemini社区对应的仓库上提交Issue。
  提交Issue请参考 [Issue提交指南]()。为了吸引更广泛的注意，您也可以把Issue的链接附在邮件内，通过[邮件列表]()发送给所有人。
- 参与Issue内的讨论
  每个Issue下面可能已经有参与者们的交流和讨论，如果您感兴趣，也可以在评论框中发表自己的意见。
- 找到愿意处理的Issue
  如果您愿意处理其中的一个issue，可以将它分配给自己。只需要在评论框内输入 /assign或 /assign @yourself，机器人就会将问题分配给您，您的名字将显示在负责人列表里。

#### 贡献编码

- 准备openGemini的开发环境
  如果您想参与编码贡献，需要准备Go和Python语言的开发环境。

- 了解不同SIG使用开发语言的编程规范，安全设计规范等。

- 下载代码和拉分支
  如果要参与代码贡献，您还需要了解如何在GitHub下载代码，通过PR合入代码等。openGemini使用GitHub代码托管平台，想了解具体的指导，请参考[GitHub Workflow Guide](https://docs.github.com/cn)。

- 修改、构建和本地验证
  在本地分支上完成修改后，进行构建和本地调试验证，参考[代码调试指南]()。

- 提交一个Pull-Request
  当你提交一个PR的时候，就意味您已经开始给社区贡献代码了。请参考 openGemini社区PR提交指导。
  为了使您的提交更容易被接受，您需要：

  - 遵循SIG组的编码约定，如果有的话
  - 准备完善的提交信息
  - 如果一次提交的代码量较大，建议将大型的内容分解成一系列逻辑上较小的内容，分别进行提交会更便于检视者理解您的想法
  - 使用适当的SIG组和监视者标签去标记PR：社区机器人会发送给您消息，以方便您更好的完成整个PR的过程

  注意：如果您的PR请求没有引起足够的关注，可以在SIG的邮件列表或community.ts@opengemini.org求助。

#### 贡献生态工具

如果你发现其他第三方软件系统、工具缺失了对openGemini的支持，或者openGemini缺失了对南向操作系统、CPU架构、存储系统的支持，可以帮openGemini把这个支持补上。与此同时，社区也会在ISSUE中发布生态工具的开发任务，您也可以主动接下任务。实际上贡献生态工具的过程就是帮助openGemini繁荣生态的过程，让openGemini成为一个具有广泛技术生态的开源时序数据库系统。

#### 贡献原创开源项目

如果您想将自己原创的基于openGemini开发的应用或解决方案贡献到openGemini社区，直接在https://github.com/openGemini中建立原创项目，将项目“托管”到openGemini社区。

#### 检视代码

openGemini是一个开放的社区，我们希望所有参与社区的人都能成为活跃的代码检视者。当成为SIG组的committer或maintainer角色时，便拥有审核代码的责任与权利。
强烈建议本着[行为准则]()，相互尊重和促进协作，希望能够促进新的贡献者积极参与，而不会使贡献者一开始就被细微的错误淹没，所以检视的时候，可以重点关注包括：
 1.贡献背后的想法是否合理
 2.贡献的架构是否正确
 3.贡献是否完善

#### 测试

为了成功发行一个社区版本，需要完成多种测试活动。不同的测试活动，测试代码的位置也有所不同，成功运行测试所需的环境的细节也会有差异，有关的信息可以参考：[社区开发者测试贡献指南]()。

#### 参与非代码类贡献

如果您的兴趣不在编写代码方面，可以在[ 非代码贡献指南 ]()中找到感兴趣的工作。

## 4. 和社区一起成长

### 4.1 社区角色说明

社区不同角色对应不同的责任与权利，每种角色都是社区不可或缺的一部分，您可以通过积极贡献不断积累经验和影响力，并获得角色上的成长。更详细角色说明与责任权利描述请查看 [角色说明]()。

### 4.2 技术委员会

openGemini技术委员会（Technical Committee，简称TC）是openGemini社区的技术决策机构，负责社区技术决策和技术资源的协调。



