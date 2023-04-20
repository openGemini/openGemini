# 贡献指南

欢迎来到openGemini！

## 1. 提交Issue/处理issue任务

- 提交Issue
  如果您准备向社区上报Bug或者提交需求，或者为openGemini社区贡献自己的意见或建议，请在openGemini社区对应的仓库上提交Issue。

- 参与Issue内的讨论
  每个Issue下面可能已经有参与者们的交流和讨论，如果您感兴趣，也可以在评论框中发表自己的意见。

- 找到愿意处理的Issue
  如果您愿意处理其中的一个issue，可以将它分配给自己。只需要在评论框内输入 /assign或 /assign @yourself，机器人就会将问题分配给您，您的名字将显示在负责人列表里。

  > **@yourself** 替换为您的 GitHub 用户名，比如 /assign @bob

## 2. 贡献源码

### 2.1 提交拉取请求详细步骤

1. 在提交拉取请求之前，请先在 [Github](https://github.com/openGemini/openGemini/pulls) 中搜索关闭或开启的相关PR，以避免重复工作。

2. 确保问题描述了您正在修复的问题，或记录了您要添加的功能的设计。提前讨论设计有助于确保我们准备好接受您的工作。

3. 签署openGemini [DCO](https://developercertificate.org)（Developer Certificate of Origin，开发者原创声明），并遵守原创契约。每次提交PR时，都需使用邮箱进行签署 ，因为我们不能接受没有签名DCO的代码。

   > git commit 命令中增加-s参数，即可自动签名。比如：git commit -s -m "fix: xxxxxxxx"

4. [Fork](https://github.com/openGemini/openGemini/fork) openGemini/openGemini 仓库

   您需要了解如何在GitHub下载代码，通过PR合入代码等。openGemini使用GitHub代码托管平台，想了解具体的指导，请参考[GitHub Workflow Guide](https://docs.github.com/cn)。

5. Clone您的仓库，在您的仓库中，在新的git分支中更改：

   `git checkout -b my-fix-branch main`

6. 添加你的**代码**和**测试用例**

7. 使用git工具完成您的commit。

    ```
    // 添加文件到暂存区
    git add .
    
    // 提交暂存区到本地仓库，增加-s参数，自动签名
    git commit -s -m "<your commit message>"
    ```
   其中，`<your commit message>`是您的提交信息，需要遵循以下命名规范：
   - feat: feature的缩写, 新的功能或特性
   - fix: bug的修复
   - docs: 文档修改
   - style: 格式修改. 比如改变缩进, 空格, 删除多余的空行, 补上漏掉的分号. 总之, 就是不影响代码含义和功能的修改
   - refactor: 代码重构. 一些不算修复bug也没有加入新功能的代码修改
   - perf: performance的缩写, 提升代码性能
   - test: 测试文件的修改
   - chore: 其他的小改动. 一般为仅仅一两行的改动, 或者连续几次提交的小改动属于这种
   
   更多详细信息，您可以参考[约定式提交](https://www.conventionalcommits.org/zh-hans/v1.0.0/)。

8. 将您的分支推送到Github

   `git push origin my-fix-branch`

9. 打开PR开始合并请求
   当你提交一个PR的时候，就意味您已经开始给社区贡献代码了。请参考 openGemini社区PR提交指导。
   为了使您的提交更容易被接受，您需要：

   - 填写完善的提交信息，并且签署DCO。
   - 如果一次提交的代码量较大，建议将大型的内容分解成一系列逻辑上较小的内容，分别进行提交会更便于检视者理解您的想法

   注意：如果您的PR请求没有引起足够的关注，可以发送邮件到community.ts@opengemini.org求助。

### 2.2 编译源码

#### 2.2.1 支持平台

我们支持以下平台:

- Linux x86/arm
- Darwin x86/arm

#### 2.2.2 编译环境信息

[GO](https://golang.org/dl/) version v1.16+

[Python](https://www.python.org/downloads/) version v3.7+

[Git](https://git-scm.com/downloads)

#### 2.2.3 GO环境变量设置

打开 `~/.profile`配置文件，在文件末尾添加如下配置：

```bash
# 设置GOPATH(可自定义目录)
export GOPATH=$HOME/gocodez
export GOPROXY=https://goproxy.cn,direct
export GO111MODULE=on
export GONOSUMDB=*
export GOSUMDB=off
```

#### 2.2.4 下载源码编译

```bash
cd $GOPATH
mkdir -p {pkg,bin,src}
cd src
git clone git@github.com:<username>/openGemini.git
cd openGemini
python3 build.py --clean
```

编译成功后，二进制保存在`build`目录中。

### 2.3 启动服务

#### 2.3.1 启动单机版

- 默认参数启动

  ```bash
  ./build/ts-server
  ```

- 带配置文件启动

  ```
  ./build/ts-server run -config=config/openGemini.singlenode.conf  
  ```

- 使用脚本启动

  ```bash
  bash scripts/install.sh
  ```

#### 2.3.2 启动伪集群版

```bash
bash scripts/install_cluster.sh
```

> maxOS用户可能第一次运行需要输入admin的密码，参考 https://superuser.com/questions/458875/how-do-you-get-loopback-addresses-other-than-127-0-0-1-to-work-on-os-x

> 临时放开 127.0.0.2和127.0.0.3

### 2.4 CI和静态分析工具

#### 2.4.1 CI

所有的pull requests都会运行CI。 社区贡献者应该查看PR checks的结果，来检查是否符合代码合入的最低门槛。 如果有任何问题请解决，以确保团队成员及时进行审核。

openGemini 项目在内部也有很多检查流程。 这可能需要一些时间，并且对社区贡献者来说并不真正可见。我们会定期将问题以及修复代码同步到社区。

#### 2.4.2 Static Analysis

该项目使用以下静态分析工具。 运行这些工具中的任何一个失败都会导致构建失败。 通常，必须调整代码以满足这些工具的要求，但也有例外。

- [go vet](https://golang.org/cmd/vet/) checks for Go code that should be considered incorrect.

  通过在本项目根目录执行以下命令，即可：

  ```bash
  make go-vet-check
  ```

- [goimports-reviser](https://github.com/incu6us/goimports-reviser) checks that Go code is correctly formatted.

  通过在本项目根目录执行以下命令，即可：

  > 注意：本地go版本在1.19及以上，请不要尝试执行，会有很多误报，社区正在修复中。

  ```
  make style-check
  ```

- [go mod tidy](https://tip.golang.org/cmd/go/#hdr-Add_missing_and_remove_unused_modules) ensures that the source code and go.mod agree.

- [staticcheck](https://staticcheck.io/docs/) checks for things like: unused code, code that can be simplified, code that is incorrect and code that will have performance issues.

  通过在本项目根目录执行以下命令，即可：

  > 注意：由于static-check本身的问题，所以本地go版本在1.19及以上，才能执行。

  ```bash
  make static-check
  ```

## 3. 参与社区其他贡献

### 3.1 贡献生态工具

如果你发现其他第三方软件系统、工具缺失了对openGemini的支持，或者openGemini缺失了对南向操作系统、CPU架构、存储系统的支持，可以帮openGemini把这个支持补上。与此同时，社区也会在ISSUE中发布生态工具的开发任务，您也可以主动接下任务。实际上贡献生态工具的过程就是帮助openGemini繁荣生态的过程，让openGemini成为一个具有广泛技术生态的开源时序数据库系统。

### 3.2 贡献原创开源项目

如果您想将自己原创的基于openGemini开发的应用或解决方案贡献到openGemini社区，直接在https://github.com/openGemini中建立原创项目，将项目“托管”到openGemini社区。

### 3.3 检视代码

openGemini是一个开放的社区，我们希望所有参与社区的人都能成为活跃的代码检视者。当成为SIG组的committer或maintainer角色时，便拥有审核代码的责任与权利。
强烈建议本着[行为准则]()，相互尊重和促进协作，希望能够促进新的贡献者积极参与，而不会使贡献者一开始就被细微的错误淹没，所以检视的时候，可以重点关注包括：
 1.贡献背后的想法是否合理
 2.贡献的架构是否正确
 3.贡献是否完善

### 3.4 测试

为了成功发行一个社区版本，需要完成多种测试活动。不同的测试活动，测试代码的位置也有所不同，成功运行测试所需的环境的细节也会有差异，有关的信息可以参考：[社区开发者测试贡献指南]()。

### 3.5 参与非代码类贡献

如果您的兴趣不在编写代码方面，可以在[ 非代码贡献指南 ]()中找到感兴趣的工作。

## 4. 和社区一起成长

### 4.1 社区角色说明

社区不同角色对应不同的责任与权利，每种角色都是社区不可或缺的一部分，您可以通过积极贡献不断积累经验和影响力，并获得角色上的成长。更详细角色说明与责任权利描述请查看 [角色说明]()。

### 4.2 技术委员会

openGemini技术委员会（Technical Committee，简称TC）是openGemini社区的技术决策机构，负责社区技术决策和技术资源的协调。



