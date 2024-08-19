# Welcome to openGemini

| Update Time   | Authors                                        |
| ------------- | ---------------------------------------------- |
| October 2022  | @[xiangyu5632](https://github.com/xiangyu5632) |
| April 2023    | @[shilinlee](https://github.com/shilinlee)     |
| April 2023    | @[xmh1011](https://github.com/xmh1011)         |
| May 2023      | @[1156230954](https://github.com/1156230954)   |
| December 2023 | @[xiangyu5632](https://github.com/xiangyu5632) |

## Directory structure

```
openGemini
├── app
├── benchmarks
├── build
├── config
├── coordinator
├── docker
├── docs
├── engine
├── images
├── lib
├── python
├── scripts
├── services
└── tests
```

| 目录        | 说明                                                         |
| ----------- | ------------------------------------------------------------ |
| app         | Including startup and communication module codes for ts-meta, ts-sql, ts-store, ts-monitor, ts-server, ts-cli and other components |
| build       | After the openGemin source code is compiled, the binary file is stored in the directory |
| config      | OpenGemini's configuration file storage directory            |
| coordinator | The coordination layer of the distributed system is mainly responsible for distributing read and write requests to different ts-store nodes, and also includes metadata interaction with ts-meta when DDL commands are executed. |
| docker      | Store files related to Docker deployment, such as Dockerfile, startup scripts, etc. |
| engine      | Storage engine implementation                                |
| lib         | Implementation of various common tools and support functions |
| python      | Implementation of AI-based time series data analysis platform, supporting time series data anomaly detection |
| scripts     | Contains openGemini’s automatic deployment scripts, unit test scripts, etc. |
| services    | openGemini's background services, such as Continue Query, Multi-level Downsample, etc. |
| tests       | Contains all functional test cases of openGemini             |

## Code of Conduct

Please make sure to read and observe our [Code of Conduct](./CODE_OF_CONDUCT.md).

## Submit Issue/Process issue task

- **Submit an Issue**
  If you are ready to report bugs or submit requirements to the community, or contribute your opinions or suggestions to the openGemini community, please submit an Issue on the corresponding repository of the openGemini community.

  For details about how to submit an issue, see the [Issue Submission Guide](https://github.com/openGemini/openGemini/issues/new/choose). To attract more attention, you can also attach the issue link in an email and send it to everyone through the [Mailing Lists](https://groups.google.com/g/openGemini).

- **Participate in discussions within the Issue**
  There may have been exchanges and discussions between participants under each Issue. If you are interested, you can also express your opinions in the comment box.

- **Find an Issue you are willing to handle**
  If you would like to work on one of the issues, you can assign it to yourself. Just enter /assign or /assign @yourself in the comment box, and the robot will assign the question to you, and your name will be displayed in the list of responsible persons. 
  
  > Replace @yourself with your GitHub username, such as /assign @bob

## Source Code Contribution 

### Detailed steps for submitting a pull request

1. Before submitting a pull request, please search for related closed or opened PRs in [Github](https://github.com/openGemini/openGemini/pulls) to avoid duplication of work.

2. Make sure the issue describes the problem you are fixing, or documents the design of the feature you want to add. Discussing the design ahead of time helps ensure we are ready to accept your work.

3. Sign the openGemini [DCO](Developer Certificate of Origin, Developer’s Original Statement) and abide by the original contract. Every time you submit a PR, you need to sign it with your email address because we cannot accept code without a signed DCO.

    > Add the -s parameter to the git commit command to automatically sign. For example: git commit -s -m "fix: xxxxxxxx"

4. [Fork](https://github.com/openGemini/openGemini/fork) openGemini/openGemini repository
    You need to know how to download the code on GitHub, incorporate the code through PR, etc. openGemini uses the GitHub code hosting platform. For specific guidance, please refer to the [GitHub Workflow Guide](https://docs.github.com/cn).

5. Clone your repository, and in your repository, make the changes in a new git branch:

    ```shell
    > git checkout -b my-fix-branch main
    ```

6. Add your code and test cases

7. Use git tools to complete your commit.

    ```shell
    //Add files to the staging area
    > git add .
    
    // Submit the staging area to the local warehouse, add the -s parameter, and automatically sign
    > git commit -s -m "<your commit message>"
    ```

    Among them, <your commit message> is your commit message and needs to follow the following naming convention:
    - feat: abbreviation of feature, new function or feature
    - fix: bug fix
    - docs: Document modification
    - style: format modification. For example, changing indentation, spaces, deleting extra blank lines, and filling in missing semicolons. In short, it is a modification that does not affect the meaning and function of the code.
    - refactor: code refactoring. Some code modifications that do not fix bugs or add new features
    - perf: abbreviation for performance, improves code performance
    - test: Modification of test files
    - Chore: other small changes. Generally, changes of only one or two lines, or small changes submitted several times in a row fall into this category
    For more details, you can refer to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

8. Push your branch to Github

    ```shell
    > git push origin my-fix-branch
    // Sometimes you may use --force to force a commit
    > git push origin my-fix-branch --force
    ```

9. Open PR and start merging requests
  When you submit a PR, it means that you have started to contribute code to the community. Please refer to the openGemini community PR submission instructions.
  To make your submission more likely to be accepted, you need to:

   - Fill in the complete submission information and sign the DCO.

   - If the amount of code submitted at one time is large, it is recommended to break down the large content into a series of logically smaller content. Submitting them separately will make it easier for the reviewer to understand your ideas.
   
   > Note: If your PR request does not attract enough attention, you can send an email to [community.ts@opengemini.org](mailto:community.ts@opengemini.org) for help.

### Compile source code

#### Support platforms

We support the following platforms:

- Linux x86/arm (64bit)
- Darwin x86/arm (64bit)
- Windows x86 (64bit)

#### Compilation environment information

[GO](https://golang.org/dl/) version v1.20+

[Python](https://www.python.org/downloads/) version v3.7+

[Git](https://git-scm.com/downloads)

#### GO environment variable settings

Open the ~/.profile configuration file and add the following configuration at the end of the file:

```shell
 # Set GOPATH (customizable directory)
 > export GOPATH=$HOME/gocodez
 > export GOPROXY=https://goproxy.cn,direct
 > export GO111MODULE=on
 > export GONOSUMDB=*
 > export GOSUMDB=off
```

Download source code and compile

```shell
 > cd $GOPATH
 > mkdir -p {pkg,bin,src}
 > cd src
 > git clone git@github.com:<username>/openGemini.git
 > cd openGemini
 > python3 build.py --clean
```

After successful compilation, the binary is saved in the build directory.

### Start the service

#### Start the stand-alone version

- Start with default parameters
   
   ```shell
   > ./build/ts-server
   ```
- Start with configuration file
   
   ```shell
   > ./build/ts-server run -config=config/openGemini.singlenode.conf
   ```
- Launch using script
   
   ```\
   > bash scripts/install.sh
   ```

#### Start the pseudo-cluster version

```shell
> bash scripts/install_cluster.sh
```

> maxOS users may need to enter the admin password for the first time running, refer to https://superuser.com/questions/458875/how-do-you-get-loopback-addresses-other-than-127-0-0-1- to-work-on-os-x, release 127.0.0.2 and 127.0.0.3 temporarily.
>



### CI and static analysis tools

#### CI (Continuous Integration)

All pull requests will run CI. Community contributors should review the results of PR checks to see if they meet the minimum threshold for code inclusion. Please resolve any issues if any to ensure timely review by a team member.

The openGemini project also has a lot of internal inspection processes. This may take some time and is not really visible to community contributors. We will regularly synchronize issues and fix codes to the community.

#### Code Static Analysis

This project uses the following static analysis tools. Failure to run any of these tools will cause the build to fail. Typically, code must be adapted to meet the requirements of these tools, but there are exceptions.

- [go vet](https://golang.org/cmd/vet/)  is used to analyze common errors and potential bugs in Go code. It can check various problems that may exist in the code, such as: unused variables, functions or packages, suspicious function calls, etc.
   By executing the following command in the root directory of this project:
   
   ```sh
   > make go-vet-check
   ```
   
- [goimports-reviser](https://github.com/incu6us/goimports-reviser) import grouping sorting and code formatting.
   By executing the following command in the root directory of this project:
   
   ```sh
   > make style-check
   ```
   
- [go mod tidy](https://tip.golang.org/cmd/go/#hdr-Add_missing_and_remove_unused_modules) downloads and adds the names and versions of the third-party open source components that the project depends on to the go.mod file to solve the problem of project missing dependencies. It will also remove the dependencies that the project does not need in the go.mod file.

- [staticcheck](https://staticcheck.io/docs/)  checks for: unused code, code that can be simplified, incorrect code, unsafe code, and code that will have performance issues.
   By executing the following command in the root directory of this project:
   
   ```sh
   > make static-check
   ```
   
   > Note: Due to problems with static-check itself, it can only be executed if the local go version is 1.19 or above.



## Participate in other contributions to the community

### Contribute ecological tools

If you find that other third-party software systems and tools lack support for openGemini, or openGemini lacks support for southbound operating systems, CPU architectures, and storage systems, you can help openGemini provide this support. At the same time, the community will also publish ecological tool development tasks in ISSUE, and you can also take the initiative to take on the tasks. The process of contributing ecological tools is a process that helps openGemini prosper the ecosystem, making openGemini an open source time series database system with a broad technology ecosystem.

Community's process:

- Submit an issue (custom) under the openGemini warehouse and explain the specific requirements
- Submit a PR and associate with the issue
- Notify the community through community [mailing lists](https://groups.google.com/g/openGemini)/ [Slak](https://join.slack.com/t/huawei-ipz9493/shared_invite/zt-1bvxs3s0i-h0BzP7ibpWfqmpJO2a4iKw), etc., and share in the community
- Merged

If support for openGemini is implemented on other third-party software systems or tools, the community can be notified directly through the community  [mailing lists](https://groups.google.com/g/openGemini)/ [Slak](https://join.slack.com/t/huawei-ipz9493/shared_invite/zt-1bvxs3s0i-h0BzP7ibpWfqmpJO2a4iKw), etc.

### Contribute your own projects

If you want to contribute your original applications or solutions developed based on openGemini to the openGemini community, you can Create an original project directly in https://github.com/openGemini .

Community's process:

- Contact the community through community [mailing lists](https://groups.google.com/g/openGemini)/ [Slak](https://join.slack.com/t/huawei-ipz9493/shared_invite/zt-1bvxs3s0i-h0BzP7ibpWfqmpJO2a4iKw), etc., Tell us about your project , and expalin why apply to join the community (no fixed template)
- Share the project at community meetings
- Waiting for the approval of the community, and then will create a new code warehouse in the community for you, and enable the corresponding permissions

### View code

openGemini is an open community, and we hope that everyone who participates in the community can become an active code reviewer. When you become the committer or maintainer role of the SIG group, you have the responsibility and rights to review the code.
It is strongly recommended to adhere to the [code of conduct](./CODE_OF_CONDUCT.md), respect each other and promote collaboration, hoping to promote the active participation of new contributors without causing contributors to be overwhelmed by minor mistakes from the beginning. Therefore, when reviewing, you can focus on the following:

  1. Is the idea behind the contribution reasonable ?
  2. Is the contributed architecture correct ?
  3. Whether the contribution is complete

### Testing

In order to successfully release a community version, a variety of testing activities need to be completed. Different testing activities, the location of the test code will also be different, and the details of the environment required to successfully run the test will also be different. For relevant information, please see [Test Contribution Guide for Community Developers]().

### Participate in non-code contributions

If your interests lie outside of writing code, you can find jobs that interest you in the  [Non-Code Contributions]().

## Grow with the community

### Community roles

Different roles in the community correspond to different responsibilities and rights. Each role is an integral part of the community. You can continue to accumulate experience and influence through active contributions, and grow in your role. For more detailed role descriptions and descriptions of responsibilities and rights, please see [Community Membership]().

### Technical Committee

The openGemini Technical Committee (TC) is the technical decision-making body of the openGemini community and is responsible for community technical decision-making and coordination of technical resources.
