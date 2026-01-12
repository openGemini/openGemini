---
order: 3
---

# 处理提交冲突

如果您发现提交的PR带有以下的标记，说明您提交的PR和您本地存在冲突，您需要处理冲突。

<img width="566" alt="image" src="https://github.com/openGemini/openGemini/assets/22270117/3b06b915-03ae-4eb9-880d-2d52f5e2abfb">

## 复制远程仓库到本地

在本地电脑执行拷贝命令：

```bash
# 把远程 fork 仓库复制到本地
git clone https://github.com/$user_name/openGemini.git

# 设置本地工作目录的 upstream 源（被 fork 的上游仓库）
git remote add upstream https://github.com/openGemini/openGemini.git

# 设置同步方式，此处
git remote set-url --push upstream no_push
```

## 保持您的分支和main的同步

```bash
git checkout main
git fetch upstream
git rebase upstream/main
```

## 再将分支切换到您使用的分支上，并开始rebase

```bash
git checkout yourbranch
git rebase main
```

## 此时您可以在git上看到冲突的提示，你可以通过vi等工具查看冲突

## 解决冲突以后，再把修改提交上去

```bash
git add .
git rebase --continue
git push -f origin yourbranch
```
