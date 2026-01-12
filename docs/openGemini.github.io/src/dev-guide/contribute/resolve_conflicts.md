---
order: 3
---

# Handle commit conflicts

If you find that the submitted PR has the following marks, it means that there is a conflict between the PR you submitted and your local, and you need to deal with the conflict.

<img width="566" alt="image" src="https://github.com/openGemini/openGemini/assets/22270117/3b06b915-03ae-4eb9-880d-2d52f5e2abfb">

## Copy the remote warehouse to the local

Execute the copy command on the local computer:

```bash
# Copy the remote fork repository to the local
git clone https://github.com/$user_name/openGemini.git

# Set the upstream source of the local working directory (forked upstream repository)
git remote add upstream https://github.com/openGemini/openGemini.git

# Set the synchronization method, here
git remote set-url --push upstream no_push
```

## Keep your branch in sync with main

```bash
git checkout main
git fetch upstream
git rebase upstream/main
```

## Then switch the branch to the branch you are using, and start rebase

```bash
git checkout your branch
git rebase main
```

## At this point, you can see the conflict prompt on git, and you can check the conflict through tools such as vi

## After the conflict is resolved, submit the modification

```bash
git add .
git rebase --continue
git push -f origin yourbranch
```
