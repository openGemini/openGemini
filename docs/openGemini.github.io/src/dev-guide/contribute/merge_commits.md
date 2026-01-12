---
order: 4
---

# Merge commits

If you submit a PR, modify it according to the review comments and submit the PR again, and you don't want the reviewers to see the PR submitted multiple times, because it is not convenient to continue to modify in the review, then you can merge the submitted PR. The PR of the merge submission is realized by compressing the Commit.

1. Now view the log on the local branch

```bash
git log
```

2. Then aggregate the top n submission records together and enter. Note that n is a number.

```bash
git rebase -i HEAD~n
```

Change the pick in front of the logs that need to be compressed to s, s is the abbreviation of squash. Note that one pick must be kept. If all picks are changed to s, there will be no merge target, and an error will occur.

3. After the modification is completed, press the ESC key, and then enter `:wq`, and an interface will pop up, asking you whether to enter the page for editing and submitting notes. After entering e, you will enter the page for merging the commit message. Please delete all the messages that need to be merged, and only keep the message of the merge target, then press the ESC key, and enter `:wq` to save and exit.

4. Final submission

```bash
git push -f origin yourbranch
```

5. Go back to the PR submission page on github, and you can see that the previous submissions have been merged.
