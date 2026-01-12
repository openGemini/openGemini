---
order: 2
---

# Contribute Code

openGemini is maintained, improved, and extended by code contributions. We welcome code contributions to openGemini. openGemini uses a workflow based on [pull requests](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests).

## Before contributing

Contributing to openGemini does *not* start with opening a pull request. We expect contributors to reach out to us first to discuss the overall approach together. Without consensus with the openGemini committers, contributions might require substantial rework or will not be reviewed. So please [create a GitHub issue](report_issue.md), discuss under an existing issue, and reach consensus.

For newcomers, you can check the [starter issues](https://github.com/openGemini/openGemini/issues?q=is%3Aissue+label%3A%22good+first+issue%22), which are annotated with a "good first issue" label. These are issues suitable for new contributors to work with and won't take long to fix. But because the label is typically added at triage time it can turn out to be inaccurate, so do feel free to leave a comment if you think the classification no longer applies.

## Contributing process

After a consensus is reached in issues, it's time to start the code contributing process:

1. Assign the issue to yourself via [/assign](http://prow.openGemini.org/command-help?repo=pingcap%2FopenGemini#assign). This lets other contributors know you are working on the issue so they won't make duplicate efforts.
2. Follow the [GitHub workflow](https://guides.github.com/introduction/flow/), commit code changes in your own git repository branch and open a pull request for code review.
3. Make sure the continuous integration checks on your pull request are green (i.e. successful).
4. Review and address [comments on your pull request](https://docs.github.com/en/github/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/commenting-on-a-pull-request).
5. When your pull request gets enough approvals (the default number is 1) and all other requirements are met, it will be merged.

Clear and kind communication is key to this process.

## Referring to an issue

Code repositories in openGemini community require **ALL** the pull requests referring to its corresponding issues. In the pull request body, there **MUST** be one line starting with `Issue Number: ` and linking the relevant issues via the [keyword](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword), for example:

If the pull request resolves the relevant issues, and you want GitHub to close these issues automatically after it merged into the default branch, you can use the syntax (`KEYWORD #ISSUE-NUMBER`) like this:

```
Issue Number: close #123
```

If the pull request links an issue but does not close it, you can use the keyword `ref` like this:

```
Issue Number: ref #456
```

Multiple issues should use full syntax for each issue and separate by a comma, like:

```
Issue Number: close #123, ref #456
```

For pull requests trying to close issues in a different repository, contributors need to first create an issue in the same repository and use this issue to track.

If the pull request body does not provide the required content, the bot will add the `do-not-merge/needs-linked-issue` label to the pull request to prevent it from being merged.

## Writing tests

One important thing when you make code contributions to openGemini is tests. Tests should be always considered as a part of your change. Any code changes that cause semantic changes or new function additions to openGemini should have corresponding test cases. And of course you can not break any existing test cases if they are still valid. It's recommended to [run tests](../get_started/test_tutorials.md) on your local environment first to find obvious problems and fix them before opening the pull request.

It's also highly appreciated if your pull request only contains test cases to increase test coverage of openGemini. Supplement test cases for existing modules is a good and easy way to become acquainted with existing code.

## Making good pull requests

When creating a pull request for submission, there are several things that you should consider to help ensure that your pull request is accepted:

* Does the contribution alter the behavior of features or components in a way that it may break previous users' programs and setups? If yes, there needs to be a discussion and agreement that this change is desirable.
* Does the contribution conceptually fit well into openGemini? Is it too much of a special case such that it makes things more complicated for the common case, or bloats the abstractions/APIs?
* Does the contribution make a big impact on openGemini's build time?
* Does your contribution affect any documentation? If yes, you should add/change proper documentation.
* If there are any new dependencies, are they under active maintenances? What are their licenses?

## Making good commits

Each feature or bugfix should be addressed by a single pull request, and for each pull request there may be several commits. In particular:

* Do *not* fix more than one issues in the same commit (except, of course, if one code change fixes all of them).
* Do *not* do cosmetic changes to unrelated code in the same commit as some feature/bugfix.

## Waiting for review

To begin with, please be patient! There are many more people submitting pull requests than there are people capable of reviewing your pull request. Getting your pull request reviewed requires a reviewer to have the spare time and motivation to look at your pull request. If your pull request has not received any notice from reviewers (i.e., no comment made) for some time, you can ping the reviewers and assignees for more attention.

When someone does manage to find the time to look at your pull request, they will most likely make comments about how it can be improved (don't worry, even committers/maintainers have their pull requests sent back to them for changes). It is then expected that you update your pull request to address these comments, and the review process will thus iterate until a satisfactory solution has emerged.
