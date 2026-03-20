#!/usr/bin/env bash

STYLE_CHECK_EXCEPT="lifted/hashicorp|lifted/protobuf"
STYLE_CHECK_GOFILE=$(find . -name '*.go' | grep -vE $STYLE_CHECK_EXCEPT)

echo "run style check for import pkg order"
for file in $STYLE_CHECK_GOFILE; do goimports-reviser -project-name none-pjn $file >> /dev/null 2>&1; done

git checkout -f go.mod
go mod tidy

GIT_STATUS=`git status | grep "Changes not staged for commit"`;
if [ "$GIT_STATUS" = "" ]; then
  echo "code already go formatted";
else
  echo "style check failed, please format your code using goimports-reviser";
  echo "ref: github.com/incu6us/goimports-reviser";
  echo "git diff files:";
  git diff --stat | tee;
  echo "git diff details: ";
  git diff | tee;
  exit 1;
fi