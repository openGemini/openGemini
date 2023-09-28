# Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include Makefile.common

.PHONY: go-build style-check gotest integration-test buildsucc static-check start-subscriber stop-subscriber

default: gotest

all: go-build buildsucc

go-build:
	@$(PYTHON) build.py --clean

buildsucc:
	@echo Build openGemini successfully!

licence-check:
	@echo "run licence check"
	@for file in $(COPYRIGHT_GOFILE); \
		do \
  			cat $$file | grep -qsE $(COPYRIGHT_HEADER) || { echo $$file "has no licence header" >> licence-check.log; }; \
	 	done
	@if [ -f licence-check.log ]; \
  	then \
		cat  licence-check.log; \
		rm -f licence-check.log; \
		exit 1; \
	else \
		echo "licence check ok"; \
		exit 0; \
	fi

go-version-check:
	bash ./scripts/ci/go_version_check.sh

style-check: install-goimports-reviser
	@echo "run style check for import pkg order"
	@for file in $(STYLE_CHECK_GOFILE); do goimports-reviser -project-name none-pjn $$file; done
	@GIT_STATUS=`git status | grep "Changes not staged for commit"`; \
		if [ "$$GIT_STATUS" = "" ]; \
		then \
			echo "code already go formatted"; \
		else \
			echo "style check failed, please format your code using goimports-reviser"; \
			echo "ref: github.com/incu6us/goimports-reviser"; \
			echo "git diff files:"; \
			git diff --stat | tee; \
			echo "git diff details: "; \
			git diff | tee; \
			exit 1; \
		fi

go-vet-check:
	bash ./scripts/ci/go_vet.sh

static-check: install-staticcheck
	bash ./scripts/ci/static_check.sh

go-generate: install-tmpl install-goyacc install-protoc install-protoc-gen-gogo
	bash ./scripts/ci/go_generate.sh

golangci-lint-check: install-golangci-lint
	bash ./scripts/ci/golangci_lint_check.sh

gotest: install-failpoint failpoint-enable
	@echo "running gotest begin."
	@index=0; for s in $(PACKAGES_OPEN_GEMINI_TESTS); do index=$$(($$index+1)); if ! $(GOTEST) -failfast -short -v -count 1 -p 1 -timeout 10m -coverprofile coverage_$$index.txt -coverpkg ./... $$s; then $(FAILPOINT_DISABLE); exit 1; fi; done
	@$(FAILPOINT_DISABLE)

build-check:
	@$(PYTHON) build.py --clean --platform windows --arch amd64
	@$(PYTHON) build.py --clean --platform windows --arch arm64
	@$(PYTHON) build.py --clean --platform darwin --arch amd64
	@$(PYTHON) build.py --clean --platform darwin --arch arm64
	@$(PYTHON) build.py --clean --platform linux --arch amd64
	@$(PYTHON) build.py --clean --platform linux --arch arm64

integration-test:
	@echo "running integration test begin."
	@URL=http://127.0.0.1:8086 $(GOTEST) -mod=mod -test.parallel 1 -timeout 10m ./tests -v GOCACHE=off -args "normal"

start-subscriber:
	sed -i '/\[subscriber\]/{n;s/.*/enabled = true/;}' config/openGemini.conf

stop-subscriber:
	sed -i '/\[subscriber\]/{n;s/.*/  # enabled = false/;}' config/openGemini.conf
