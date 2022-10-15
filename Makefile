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

GOPATH ?= $(shell go env GOPATH)
export PATH := $(PATH):$(GOPATH)/bin

GO          := GO111MODULE=on go
GOTEST      := $(GO) test
PYTHON      := python

FAILPOINT_ENABLE  := find $$PWD/ -type d | grep -vE "(\.git|\.github|\.tests)" | xargs failpoint-ctl enable
FAILPOINT_DISABLE := find $$PWD/ -type d | grep -vE "(\.git|\.github|\.tests)" | xargs failpoint-ctl disable

PACKAGE_LIST_OPEN_GEMINI_TESTS  := go list ./... | grep -vE "tests|open_src\/github.com\/hashicorp"
PACKAGES_OPEN_GEMINI_TESTS ?= $$($(PACKAGE_LIST_OPEN_GEMINI_TESTS))

COPYRIGHT_EXCEPT  := "open_src|tests|lib/netstorage/data/data.pb.go|lib/statisticsPusher/statistics/handler_statistics.go|app/ts-meta/meta/snapshot.go|engine/index/tsi/tag_filters.go|engine/index/mergeindex/item.go"
COPYRIGHT_GOFILE  := $$(find . -name '*.go' | grep -vE $(COPYRIGHT_EXCEPT))
COPYRIGHT_HEADER  := "Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd."

.PHONY: go-build style-check gotest integration-test buildsucc

default: gotest

all: go-build buildsucc

go-build:
	@$(PYTHON) build.py --clean

buildsucc:
	@echo Build openGemini successfully!

install-goimports-reviser:
	@$(GO) install github.com/incu6us/goimports-reviser/v3@v3.1.0

licence-check:
	@echo "run licence check"
	@for file in $(COPYRIGHT_GOFILE); \
		do \
  			cat $$file | grep -qs $(COPYRIGHT_HEADER) || { echo $$file "has no licence header" >> licence-check.log; }; \
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


style-check: install-goimports-reviser
	@echo "run style check for import pkg order"
	for file in $$(find . -name '*.go'); do if ! goimports-reviser -project-name fuck -set-exit-status $$file; then exit 1; fi; done

install-failpoint:
	@$(GO) install github.com/pingcap/failpoint/failpoint-ctl

failpoint-enable:
	@$(FAILPOINT_ENABLE)

failpoint-disable:
	@$(FAILPOINT_DISABLE)

gotest: install-failpoint failpoint-enable
	@echo "running gotest begin."
	@index=0; for s in $(PACKAGES_OPEN_GEMINI_TESTS); do index=$$(($$index+1)); if ! $(GOTEST) -failfast -short -v -count 1 -p 1 -timeout 10m -coverprofile coverage_$$index.txt -coverpkg ./... $$s; then $(FAILPOINT_DISABLE); exit 1; fi; done
	@$(FAILPOINT_DISABLE)

integration-test:
	@echo "running integration test begin."
	@URL=http://127.0.0.1:8086 $(GOTEST) -mod=mod -test.parallel 1 -timeout 10m ./tests -v GOCACHE=off -args "normal"