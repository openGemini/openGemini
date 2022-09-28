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

default: gotest

all: go_build buildsucc

go_build:
	@$(PYTHON) build.py --clean

buildsucc:
	@echo Build openGemini successfully!

install_failpoint:
	@$(GO) install github.com/pingcap/failpoint/failpoint-ctl

failpoint-enable:
	@$(FAILPOINT_ENABLE)

failpoint-disable:
	@$(FAILPOINT_DISABLE)

gotest: install_failpoint failpoint-enable
	@echo "running gotest begin."
	@rm -f gotest.log
	index=0; for s in $(PACKAGES_OPEN_GEMINI_TESTS); do index=$$(($$index+1)); if ! $(GOTEST) -failfast -short -v -count 1 -p 1 -timeout 10m -coverprofile=coverage_$$index.txt $$s >> gotest.log; then $(FAILPOINT_DISABLE); cat gotest.log; exit 1; fi; done
	#$(GOTEST) -v -count 1 -p 1 -timeout 20m -cover $(PACKAGES_OPEN_GEMINI_TESTS) -coverprofile=coverage.out > gotest.log || { $(FAILPOINT_DISABLE); cat gotest.log; exit 1; }
	@$(FAILPOINT_DISABLE)

at:
	@echo "running at begin."
	URL=http://127.0.0.1:8086 $(GO) test -mod=mod -test.parallel 1 -timeout 10m ./tests -v GOCACHE=off -args "normal"