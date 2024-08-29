// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"

	parse "github.com/influxdata/influxdb/cmd"
	"github.com/openGemini/openGemini/app/ts-cli/analyzer"
	"github.com/openGemini/openGemini/app/ts-cli/cmd"
)

func main() {
	doRun(os.Args[1:]...)
}

func doRun(args ...string) {
	name, _ := parse.ParseCommandName(args)
	switch name {
	case "analyze":
		az := analyzer.NewAnalyzer()
		az.Analyze(args[1])
		return
	}

	cmd.Execute()
}
