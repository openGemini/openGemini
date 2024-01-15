/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"os"

	"github.com/openGemini/openGemini/app"
	meta "github.com/openGemini/openGemini/app/ts-meta/run"
	"github.com/openGemini/openGemini/app/ts-server/run"
	ingestserver "github.com/openGemini/openGemini/app/ts-sql/sql"
	store "github.com/openGemini/openGemini/app/ts-store/run"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
)

var (
	TsVersion   = "v1.1.0rc0"
	TsCommit    string
	TsBranch    string
	TsBuildTime string
)

func main() {
	app.InitParse()
	errno.SetNode(errno.NodeServer)

	info := app.ServerInfo{
		App:       config.AppSingle,
		Version:   TsVersion,
		Commit:    TsCommit,
		Branch:    TsBranch,
		BuildTime: TsBuildTime,
	}

	cmdMeta := meta.NewCommand(info, false)
	cmdStore := store.NewCommand(info, false)
	cmdSql := ingestserver.NewCommand(info)
	cmdMeta.Logo = app.TSSERVER
	cmdSql.Logo = ""
	cmdStore.Logo = ""

	cmdSql.AfterOpen = func() {
		run.InitStorage(cmdSql.Server, cmdStore.Server)
	}
	app.Run(os.Args[1:], cmdMeta, cmdStore, cmdSql)
}
