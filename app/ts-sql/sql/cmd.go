// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package ingestserver

import (
	"fmt"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/lib/config"
)

func NewCommand(info app.ServerInfo, enableGossip bool) *app.Command {
	cmd := app.NewCommand()
	cmd.Info = info
	cmd.Logo = app.SQLLOGO
	cmd.Version = info.FullVersion()
	cmd.Usage = fmt.Sprintf(app.RunUsage, info.App, info.App)
	cmd.Config = config.NewTSSql(enableGossip)
	cmd.NewServerFunc = NewServer

	return cmd
}
