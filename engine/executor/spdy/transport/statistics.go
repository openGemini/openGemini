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

package transport

import stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"

const (
	AppSql = iota
	AppMeta
	AppStore
)

func InitStatistics(app int) {
	switch app {
	case AppSql:
		NewNodeManager().SetJob(stat.NewSpdyJob(stat.Sql2Store))
		NewWriteNodeManager().SetJob(stat.NewSpdyJob(stat.Sql2Store))
		NewMetaNodeManager().SetJob(stat.NewSpdyJob(stat.Sql2Meta))
	case AppMeta:
		NewNodeManager().SetJob(stat.NewSpdyJob(stat.Meta2Store))
		NewMetaNodeManager().SetJob(stat.NewSpdyJob(stat.Meta2Meta))
	case AppStore:
		NewMetaNodeManager().SetJob(stat.NewSpdyJob(stat.Store2Meta))
	}
}
