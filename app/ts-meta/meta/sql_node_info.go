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

package meta

import (
	"container/list"
	"time"
)

// cqLeaseInfo represents metadata about a ts-sql.
type cqLeaseInfo struct {
	LastHeartbeat *list.Element
	CQNames       []string
}

type HeartbeatInfo struct {
	Host              string
	LastHeartbeatTime time.Time
}

type CqInfo struct {
	// All CQ names that running on this ts-sql.
	RunningCQNames []string

	RunningCqs map[string]string

	// All cqs that assigned to this ts-sql. move to Cqs when ts-sql get these cqs.
	AssignCqs map[string]string

	// All cqs that should revoke from this ts-sql. Delete corresponding cqs in RunningCqs when ts-sql get these cqs.
	RevokeCqs map[string]string

	IsNew bool
}

func (i *CqInfo) GetLoad() int {
	return len(i.RunningCqs) + len(i.AssignCqs) - len(i.RevokeCqs)
}
