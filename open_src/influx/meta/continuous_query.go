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
	"time"

	"github.com/gogo/protobuf/proto"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

func (data *Data) CreateContinuousQuery(dbName string, cqi *ContinuousQueryInfo) error {
	dbi, err := data.GetDatabase(dbName)
	if err != nil {
		return err
	}
	err = checkCanCreateContinuousQuery(dbi, cqi)
	if err != nil {
		return err
	}

	if dbi.ContinuousQueries == nil {
		dbi.ContinuousQueries = make(map[string]*ContinuousQueryInfo)
	}
	dbi.ContinuousQueries[cqi.Name] = cqi
	data.MaxCQChangeID++
	return nil
}

func checkCanCreateContinuousQuery(dbi *DatabaseInfo, cqi *ContinuousQueryInfo) error {
	// validate continuous query
	if cqi == nil {
		return ErrContinuosQueryRequired
	} else if cqi.Name == "" {
		return ErrContinuosQueryNameRequired
	} else if cqi.Query == "" {
		return ErrContinuosQuerySourceRequired
	}

	if err := cqi.CheckSpecValid(); err != nil {
		return err
	}

	if _, ok := dbi.ContinuousQueries[cqi.Name]; ok {
		// Benevor TODO: how about a cq with the same name but marked as deleted.
		return ErrSameContinuousQueryName
	}

	// check same action
	if err := dbi.CheckConfilctWithConfiltCq(cqi); err != nil {
		return err
	}

	return nil
}

// DropContinuousQuery drops one continuous query and notify ALL sql nodes that CQ has been changed.
func (data *Data) DropContinuousQuery(cqName string, database string) (bool, error) {
	dbi, err := data.GetDatabase(database)
	if err != nil {
		return false, err
	}
	if _, ok := dbi.ContinuousQueries[cqName]; !ok {
		return false, nil
	}
	delete(dbi.ContinuousQueries, cqName)
	data.MaxCQChangeID++
	return true, nil
}

func (data *Data) BatchUpdateContinuousQueryStat(cqStates []*proto2.CQState) error {
	for _, dbi := range data.Databases {
		for _, cqStat := range cqStates {
			if cqi, ok := dbi.ContinuousQueries[cqStat.GetName()]; ok {
				cqi.UpdateContinuousQueryStat(cqStat.GetLastRunTime())
			}
		}
	}
	return nil
}

// ContinuousQueryInfo represents metadata about a continuous query.
type ContinuousQueryInfo struct {
	// Name of the continuous query to be created.
	Name string

	// String corresponding to continuous query statement
	Query string

	// Mark whether this cq has been deleted
	MarkDeleted bool // TODO: delete me

	// Last successful run time
	LastRunTime time.Time
}

// NewContinuousQueryInfo returns a new instance of ContinuousQueryInfo
// with default values.
func NewContinuousQueryInfo() *ContinuousQueryInfo {
	return &ContinuousQueryInfo{}
}

// Apply applies a specification to the continuous query info.
func (cqi *ContinuousQueryInfo) Apply(spec *ContinuousQuerySpec) *ContinuousQueryInfo {
	cq := &ContinuousQueryInfo{
		Name:        cqi.Name,
		Query:       cqi.Query,
		MarkDeleted: cqi.MarkDeleted,
	}

	if spec.Name != "" {
		cq.Name = spec.Name
	}

	if spec.Query != "" {
		cq.Query = spec.Query
	}

	return cq
}

// Marshal serializes to a protobuf representation.
func (cqi *ContinuousQueryInfo) Marshal() *proto2.ContinuousQueryInfo {
	pb := &proto2.ContinuousQueryInfo{
		Name:        proto.String(cqi.Name),
		Query:       proto.String(cqi.Query),
		MarkDeleted: proto.Bool(cqi.MarkDeleted),
		LastRunTime: proto.Int64(cqi.LastRunTime.UnixNano()),
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (cqi *ContinuousQueryInfo) unmarshal(pb *proto2.ContinuousQueryInfo) {
	cqi.Name = pb.GetName()
	cqi.Query = pb.GetQuery()
	cqi.MarkDeleted = pb.GetMarkDeleted()
	cqi.LastRunTime = time.Unix(0, pb.GetLastRunTime())
}

// Clone returns a deep copy of cqi.
func (cqi ContinuousQueryInfo) Clone() *ContinuousQueryInfo {
	other := cqi
	return &other
}

func (cqi *ContinuousQueryInfo) CheckSpecValid() error {
	// Benevor TODO : what need to check

	return nil
}

func (cqi *ContinuousQueryInfo) UpdateContinuousQueryStat(lastRun int64) {
	cqi.LastRunTime = time.Unix(0, lastRun)
}

type ContinuousQuerySpec struct {
	// Name of the continuous query to be created.
	Name string

	// String corresponding to continuous query statement
	Query string
}

// NewContinuousQueryInfoBySpec creates a new continuous query info from the specification.
func (s *ContinuousQuerySpec) NewContinuousQueryInfoBySpec() *ContinuousQueryInfo {
	return NewContinuousQueryInfo().Apply(s)
}
