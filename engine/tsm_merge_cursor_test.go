// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	assert2 "github.com/stretchr/testify/assert"
)

func TestAddLocationsWithLimit(t *testing.T) {
	files := []immutable.TSSPFile{MocTsspFile{}, MocTsspFile{}}
	l := &immutable.LocationCursor{}
	opt := &query.ProcessorOptions{
		Ascending: false,
		Limit:     3,
		StartTime: 0,
		EndTime:   10,
	}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	ctx := &idKeyCursorContext{
		schema:      record.Schemas{record.Field{Type: influx.Field_Type_Int, Name: "value"}},
		querySchema: qs,
		decs:        immutable.NewReadContext(qs.Options().IsAscending()),
		tr:          util.TimeRange{Min: 0, Max: 10},
	}
	AddLocationsWithLimit(l, files, ctx, 0527)
	assert2.Equal(t, 1, l.Len())
}

func TestNotAddLocationsWithLimit(t *testing.T) {
	files := []immutable.TSSPFile{MocTsspFile{}, MocTsspFile{}}
	l := &immutable.LocationCursor{}
	opt := &query.ProcessorOptions{
		Ascending: false,
		Limit:     3,
		StartTime: 0,
		EndTime:   5,
	}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	ctx := &idKeyCursorContext{
		schema:      record.Schemas{record.Field{Type: influx.Field_Type_Int, Name: "value"}},
		querySchema: qs,
		decs:        immutable.NewReadContext(qs.Options().IsAscending()),
		tr:          util.TimeRange{Min: 0, Max: 5},
	}
	AddLocationsWithLimit(l, files, ctx, 0527)
	assert2.Equal(t, 2, l.Len())
}
