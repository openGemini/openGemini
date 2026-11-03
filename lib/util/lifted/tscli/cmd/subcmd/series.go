// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package subcmd

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// searchSeriesKeys adapts the server's public lookup API to the encoded keys
// used by the export parsers. GetSeries expands tag arrays and filters entries.
func searchSeriesKeys(getSeries func(uint64, []byte, influxql.Expr, func(*influx.SeriesKey)) error, sid uint64) ([][]byte, error) {
	var keys [][]byte
	err := getSeries(sid, nil, nil, func(key *influx.SeriesKey) {
		row := influx.Row{Name: string(key.Measurement)}
		for _, tag := range key.TagSet {
			row.Tags = append(row.Tags, influx.Tag{Key: string(tag.Key), Value: string(tag.Value)})
		}
		row.UnmarshalIndexKeys(nil)
		keys = append(keys, row.IndexKey)
	})
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no series keys for series ID %d", sid)
	}
	return keys, nil
}
