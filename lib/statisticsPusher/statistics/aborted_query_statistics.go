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
package statistics

const (
	System = "system"
	User   = "user"

	StatAbortQuerySource = "source"
	AbortedQueryStat     = "aborted_queries"
)

type AbortedQueryStatistics struct {
	Query  string
	Source string
}

var abortedQueries = make(chan *AbortedQueryStatistics, 256)

func NewAbortedQueryStatistics(query, source string) *AbortedQueryStatistics {
	return &AbortedQueryStatistics{
		Query:  query,
		Source: source,
	}
}

func AppendAbortedQueryStat(d *AbortedQueryStatistics) {
	if d == nil {
		return
	}
	select {
	case abortedQueries <- d:
	default:
	}
}

func CollectAbortQueryStatistics(buffer []byte) ([]byte, error) {
	queries := getAbortedQuery()
	for _, q := range queries {
		tagMap := make(map[string]string)
		tagMap[StatAbortQuerySource] = q.Source
		valueMap := map[string]interface{}{
			"query": q.Query,
		}
		buffer = AddPointToBuffer(AbortedQueryStat, tagMap, valueMap, buffer)

	}
	return buffer, nil
}

func getAbortedQuery() (ds []*AbortedQueryStatistics) {
	for {
		select {
		case d := <-abortedQueries:
			ds = append(ds, d)
		default:
			return
		}
	}
}
