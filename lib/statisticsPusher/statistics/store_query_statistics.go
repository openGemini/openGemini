// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package statistics

var storeQuery = &StoreQuery{}

func init() {
	NewCollector().Register(storeQuery)
}

func NewStoreQuery() *StoreQuery {
	storeQuery.enabled = true
	return storeQuery
}

type StoreQuery struct {
	BaseCollector

	StoreQueryRequests        *ItemInt64 `name:"storeQueryReq"` // only for SelectProcessor
	StoreQueryRequestDuration *ItemInt64 `name:"storeQueryReqDurationNs"`
	StoreActiveQueryRequests  *ItemInt64 `name:"storeQueryReqActive"`

	UnmarshalQueryTimeTotal    *ItemInt64 `name:"UnmarshalQueryTimeTotal"`    // use with StoreQueryRequests
	GetShardResourceTimeTotal  *ItemInt64 `name:"GetShardResourceTimeTotal"`  // use with StoreQueryRequests
	IndexScanDagBuildTimeTotal *ItemInt64 `name:"IndexScanDagBuildTimeTotal"` // use with StoreQueryRequests

	// use with StoreQueryRequests and QueryShardNumTotal, each IndexScanDagRunTime end until ChunkReaderDag begin run
	IndexScanDagRunTimeTotal *ItemInt64 `name:"IndexScanDagRunTimeTotal"`

	// use with StoreQueryRequests and QueryShardNumTotal
	IndexScanRunTimeTotal *ItemInt64 `name:"IndexScanRunTimeTotal"`

	// use with StoreQueryRequests
	QueryShardNumTotal *ItemInt64 `name:"QueryShardNumTotal"`

	// use with StoreQueryRequests and QueryShardNumTotal
	IndexScanSeriesNumTotal *ItemInt64 `name:"IndexScanSeriesNumTotal"`

	// use with StoreQueryRequests and QueryShardNumTotal
	ChunkReaderDagBuildTimeTotal *ItemInt64 `name:"ChunkReaderDagBuildTimeTotal"`
	// use with StoreQueryRequests and QueryShardNumTotal
	ChunkReaderDagRunTimeTotal *ItemInt64 `name:"ChunkReaderDagRunTimeTotal"`
}

func (s *StoreQuery) MeasurementName() string {
	return "store_query"
}
