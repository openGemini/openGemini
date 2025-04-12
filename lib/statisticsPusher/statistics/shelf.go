// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

var shelf = &Shelf{}

func init() {
	NewCollector().Register(shelf)
}

func NewShelf() *Shelf {
	shelf.enabled = true
	return shelf
}

type Shelf struct {
	BaseCollector

	ActiveShardTotal  *ItemInt64
	WALWaitConvert    *ItemInt64 // Number of WAL files waiting to be converted to TSSP
	WALConverting     *ItemInt64 // Number of WAL files being converted
	WALFileCount      *ItemInt64
	WALFileSizeSum    *ItemInt64 // KB
	TSSPConvertCount  *ItemInt64
	TSSPConvertDurSum *ItemInt64 // milliseconds

	WriteFailedCount *ItemInt64
	WriteCount       *ItemInt64
	WriteDurSum      *ItemInt64 // microseconds
	ScheduleDurSum   *ItemInt64 // microseconds

	IndexCreateCount  *ItemInt64
	IndexCreateDurSum *ItemInt64 // microseconds

	// statistics the number of disk operations and latency
	DiskFlushCount  *ItemInt64
	DiskFlushDurSum *ItemInt64 // microseconds
	DiskSyncCount   *ItemInt64
	DiskSyncDurSum  *ItemInt64 // microseconds
}
