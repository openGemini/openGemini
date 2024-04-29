/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package obs

import (
	"path"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

var PrefixDataPath string

const (
	shardSeparator     = "_"
	pathSeparator      = "/"
	StoreDirName       = "data"
	ColumnStoreDirName = "columnstore"
	ObsFileSuffix      = ".obs"
	ObsFileTmpSuffix   = ".init"
)

// "data/dbName/ptID/rpName/shardId_startTime_endTime_indexId/columnstore"
func GetShardPath(shardId, IndexId uint64, ptId uint32, startTime, endTime time.Time, databaseName, rpName string) string {
	shardPath := strconv.FormatUint(shardId, 10) + shardSeparator + strconv.FormatInt(meta.MarshalTime(startTime), 10) +
		shardSeparator + strconv.FormatInt(meta.MarshalTime(endTime), 10) +
		shardSeparator + strconv.FormatUint(IndexId, 10)
	logPath := path.Join(StoreDirName, databaseName, strconv.FormatUint(uint64(ptId), 10), rpName, shardPath, ColumnStoreDirName)
	return logPath
}

// "data/dbName/ptID/rpName/shardId_startTime_endTime_indexId/columnstore/mstName"
func GetBaseMstPath(shardPath, mstName string) string {
	return path.Join(shardPath, mstName)
}

// "localDataPath/data/dbName/ptID/rpName/shardId_startTime_endTime_indexId/columnstore/mstName"
func GetLocalMstPath(localPath, mstPath string) string {
	return path.Join(localPath, mstPath)
}

func SetPrefixDataPath(dataPath string) {
	PrefixDataPath = dataPath
}

func GetPrefixDataPath() string {
	return PrefixDataPath
}
