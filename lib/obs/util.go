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
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
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
	shardPath := strconv.FormatUint(shardId, 10) + shardSeparator + strconv.FormatInt(MarshalTime(startTime), 10) +
		shardSeparator + strconv.FormatInt(MarshalTime(endTime), 10) +
		shardSeparator + strconv.FormatUint(IndexId, 10)
	logPath := path.Join(StoreDirName, databaseName, strconv.FormatUint(uint64(ptId), 10), rpName, shardPath, ColumnStoreDirName)
	return logPath
}

// example: data/test/8/mst/9_1756944000000000000_1757030400000000000_9/columnstore/mst_0000, 9 is the shardID
func GetShardID(dataPath string) int64 {
	path := strings.Split(dataPath, "/")
	if len(path) != 7 {
		return 0
	}
	shardPath := strings.Split(path[4], "_")
	if len(shardPath) < 4 {
		return 0
	}
	num, err := strconv.ParseInt(shardPath[0], 10, 64)
	if err != nil {
		return 0
	}
	return num
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

func Join(elem ...string) string {
	for i, e := range elem {
		if e != "" {
			return path.Clean(strings.Join(elem[i:], pathSeparator))
		}
	}
	return ""
}

type LogPathInfo struct {
	RepoName  string
	LogStream string
	PtId      uint32
	ShardId   uint64
	StartTime int64
	EndTime   int64
}

func (info *LogPathInfo) Contains(t int64) bool {
	return (t >= info.StartTime) && (t < info.EndTime)
}

// logPath format: "data/dbName/ptID/rpName/shardId_startTime_endTime_indexId/columnstore"
func ParseLogPath(logPath string) (*LogPathInfo, error) {
	logPathSplit := strings.Split(logPath, pathSeparator)
	// according to the format of logpath, it needs to be divided into at least 6 parts
	if len(logPathSplit) < 6 {
		return nil, fmt.Errorf("not a standard logstream path: %s", logPath)
	}

	logInfo := &LogPathInfo{}
	logInfo.RepoName = logPathSplit[1]
	ptId, err := strconv.ParseUint(logPathSplit[2], 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.InvalidDataDir)
	}
	logInfo.PtId = uint32(ptId)
	logInfo.LogStream = logPathSplit[3]

	shardPathSplit := strings.Split(logPathSplit[4], shardSeparator)
	logInfo.ShardId, err = strconv.ParseUint(shardPathSplit[0], 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.InvalidDataDir)
	}
	logInfo.StartTime, err = strconv.ParseInt(shardPathSplit[1], 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.InvalidDataDir)
	}
	logInfo.EndTime, err = strconv.ParseInt(shardPathSplit[2], 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.InvalidDataDir)
	}

	return logInfo, nil
}

// MarshalTime converts t to nanoseconds since epoch. A zero time returns 0.
func MarshalTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
