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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	internal "github.com/openGemini/openGemini/open_src/influx/query/proto"
)

type LogPath struct {
	DatabaseName    string
	MeasurementName string
	SegmentPath     string
	Ak              string
	Sk              string
	BucketName      string
	Endpoint        string
	BasePath        string
	Version         uint32
}

func (l *LogPath) IsObsEnabled() bool {
	return l.Ak != "" && l.Sk != ""
}

func DecodeLogPaths(internalLogPaths []*internal.LogPath) []*LogPath {
	ret := make([]*LogPath, 0, len(internalLogPaths))
	for _, internalLogPath := range internalLogPaths {
		ret = append(ret, DecodeLogPath(internalLogPath))
	}
	return ret
}

func DecodeLogPath(internalLogPath *internal.LogPath) *LogPath {
	return &LogPath{
		DatabaseName:    internalLogPath.DatabaseName,
		MeasurementName: internalLogPath.MeasurementName,
		SegmentPath:     internalLogPath.SegmentPath,
		Ak:              internalLogPath.Ak,
		Sk:              internalLogPath.Sk,
		BucketName:      internalLogPath.BucketName,
		Endpoint:        internalLogPath.Endpoint,
		BasePath:        internalLogPath.BasePath,
		Version:         internalLogPath.Version,
	}
}

func EncodeLogPaths(logPaths []*LogPath) []*internal.LogPath {
	ret := make([]*internal.LogPath, 0, len(logPaths))
	for _, logPath := range logPaths {
		ret = append(ret, EncodeLogPath(logPath))
	}
	return ret
}

func EncodeLogPath(logPath *LogPath) *internal.LogPath {
	return &internal.LogPath{
		DatabaseName:    logPath.DatabaseName,
		MeasurementName: logPath.MeasurementName,
		SegmentPath:     logPath.SegmentPath,
		Ak:              logPath.Ak,
		Sk:              logPath.Sk,
		BucketName:      logPath.BucketName,
		Endpoint:        logPath.Endpoint,
		BasePath:        logPath.BasePath,
		Version:         logPath.Version,
	}
}

var filePathCache = sync.Map{}

func GetFilePath(filePath string) string {
	v, ok := filePathCache.Load(filePath)
	if ok {
		return v.(string)
	}
	return ""
}

func SetFilePath(filePath, checkPath string) {
	filePathCache.Store(filePath, checkPath)
}

const (
	shardSeparator = "_"
	pathSeparator  = "/"
	StoreDirName   = "data"
)

func GetLogPath(shardId, IndexId uint64, ptId uint32, startTime, endTime time.Time, databaseName, measurementName string, rpName string) string {
	shardPath := strconv.FormatUint(shardId, 10) + shardSeparator + strconv.FormatInt(meta.MarshalTime(startTime), 10) +
		shardSeparator + strconv.FormatInt(meta.MarshalTime(endTime), 10) +
		shardSeparator + strconv.FormatUint(IndexId, 10)
	logPath := path.Join(StoreDirName, databaseName, strconv.FormatUint(uint64(ptId), 10), rpName, shardPath, config.ColumnStoreDirName, measurementName)
	checkPath := GetFilePath(logPath)
	if checkPath != "" {
		return checkPath
	}
	shardPathT := "*" + shardSeparator + strconv.FormatInt(meta.MarshalTime(startTime), 10) +
		shardSeparator + strconv.FormatInt(meta.MarshalTime(endTime), 10) +
		shardSeparator + "*"
	logPathT := path.Join(StoreDirName, databaseName, strconv.FormatUint(uint64(ptId), 10), rpName, shardPathT, config.ColumnStoreDirName, measurementName)
	fullPathT := filepath.Join(config.GetDataDir(), logPathT)
	matches, err := filepath.Glob(fullPathT)
	if err == nil && len(matches) > 0 {
		if config.GetDataDir()[len(config.GetDataDir())-1] == '/' {
			checkPath = matches[0][len(config.GetDataDir()):]
		} else {
			checkPath = matches[0][len(config.GetDataDir())+1:]
		}
		SetFilePath(logPath, checkPath)
		return checkPath
	}
	return logPath
}

func GetDatabasePath(databaseName string) string {
	return path.Join(StoreDirName, databaseName)
}

func GetMeasurementPath(ptId uint32, databaseName, measurementName string) string {
	return path.Join(StoreDirName, databaseName, strconv.FormatUint(uint64(ptId), 10), measurementName)
}

type LogPathInfo struct {
	Database    string
	Measurement string
	PtId        uint32
	ShardId     uint64
	StartTime   int64
	EndTime     int64
}

func (info *LogPathInfo) Contains(t int64) bool {
	return (t >= info.StartTime) && (t < info.EndTime)
}

// for example: data/db0/0/rp0/1_-259200000000000_345600000000000_1/columnstore/cpu_0000
func ParseLogPath(logPath string) (*LogPathInfo, error) {
	logPathSplit := strings.Split(logPath, pathSeparator)
	if len(logPathSplit) < 7 {
		return nil, fmt.Errorf("not a standard path: %s", logPath)
	}

	logInfo := &LogPathInfo{}
	logInfo.Database = logPathSplit[1]
	ptId, err := strconv.ParseUint(logPathSplit[2], 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.InvalidDataDir)
	}
	logInfo.PtId = uint32(ptId)
	logInfo.Measurement = logPathSplit[6]

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
