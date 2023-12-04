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
	"testing"
	"time"

	internal "github.com/openGemini/openGemini/open_src/influx/query/proto"
	"github.com/stretchr/testify/assert"
)

func TestParseLogPath(t *testing.T) {
	startTime := time.Now().Truncate(24 * time.Hour)
	endTime := startTime.Add(24 * time.Hour)
	segPath := GetLogPath(0, 0, 1, startTime, endTime, "repo", "logstream", "rp0")
	logInfo, err := ParseLogPath(segPath)
	if err != nil {
		t.Fatalf("ParseLogPath error: %+v", err)
	}
	if logInfo.Database != "repo" {
		t.Fatalf("ParseLogPath repo failed, expect: repo, real: %s", logInfo.Database)
	}
	if logInfo.StartTime != startTime.UnixNano() {
		t.Fatalf("ParseLogPath repo failed, expect: %d, real: %d", startTime.UnixNano(), logInfo.StartTime)
	}
	if logInfo.EndTime != endTime.UnixNano() {
		t.Fatalf("ParseLogPath repo failed, expect: %d, real: %d", endTime.UnixNano(), logInfo.EndTime)
	}

	_, err = ParseLogPath("data/cardinality1/test1/7_1695859200000000000_1695945600000000000_7")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/cardinality1/abc/test1/7_1695859200000000000_1695945600000000000_7/logstore/abc")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/cardinality1/0/test1/x_1695859200000000000_1695945600000000000_7/logstore/abc")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/cardinality1/0/test1/7_x695859200000000000_1695945600000000000_7/logstore/abc")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/cardinality1/0/test1/a_1695859200000000000_x695945600000000000_7/logstore/abc")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}

	_, err = ParseLogPath("data/cardinality1/0/test1/1_1695859200000000000_x695945600000000000_7/logstore/abc")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}

	path, err := ParseLogPath("data/db0/0/rp0/1_-259200000000000_345600000000000_1/columnstore/cpu_0000")
	if err != nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	assert.Equal(t, true, path.Contains(1))
	assert.Equal(t, "data/test", GetDatabasePath("test"))
	assert.Equal(t, "data/test/1/test", GetMeasurementPath(1, "test", "test"))
}

func TestFilePathCache(t *testing.T) {
	filePath := GetFilePath("xx")
	assert.Equal(t, "", filePath)
	SetFilePath("xx", "xx1")
	filePath = GetFilePath("xx")
	assert.Equal(t, "xx1", filePath)
	SetFilePath("data/repo/1/rp0/0_0_1000_0/columnstore/logstream", "xx")
	filePath = GetLogPath(0, 0, 1, time.UnixMicro(0), time.UnixMicro(1), "repo", "logstream", "rp0")
	assert.Equal(t, "xx", filePath)
}

func TestDecodeLogPath(t *testing.T) {
	p := DecodeLogPaths([]*internal.LogPath{&internal.LogPath{BasePath: t.Name()}})
	assert.Equal(t, 1, len(p))
	encodePath := EncodeLogPaths(p)
	assert.Equal(t, 1, len(encodePath))
}
