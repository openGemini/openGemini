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
)

func TestObsOptionsClone(t *testing.T) {
	srcOpts := &ObsOptions{
		Enabled:    true,
		BucketName: "test_bucket_name",
		Endpoint:   "test_endpoint",
		Ak:         "test_ak",
		Sk:         "test_sk",
		BasePath:   "test_base_path",
	}
	cloneOpts := srcOpts.Clone()
	if cloneOpts.Enabled != srcOpts.Enabled || cloneOpts.BucketName != srcOpts.BucketName {
		t.Fatal("ObsOptions clone failed")
	}
	if !cloneOpts.Validate() {
		t.Fatal("ObsOptions clone failed")
	}

	srcOpts = nil
	cloneOpts = srcOpts.Clone()
	if cloneOpts != nil {
		t.Fatal("ObsOptions clone failed")
	}
}

func TestParseLogPath(t *testing.T) {
	startTime := time.Now().Truncate(24 * time.Hour)
	endTime := startTime.Add(24 * time.Hour)
	segPath := GetShardPath(0, 0, 1, startTime, endTime, "repo", "logstream")
	logInfo, err := ParseLogPath(segPath)
	if err != nil {
		t.Fatalf("ParseLogPath error: %+v", err)
	}
	if logInfo.RepoName != "repo" {
		t.Fatalf("ParseLogPath repo failed, expect: repo, real: %s", logInfo.RepoName)
	}
	if logInfo.StartTime != startTime.UnixNano() {
		t.Fatalf("ParseLogPath repo failed, expect: %d, real: %d", startTime.UnixNano(), logInfo.StartTime)
	}
	if logInfo.EndTime != endTime.UnixNano() {
		t.Fatalf("ParseLogPath repo failed, expect: %d, real: %d", endTime.UnixNano(), logInfo.EndTime)
	}

	_, err = ParseLogPath("data/test/test1/7_1695859200000000000_1695945600000000000_7")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/test/abc/test1/7_1695859200000000000_1695945600000000000_7/columnstore")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/test/0/test1/x_1695859200000000000_1695945600000000000_7/columnstore")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/test/0/test1/7_x695859200000000000_1695945600000000000_7/columnstore")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
	_, err = ParseLogPath("data/test/0/test1/a_1695859200000000000_x695945600000000000_7/columnstore")
	if err == nil {
		t.Fatal("Expect ParseLogPath failed")
	}
}

func TestGetShardID(t *testing.T) {
	shardId := GetShardID("data/test/8/mst/9_1756944000000000000_1757030400000000000_9/columnstore/mst_0000")
	if shardId != 9 {
		t.Errorf("get wrong shardID")
	}
	shardId = GetShardID("data/test/8/mst/x_1756944000000000000_1757030400000000000_9/columnstore/mst_0000")
	if shardId != 0 {
		t.Errorf("get wrong shardID")
	}
}
