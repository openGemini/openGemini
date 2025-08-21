// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"os"
	"path"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
)

func TestParseIndexDir(t *testing.T) {
	_, _, err := parseIndexDir("1635724800000000000_1636329600000000000")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, err = parseIndexDir("xxx_1635724800000000000_1636329600000000000")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, err = parseIndexDir("1_xxx_1636329600000000000")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, err = parseIndexDir("1_1635724800000000000_xxx")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, err = parseIndexDir("1_1635724800000000000_1636329600000000000")
	require.NoError(t, err)
}

func TestParseShardDir(t *testing.T) {
	_, _, _, err := parseShardDir("1_1635724800000000000_1636329600000000000")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, _, err = parseShardDir("xxx_1635724800000000000_1636329600000000000_1")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, _, err = parseShardDir("1_xxx_1636329600000000000_1")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, _, err = parseShardDir("1_1635724800000000000_xxx_1")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, _, err = parseShardDir("1_1635724800000000000_1636329600000000000_xxx")
	require.EqualError(t, err, errno.NewError(errno.InvalidDataDir).Error())

	_, _, _, err = parseShardDir("1_1635724800000000000_1636329600000000000_1")
	require.NoError(t, err)
}

func TestOpenShard(t *testing.T) {
	dir := t.TempDir()
	dbPTInfo := NewDBPTInfo(defaultDb, defaultPtId, "", "", nil, nil, nil)
	lockPath := path.Join("", "LOCK")
	dbPTInfo.lockPath = &lockPath
	durationInfos := make(map[uint64]*meta.ShardDurationInfo)
	resC := make(chan *res, 1)
	openShardsLimit <- struct{}{}
	dbPTInfo.openShard(0, nil, "xxx", "0", durationInfos, resC, 0, nil)
	r := <-resC
	require.NoError(t, r.err)
	openShardsLimit <- struct{}{}
	dbPTInfo.openShard(0, nil, dir+"1_1635724800000000000_1636329600000000000_1", "1", durationInfos, resC, 0, nil)
	r = <-resC
	require.NoError(t, r.err)

	durationInfos[10] = &meta.ShardDurationInfo{DurationInfo: meta.DurationDescriptor{Duration: time.Second}}
	sh, err := dbPTInfo.loadProcess(0, nil, dir+"10_1635724800000000000_1636329600000000000_100", "1", 100, 10, durationInfos, nil, nil)
	require.Empty(t, sh)
	require.NoError(t, err)

	ltime := uint64(time.Now().Unix())
	indexIdent := &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 2,
		IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}

	opts := new(tsi.Options).
		Path("").
		Ident(indexIdent).
		IndexType(index.Text).
		StartTime(time.Now()).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)
	dbPTInfo.indexBuilder[100] = tsi.NewIndexBuilder(opts)
	dbPTInfo.indexBuilder[100].Relations = make([]*tsi.IndexRelation, 0)
	mst0 := &meta.MeasurementInfo{
		Name: "mst0_0000",
	}
	mst1 := &meta.MeasurementInfo{
		Name: "mst1_0000",
	}
	mstPath := dir + "10_1635724800000000000_1636329600000000000_100/columnstore/mst1_0000"
	if err := os.MkdirAll(mstPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(mstPath)
	client := &MockMetaClient{
		mstInfo: []*meta.MeasurementInfo{
			mst0,
			mst1,
		},
	}
	durationInfos[10] = &meta.ShardDurationInfo{DurationInfo: meta.DurationDescriptor{Duration: time.Second}, Ident: meta.ShardIdentifier{EngineType: uint32(config.COLUMNSTORE)}}
	sh, err = dbPTInfo.loadProcess(0, nil, dir+"10_1635724800000000000_1636329600000000000_100",
		"1", 100, 10, durationInfos, &meta.TimeRangeInfo{StartTime: time.Now(), EndTime: time.Now().Add(1 * time.Hour)}, client)
	require.NoError(t, err)
	config.SetProductType(config.LogKeeperService)
	defer config.SetProductType("")
	sh, err = dbPTInfo.loadProcess(0, nil, dir+"10_1635724800000000000_1636329600000000000_100",
		"1", 100, 10, durationInfos, &meta.TimeRangeInfo{StartTime: time.Now(), EndTime: time.Now().Add(1 * time.Hour)}, client)
	require.NoError(t, err)
}

func TestChangeSubPathToColdOne(t *testing.T) {
	dbPTInfo := NewDBPTInfo(defaultDb, defaultPtId, "", "", nil, nil, nil)
	dbPTInfo.dbObsOptions = &obs.ObsOptions{}
	//dbPTInfo.database =
	client := &MockMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			defaultDb: &meta.DatabaseInfo{
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp": &meta.RetentionPolicyInfo{
						IndexGroups: []meta.IndexGroupInfo{
							{
								Indexes: []meta.IndexInfo{
									{
										ID:   uint64(1),
										Tier: util.Cleared,
									},
									{
										ID:   uint64(2),
										Tier: util.Cleared,
									},
									{
										ID:   uint64(3),
										Tier: util.Cold,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	subPaths := []string{"1_1744588800000000000_1745193600000000000"}
	res := changeSubPathToColdOne(subPaths, dbPTInfo, client, "rp")
	require.Equal(t, 1, len(res))
	require.Equal(t, "3_1744588800000000000_1745193600000000000", res[0])

	subPaths = []string{"1_test_1745193600000000000"}
	res = changeSubPathToColdOne(subPaths, dbPTInfo, client, "rp")
	require.Equal(t, "1_test_1745193600000000000", res[0])

	subPaths = []string{"3_1744588800000000000_1745193600000000000"}
	res = changeSubPathToColdOne(subPaths, dbPTInfo, client, "rp")
	require.Equal(t, "3_1744588800000000000_1745193600000000000", res[0])

	subPaths = []string{"2_1744588800000000000_1745193600000000000"}
	res = changeSubPathToColdOne(subPaths, dbPTInfo, client, "rp")
	require.Equal(t, "2_1744588800000000000_1745193600000000000", res[0])

	dbPTInfo = NewDBPTInfo("test", defaultPtId, "", "", nil, nil, nil)
	res = changeSubPathToColdOne(subPaths, dbPTInfo, client, "rp")
	require.Equal(t, "2_1744588800000000000_1745193600000000000", res[0])
}

func TestHasCoverShard(t *testing.T) {
	dbpt := &DBPTInfo{
		shards: make(map[uint64]Shard),
	}
	dbpt.shards[1] = &shard{
		durationInfo: &meta.DurationDescriptor{
			MergeDuration: 1,
		},
		startTime: time.Unix(0, 1),
		endTime:   time.Unix(0, 2),
		ident: &meta.ShardIdentifier{
			Policy: "rp0",
		},
	}
	srcTimeRange := &meta.ShardTimeRangeInfo{
		TimeRange: meta.TimeRangeInfo{StartTime: time.Unix(0, 1), EndTime: time.Unix(0, 2)},
	}
	re := dbpt.HasCoverShard(srcTimeRange, "rp0", 1)
	require.Equal(t, re, true)
}
