/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package engine

import (
	"path"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/influx/meta"
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
	dbPTInfo := NewDBPTInfo(defaultDb, defaultPtId, "", "", nil)
	lockPath := path.Join("", "LOCK")
	dbPTInfo.lockPath = &lockPath
	durationInfos := make(map[uint64]*meta.ShardDurationInfo)
	resC := make(chan *res, 1)
	openShardsLimit <- struct{}{}
	dbPTInfo.openShard(0, "xxx", "0", durationInfos, resC, 0, nil)
	r := <-resC
	require.NoError(t, r.err)
	openShardsLimit <- struct{}{}
	dbPTInfo.openShard(0, "1_1635724800000000000_1636329600000000000_1", "1", durationInfos, resC, 0, nil)
	r = <-resC
	require.NoError(t, r.err)
}
