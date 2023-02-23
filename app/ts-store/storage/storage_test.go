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

package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	retention2 "github.com/influxdata/influxdb/services/retention"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/stretchr/testify/require"
)

func TestAppendDownSamplePolicyService(t *testing.T) {
	s := Storage{}
	c := retention2.NewConfig()
	c.Enabled = false
	s.appendDownSamplePolicyService(c)
	c.Enabled = true
	s.appendDownSamplePolicyService(c)
	if len(s.Services) <= 0 {
		t.Errorf("services append fail")
	}
}

func TestReportLoad(t *testing.T) {
	st := &Storage{
		log:  logger.NewLogger(errno.ModuleStorageEngine),
		stop: make(chan struct{}),
		loadCtx: &metaclient.LoadCtx{
			LoadCh: make(chan *metaclient.DBPTCtx),
		},
		metaClient: metaclient.NewClient(t.TempDir(), false, 100),
	}
	require.NoError(t, st.metaClient.Close())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		st.ReportLoad()
		wg.Done()
	}()

	var ctx = make([]*metaclient.DBPTCtx, 5)
	for i := 0; i < 5; i++ {
		ctx[i] = &metaclient.DBPTCtx{
			DBPTStat: &proto2.DBPtStatus{
				DB:   proto.String("db0"),
				PtID: proto.Uint32(100),
				RpStats: []*proto2.RpShardStatus{{
					RpName: proto.String("default"),
					ShardStats: &proto2.ShardStatus{
						ShardID:     proto.Uint64(101),
						ShardSize:   proto.Uint64(102),
						SeriesCount: proto.Int32(103),
						MaxTime:     proto.Int64(104),
					},
				}},
			},
		}
	}

	for i := 0; i < 5; i++ {
		st.loadCtx.LoadCh <- ctx[i]
	}

	time.Sleep(time.Second / 10)
	close(st.stop)
	wg.Wait()
}
