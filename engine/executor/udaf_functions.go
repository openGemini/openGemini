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

package executor

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/influxdata/influxdb/uuid"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/services/castor"
)

type recordToChunkFunc func(r arrow.Record, c Chunk, fields map[string]struct{}) error

func CastorReduce(fn recordToChunkFunc) WideReduce {
	return func(in []Chunk, out Chunk, args ...interface{}) error {
		if len(in) == 0 {
			return errno.NewError(errno.EmptyData)
		}
		srv := castor.GetService()
		if srv == nil {
			return errno.NewError(errno.ServiceNotEnable)
		}
		if !srv.IsAlive() {
			return errno.NewError(errno.ServiceNotAlive)
		}

		inputs, ok := args[0].([]influxql.Expr)
		if !ok {
			return errno.NewError(errno.TypeAssertFail, influxql.AnyField)
		}
		taskId := uuid.TimeUUID().String()
		recs, err := ChunkToArrowRecords(in, taskId, inputs)
		if err != nil {
			return err
		}

		// send data
		respChan, err := castor.NewRespChan(len(recs))
		if err != nil {
			return err
		}
		respCnt := 0
		srv.RegisterResultChan(taskId, respChan)
		defer func() {
			for _, r := range recs {
				r.Release()
			}
			srv.DeregisterResultChan(taskId)
			respChan.Close()
		}()
		for _, r := range recs {
			srv.HandleData(r)
		}

		// wait response
		out.SetName(in[0].Name())
		ticker := time.NewTicker(srv.Config.GetWaitTimeout())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				return errno.NewError(errno.ResponseIncomplete, len(recs), respCnt)
			case resp := <-respChan.C:
				respCnt++
				defer resp.Release()
				if err := fn(resp, out, castor.DesiredFieldKeySet); err != nil {
					logger.GetLogger().Error(err.Error())
					return err
				}
				if respCnt == len(recs) {
					return nil
				}
			case err := <-respChan.ErrCh:
				return err
			}
		}
	}
}
