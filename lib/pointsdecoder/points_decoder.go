// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package pointsdecoder

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

func GetDecoderWork() *DecoderWork {
	v := decoderWorkPool.Get()
	if v == nil {
		return &DecoderWork{
			logger: logger.NewLogger(errno.ModuleWrite),
		}
	}
	return v.(*DecoderWork)
}

func PutDecoderWork(ww *DecoderWork) {
	bufferpool.PutPoints(ww.reqBuf)
	ww.reset()
	decoderWorkPool.Put(ww)
}

var decoderWorkPool sync.Pool

type DecoderWork struct {
	reqBuf           []byte
	streamVars       []*msgservice.StreamVar
	rows             []influx.Row
	tagpools         []influx.Tag
	fieldpools       []influx.Field
	indexKeypools    []byte
	indexOptionpools []influx.IndexOption
	lastResetTime    uint64
	refCount         int64

	logger *logger.Logger
}

func (ww *DecoderWork) Ref() {
	atomic.AddInt64(&ww.refCount, 1)
}
func (ww *DecoderWork) UnRef() int64 {
	return atomic.AddInt64(&ww.refCount, -1)
}

func (ww *DecoderWork) SetReqBuf(buff []byte) {
	ww.reqBuf = buff
}

func (ww *DecoderWork) SetStreamVars(vars []*msgservice.StreamVar) {
	ww.streamVars = vars
}

func (ww *DecoderWork) SetRows(rows []influx.Row) {
	ww.rows = rows
}

func (ww *DecoderWork) GetRows() []influx.Row {
	return ww.rows
}

func (ww *DecoderWork) GetLogger() *logger.Logger {
	return ww.logger
}

func (ww *DecoderWork) PutWritePointsWork() {
	PutDecoderWork(ww)
}

func (ww *DecoderWork) reset() {
	if (len(ww.reqBuf)*4 > cap(ww.reqBuf) || len(ww.rows)*4 > cap(ww.rows)) && fasttime.UnixTimestamp()-ww.lastResetTime > 10 {
		ww.reqBuf = nil
		ww.rows = nil
		ww.lastResetTime = fasttime.UnixTimestamp()
	}

	ww.tagpools = ww.tagpools[:0]
	ww.fieldpools = ww.fieldpools[:0]
	ww.indexKeypools = ww.indexKeypools[:0]
	ww.indexOptionpools = ww.indexOptionpools[:0]
	ww.rows = ww.rows[:0]
	ww.reqBuf = ww.reqBuf[:0]
	ww.streamVars = ww.streamVars[:0]
	ww.refCount = 0
}

// DecodePoints decodes the network data reqBuf to Points.
// DecoderWork is a reuse object.
func (ww *DecoderWork) DecodePoints() (db string, rp string, ptId uint32, shard uint64, streamShardIdList []uint64, binaryRows []byte, err error) {
	var tail []byte
	db, rp, ptId, tail, err = ww.DecodeDBPT()
	if err != nil {
		ww.logger.Error(err.Error())
		return
	}
	shard, streamShardIdList, binaryRows, err = ww.DecodeShardAndRows(db, rp, ptId, tail)
	return
}

// DecodeDBPT decodes the data for [db, rp, ptId]
func (ww *DecoderWork) DecodeDBPT() (db string, rp string, ptId uint32, tail []byte, err error) {
	start := time.Now()
	tail = ww.reqBuf

	if len(tail) < 2 {
		err = errors.New("invalid points buffer")
		ww.logger.Error(err.Error())
		return
	}
	ty := tail[0]
	if ty != netstorage.PackageTypeFast {
		err = errors.New("not a fast marshal points package")
		ww.logger.Error(err.Error())
		return
	}
	tail = tail[1:]

	l := int(tail[0])
	if len(tail) < l {
		err = errors.New("no data for db name")
		ww.logger.Error(err.Error())
		return
	}
	tail = tail[1:]
	db = util.Bytes2str(tail[:l])
	tail = tail[l:]

	l = int(tail[0])
	if len(tail) < l {
		err = errors.New("no data for rp name")
		ww.logger.Error(err.Error())
		return
	}
	tail = tail[1:]
	rp = util.Bytes2str(tail[:l])

	tail = tail[l:]

	if len(tail) < 16 {
		err = errors.New("no data for points data")
		ww.logger.Error(err.Error())
		return
	}
	ptId = encoding.UnmarshalUint32(tail)
	tail = tail[4:]
	atomic.AddInt64(&statistics.PerfStat.WriteUnmarshalNs, time.Since(start).Nanoseconds())
	return
}

// DecodeShardAndRows  decodes the tail from [shardId] to [...rows]
func (ww *DecoderWork) DecodeShardAndRows(db string, rp string, ptId uint32, tail []byte) (shard uint64, streamShardIdList []uint64, binaryRows []byte, err error) {
	start := time.Now()

	shard = encoding.UnmarshalUint64(tail)
	tail = tail[8:]

	sdLen := encoding.UnmarshalUint32(tail)
	tail = tail[4:]

	streamShardIdList = make([]uint64, sdLen)
	tail, err = encoding.UnmarshalVarUint64s(streamShardIdList, tail)
	if err != nil {
		ww.logger.Error(err.Error())
		return
	}

	binaryRows = tail

	ww.rows = ww.rows[:0]
	ww.tagpools = ww.tagpools[:0]
	ww.fieldpools = ww.fieldpools[:0]
	ww.indexKeypools = ww.indexKeypools[:0]
	for i := range ww.indexOptionpools {
		ww.indexOptionpools[i].IndexList = ww.indexOptionpools[i].IndexList[:0]
	}
	ww.indexOptionpools = ww.indexOptionpools[:0]
	ww.rows, ww.tagpools, ww.fieldpools, ww.indexOptionpools, ww.indexKeypools, err =
		influx.FastUnmarshalMultiRows(tail, ww.rows, ww.tagpools, ww.fieldpools, ww.indexOptionpools, ww.indexKeypools)
	if err != nil {
		ww.logger.Error("unmarshal rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
		return
	}

	if len(streamShardIdList) > 0 {
		// set stream vars into the rows
		if len(ww.rows) != len(ww.streamVars) {
			errStr := "unmarshal rows failed, the num of the rows is not equal to the stream vars"
			ww.logger.Error(errStr, zap.String("db", db),
				zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
			err = errors.New(errStr)
			return
		}
		for i := 0; i < len(ww.rows); i++ {
			ww.rows[i].StreamOnly = ww.streamVars[i].Only
			ww.rows[i].StreamId = ww.streamVars[i].Id
		}
	}

	atomic.AddInt64(&statistics.PerfStat.WriteUnmarshalNs, time.Since(start).Nanoseconds())
	return
}
