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

package transport

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Server processes connections from insert and select.
type Server struct {
	closed chan struct{}

	log *logger.Logger

	selectServer *SelectServer
	insertServer *InsertServer
}

// NewServer returns new Server.
func NewServer(ingestAddr string, selectAddr string, storage *storage.Storage) (*Server, error) {
	selectServer := NewSelectServer(selectAddr, storage)
	insertServer := NewInsertServer(ingestAddr, storage)

	s := &Server{
		closed: make(chan struct{}),
		log:    logger.NewLogger(errno.ModuleStorageEngine),

		selectServer: selectServer,
		insertServer: insertServer,
	}

	return s, nil
}

func (s *Server) setIsStopping() {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
}

func getWritePointsWork() *writePointsWork {
	v := writePointsWorkPool.Get()
	if v == nil {
		return &writePointsWork{
			logger: logger.NewLogger(errno.ModuleWrite),
		}
	}
	return v.(*writePointsWork)
}

func putWritePointsWork(ww *writePointsWork) {
	bufferpool.PutPoints(ww.reqBuf)
	ww.reset()
	writePointsWorkPool.Put(ww)
}

var writePointsWorkPool sync.Pool

type writePointsWork struct {
	storage          *storage.Storage
	reqBuf           []byte
	rows             []influx.Row
	tagpools         []influx.Tag
	fieldpools       []influx.Field
	indexKeypools    []byte
	indexOptionpools []influx.IndexOption
	lastResetTime    uint64

	logger *logger.Logger
}

func (ww *writePointsWork) reset() {
	if (len(ww.reqBuf)*4 > cap(ww.reqBuf) || len(ww.rows)*4 > cap(ww.rows)) && fasttime.UnixTimestamp()-ww.lastResetTime > 10 {
		ww.reqBuf = nil
		ww.rows = nil
		ww.lastResetTime = fasttime.UnixTimestamp()
	}

	ww.tagpools = ww.tagpools[:0]
	ww.fieldpools = ww.fieldpools[:0]
	ww.indexKeypools = ww.indexKeypools[:0]
	ww.indexOptionpools = ww.indexOptionpools[:0]
	ww.storage = nil
	ww.rows = ww.rows[:0]
	ww.reqBuf = ww.reqBuf[:0]
}

func (ww *writePointsWork) decodePoints() (db string, rp string, ptId uint32, shard uint64, binaryRows []byte, err error) {
	tail := ww.reqBuf

	start := time.Now()

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
	db = record.Bytes2str(tail[:l])
	tail = tail[l:]

	l = int(tail[0])
	if len(tail) < l {
		err = errors.New("no data for rp name")
		ww.logger.Error(err.Error())
		return
	}
	tail = tail[1:]
	rp = record.Bytes2str(tail[:l])

	tail = tail[l:]

	if len(tail) < 16 {
		err = errors.New("no data for points data")
		ww.logger.Error(err.Error())
		return
	}
	ptId = encoding.UnmarshalUint32(tail)
	tail = tail[4:]

	shard = encoding.UnmarshalUint64(tail)
	tail = tail[8:]

	binaryRows = tail

	ww.rows = ww.rows[:0]
	ww.tagpools = ww.tagpools[:0]
	ww.fieldpools = ww.fieldpools[:0]
	ww.indexKeypools = ww.indexKeypools[:0]
	ww.indexOptionpools = ww.indexOptionpools[:0]
	ww.rows, ww.tagpools, ww.fieldpools, ww.indexOptionpools, ww.indexKeypools, err =
		influx.FastUnmarshalMultiRows(tail, ww.rows, ww.tagpools, ww.fieldpools, ww.indexOptionpools, ww.indexKeypools)
	if err != nil {
		ww.logger.Error("unmarshal rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
		return
	}
	atomic.AddInt64(&statistics.PerfStat.WriteUnmarshalNs, time.Since(start).Nanoseconds())
	return
}

func (ww *writePointsWork) WritePoints() error {
	db, rp, ptId, shard, binaryRows, err := ww.decodePoints()
	if err != nil {
		err = errno.NewError(errno.ErrUnmarshalPoints, err)
		ww.logger.Error("unmarshal rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
		return err
	}
	if err = ww.storage.WriteRows(db, rp, ptId, shard, ww.rows, binaryRows); err != nil {
		ww.logger.Error("write rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
	}
	return err
}

func (s *Server) MustClose() {
	// Mark the server as stoping.
	s.setIsStopping()
	s.selectServer.Close()
	s.insertServer.Close()

}
