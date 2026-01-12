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

package transport

import (
	"fmt"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/stream"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/pointsdecoder"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	servicesstream "github.com/openGemini/openGemini/services/stream"
	"go.uber.org/zap"
)

var writeHandler []func(ww *pointsdecoder.DecoderWork, log *logger.Logger, store *storage.Storage) error

func init() {
	writeHandler = make([]func(ww *pointsdecoder.DecoderWork, log *logger.Logger, store *storage.Storage) error, config.PolicyEnd)
	writeHandler[config.WriteAvailableFirst] = WritePointsForSSOrWAF
	writeHandler[config.SharedStorage] = WritePointsForSSOrWAF
	writeHandler[config.Replication] = WritePointsForRep
}

// Server processes connections from insert and select.
type Server struct {
	closed chan struct{}

	log *logger.Logger

	selectServer *SelectServer
	insertServer *InsertServer
}

// NewServer returns new Server.
func NewServer(ingestAddr string, selectAddr string) *Server {
	selectServer := NewSelectServer(selectAddr)
	insertServer := NewInsertServer(ingestAddr)

	return &Server{
		closed: make(chan struct{}),
		log:    logger.NewLogger(errno.ModuleStorageEngine),

		selectServer: selectServer,
		insertServer: insertServer,
	}
}

func (s *Server) Open() error {
	if err := s.insertServer.Open(); err != nil {
		return fmt.Errorf("cannot create a server with addr=%s: %s", s.insertServer.addr, err)
	}
	if err := s.selectServer.Open(); err != nil {
		return fmt.Errorf("cannot create a server with addr=%s: %s", s.selectServer.addr, err)
	}
	return nil
}

func (s *Server) Run(store *storage.Storage, stream stream.Engine) {
	go s.insertServer.Run(store, stream)
	//TODO stream support query
	go s.selectServer.Run(store)
	if stream != nil {
		go stream.Run()
	}
}

func (s *Server) setIsStopping() {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
}

func WritePointsForSSOrWAF(ww *pointsdecoder.DecoderWork, log *logger.Logger, store *storage.Storage) error {
	db, rp, ptId, shard, _, binaryRows, err := ww.DecodePoints()
	if err != nil {
		err = errno.NewError(errno.ErrUnmarshalPoints, err)
		log.Error("unmarshal rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
		return err
	}

	if err = storage.WriteRows(store, db, rp, ptId, shard, ww.GetRows(), binaryRows); err != nil {
		log.Error("write rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
	}
	return err
}

func WritePointsForRep(ww *pointsdecoder.DecoderWork, log *logger.Logger, store *storage.Storage) error {
	db, rp, ptId, tail, err := ww.DecodeDBPT() // the tail contains data: [shard_id, streamShardIdList, rows...]
	if err != nil {
		err = errno.NewError(errno.ErrUnmarshalPoints, err)
		log.Error("unmarshal rows failed", zap.String("db", db), zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Error(err))
		return err
	}

	if err = storage.WriteRowsForRep(store, db, rp, ptId, 0, ww.GetRows(), tail); err != nil {
		if errno.Equal(err, errno.RepConfigWriteNoRepDB) {
			shard, _, newTail, err1 := ww.DecodeShardAndRows(db, rp, ptId, tail)
			if err1 != nil {
				err = errno.NewError(errno.ErrUnmarshalPoints, err1)
				log.Error("unmarshal rows failed", zap.String("db", db), zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Error(err))
				return err
			}
			return storage.WriteRows(store, db, rp, ptId, shard, ww.GetRows(), newTail)
		}
		log.Error("WritePointsForRep write rows failed", zap.String("db", db), zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Error(err))
	}
	return err
}

func (s *Server) MustClose() {
	// Mark the server as stopping.
	s.setIsStopping()
	s.selectServer.Close()
	s.insertServer.Close()

}

type Storage interface {
	WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error
	GetMetaClient() metaclient.MetaClient
}

func WriteStreamPoints(ww *pointsdecoder.DecoderWork, log *logger.Logger, store Storage, streamEngine stream.Engine) (error, bool) {
	var inUse bool
	db, rp, ptId, shard, streamShardIdList, binaryRows, err := ww.DecodePoints()
	if err != nil {
		err = errno.NewError(errno.ErrUnmarshalPoints, err)
		log.Error("unmarshal rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
		return err, false
	}

	if streamEngine != nil && len(streamShardIdList) > 0 && servicesstream.IsWriteStreamPointsEnabled() {
		streamIdDstShardIdMap := make(map[uint64]uint64)
		if len(streamShardIdList)%2 != 0 {
			err = errno.NewError(errno.ErrUnmarshalPoints, err)
			return err, false
		}
		for i := 0; i < len(streamShardIdList); i += 2 {
			streamIdDstShardIdMap[streamShardIdList[i]] = streamShardIdList[i+1]
		}

		streamRows := make([]influx.Row, 0)
		inUse, err = streamEngine.WriteRows(&stream.WriteStreamRowsCtx{
			DB:                    db,
			RP:                    rp,
			PtId:                  ptId,
			ShardId:               shard,
			StreamIdDstShardIdMap: streamIdDstShardIdMap,
			WW:                    ww,
			StreamRows:            &streamRows})
		if err != nil {
			return err, inUse
		}
		if len(streamRows) > 0 {
			ww.SetRows(append(ww.GetRows(), streamRows...))
			binaryRows = binaryRows[:0]
			binaryRows, err = influx.FastMarshalMultiRows(binaryRows, ww.GetRows())
			if err != nil {
				return err, inUse
			}
		}
	}

	rows := filterStreamRows(ww)
	if len(rows) == 0 {
		return err, inUse
	}
	ww.SetRows(rows)
	if err = store.WriteRows(db, rp, ptId, shard, ww.GetRows(), binaryRows); err != nil {
		log.Error("write rows failed", zap.String("db", db),
			zap.String("rp", rp), zap.Uint32("ptId", ptId), zap.Uint64("shardId", shard), zap.Error(err))
	}

	return err, inUse
}

func filterStreamRows(ww *pointsdecoder.DecoderWork) []influx.Row {
	var needFilter bool
	originRows := ww.GetRows()
	for _, row := range originRows {
		if row.StreamOnly {
			needFilter = true
		}
	}
	if !needFilter {
		return originRows
	}
	rows := make([]influx.Row, 0)
	for _, row := range originRows {
		if !row.StreamOnly {
			rows = append(rows, row)
		}
	}
	return rows
}
