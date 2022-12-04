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
package castor

import (
	"bufio"
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
)

type ByteReadReader interface {
	io.Reader
	io.ByteReader
}

// write record into connection
func writeData(record array.Record, out io.WriteCloser) error {
	w := ipc.NewWriter(out, ipc.WithSchema(record.Schema()))
	err := w.Write(record)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return nil
}

// read record from connection
func readData(in ByteReadReader) ([]array.Record, error) {
	rdr, err := ipc.NewReader(in)
	if err != nil {
		if strings.HasSuffix(err.Error(), ": EOF") {
			return nil, io.EOF
		}
		if strings.Contains(err.Error(), "closed network") {
			return nil, io.ErrClosedPipe
		}
		return nil, err
	}
	var records []array.Record
	for rdr.Next() {
		out := rdr.Record()
		out.Retain()
		records = append(records, out)
	}
	if rdr.Err() != nil {
		return nil, rdr.Err()
	}
	return records, nil
}

type castorCli struct {
	dataSocketIn  io.WriteCloser // write into pyworker
	dataSocketOut ByteReadReader // read from pyworker

	alive     bool
	logger    *logger.Logger
	cnt       *int32 // reference of service client quantity
	mu        sync.Mutex
	chanSet   *chanSet
	writeChan chan *data
	cancel    context.CancelFunc
	ctx       context.Context
}

func newClient(addr string, logger *logger.Logger, chanSet *chanSet, cnt *int32) (*castorCli, *errno.Error) {
	conn, err := getConn(addr)
	if err != nil {
		return nil, errno.NewError(errno.FailToConnectToPyworker)
	}
	readerBuf := bufio.NewReader(conn)
	ctx, cancel := context.WithCancel(context.Background())
	cli := &castorCli{
		dataSocketIn:  conn,
		dataSocketOut: readerBuf,
		alive:         true,
		logger:        logger,
		cnt:           cnt,
		chanSet:       chanSet,
		writeChan:     make(chan *data, 1),
		cancel:        cancel,
		ctx:           ctx,
	}
	atomic.AddInt32(cli.cnt, 1)
	go cli.read()
	go cli.write()
	return cli, nil
}

// write send data to through internal connection
func (h *castorCli) write() {
	for {
		select {
		case data, ok := <-h.writeChan:
			if !ok {
				return
			}
			if err := writeData(data.record, h.dataSocketIn); err != nil {
				if data.retryCnt >= maxSendRetry {
					h.chanSet.dataFailureChan <- data
				} else {
					data.retryCnt++
					h.chanSet.dataRetryChan <- data
				}
				h.close()
				return
			}
			h.chanSet.clientPool <- h
		case <-h.ctx.Done():
			return
		}
	}
}

// read receive data to from internal connection
func (h *castorCli) read() {
	for {
		records, err := readData(h.dataSocketOut)
		if err != nil {
			if err == io.EOF || err == io.ErrClosedPipe {
				h.logger.Info("connection closed")
			} else {
				h.logger.Error(err.Error())
			}
			h.close()
			return
		}
		for _, record := range records {
			if err := checkRecordType(record); err != nil {
				h.logger.Error(err.Error())
				continue
			}
			h.chanSet.resultChan <- record
		}
	}
}

// close mark itself as not alive and close connection
func (h *castorCli) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.alive {
		return
	}
	h.alive = false
	h.cancel()
	close(h.writeChan)
	if err := h.dataSocketIn.Close(); err != nil {
		h.logger.Error(err.Error())
	}
	atomic.AddInt32(h.cnt, -1)
	h.logger.Info("close castorCli")
}

func checkRecordType(rec array.Record) *errno.Error {
	msgType, err := GetMetaValueFromRecord(rec, string(MessageType))
	if err != nil {
		return errno.NewError(errno.UnknownDataMessage)
	}
	switch msgType {
	case string(DATA):
		return nil
	default:
		return errno.NewError(errno.UnknownDataMessageType, DATA, msgType)
	}
}
