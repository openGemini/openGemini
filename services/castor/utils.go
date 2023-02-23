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
	"net"
	"sync"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/lib/errno"
)

// check if key is internal
func IsInternalKey(key string) bool {
	_, exist := internalKeySet[key]
	return exist
}

// return value from record's metadata according to key
func GetMetaValueFromRecord(data array.Record, key string) (string, *errno.Error) {
	md := data.Schema().Metadata()
	idx := md.FindKey(key)
	if idx == -1 {
		return "", errno.NewError(errno.MessageNotFound, key)
	}
	return md.Values()[idx], nil
}

func getConn(addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// return response channel with buffer size
func NewRespChan(size int) (*respChan, *errno.Error) {
	if size < 0 {
		return nil, errno.NewError(errno.ShortBufferSize, 0, size)
	} else if size > maxRespBufSize {
		return nil, errno.NewError(errno.DataTooMuch, maxRespBufSize, size)
	}
	return &respChan{
		C:     make(chan array.Record, size),
		ErrCh: make(chan *errno.Error, size),
		alive: true,
	}, nil
}

type respChan struct {
	mu    sync.Mutex
	C     chan array.Record
	ErrCh chan *errno.Error
	alive bool
}

// Close will stop receiving and release all data in channel
func (r *respChan) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.alive {
		return
	}
	r.alive = false
	for len(r.C) > 0 {
		resp := <-r.C
		resp.Release()
	}
	close(r.C)
	close(r.ErrCh)
}

func newData(record array.Record) *data {
	return &data{record: record}
}

type data struct {
	record   array.Record
	retryCnt int
	err      *errno.Error
}

func (d *data) release() {
	d.record.Release()
}

func (d *data) size() int {
	return int(d.record.NumRows())
}

type dataChanSet struct {
	dataChan   chan *data
	resultChan chan array.Record
}
