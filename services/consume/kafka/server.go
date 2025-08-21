// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"slices"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/services/consume/kafka/handle"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
	"go.uber.org/zap"
)

var maxRequestSize uint64

type Server struct {
	Listener net.Listener
}

var bufferPool = pool.NewDefaultUnionPool(func() *pool.Buffer {
	return &pool.Buffer{}
})

func getBuffer() (*pool.Buffer, func()) {
	buf := bufferPool.Get()
	return buf, func() {
		buf.B = buf.B[:0]
		buf.Swap = buf.Swap[:0]
		bufferPool.Put(buf)
	}
}

func (s *Server) Open(addr string, size uint64) error {
	maxRequestSize = size
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.Listener = listener
	logger.NewLogger(errno.ModuleConsume).Info("listen success", zap.String("addr", addr))

	go func() {
		defer util.MustClose(listener)
		for {
			conn, err := listener.Accept()
			if err != nil {
				logger.GetLogger().Error("accept failed", zap.Error(err))
				continue
			}
			go s.Process(conn)
		}
	}()
	return nil
}

func (s *Server) Process(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			logger.GetLogger().Error("failed to close connection", zap.Error(err),
				zap.String("local addr", conn.LocalAddr().String()),
				zap.String("remote addr", conn.RemoteAddr().String()))
		}
	}()
	err := s.process(conn)
	if err != nil {
		logger.GetLogger().Error("failed to handle connection", zap.Error(err),
			zap.String("local addr", conn.LocalAddr().String()),
			zap.String("remote addr", conn.RemoteAddr().String()))
	}
}

func (s *Server) process(conn net.Conn) error {
	hm := handle.NewHandlerManager()
	dec := &codec.BinaryDecoder{}
	reader := NewReader(conn)
	buffer, release := getBuffer()
	defer release()

	header := &protocol.RequestHeader{}

	for {
		body, err := reader.Read()
		if err != nil {
			return err
		}

		dec.Reset(body)
		err = header.Unmarshal(dec)
		if err != nil {
			return err
		}

		err = hm.Call(*header, dec.Remain(), func(msg protocol.Marshaler) error {
			buffer.B = protocol.MarshalSize(buffer.B[:0], msg)

			return s.writeTo(conn, buffer.B)
		})
		if err != nil {
			return err
		}
	}
}

func (s *Server) writeTo(conn net.Conn, buf []byte) error {
	n, err := conn.Write(buf)
	if err != nil {
		return err
	}

	if n < len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

func (s *Server) Close() error {
	if s.Listener != nil {
		return s.Listener.Close()
	}
	return nil
}

type Reader struct {
	r       io.Reader
	sizeBuf [4]byte
	buf     []byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) Read() ([]byte, error) {
	if _, err := io.ReadFull(r.r, r.sizeBuf[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(r.sizeBuf[:])
	if uint64(size) > maxRequestSize {
		return nil, fmt.Errorf("invalid request size. max: %d; current: %d", maxRequestSize, size)
	}

	r.buf = slices.Grow(r.buf[:0], int(size))[:size]
	if _, err := io.ReadFull(r.r, r.buf); err != nil {
		return nil, err
	}

	return r.buf, nil
}
