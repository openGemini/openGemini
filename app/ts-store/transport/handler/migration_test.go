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

package handler

import (
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	netdata "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/require"
)

type MockNewResponser struct {
	err error
}

func NewMockNewResponser(err error) *MockNewResponser {
	return &MockNewResponser{err: err}
}

func (m MockNewResponser) Encode(bytes []byte, i interface{}) ([]byte, error) {
	panic("implement me")
}

func (m MockNewResponser) Decode(bytes []byte) (interface{}, error) {
	panic("implement me")
}

func (m MockNewResponser) Response(i interface{}, b bool) error {
	return m.err
}

func (m MockNewResponser) Callback(i interface{}) error {
	panic("implement me")
}

func (m MockNewResponser) Apply() error {
	panic("implement me")
}

func (m MockNewResponser) Type() uint8 {
	panic("implement me")
}

func (m MockNewResponser) Session() *spdy.MultiplexedSession {
	panic("implement me")
}

func (m MockNewResponser) Sequence() uint64 {
	panic("implement me")
}

func (m MockNewResponser) StartAnalyze(span *tracing.Span) {
	panic("implement me")
}

func (m MockNewResponser) FinishAnalyze() {
	panic("implement me")
}

func Test_MigrationProcessor_Move(t *testing.T) {
	h := &MigrationProcessor{
		store: NewMockStoreEngine(0),
		log:   logger.NewLogger(errno.ModuleHA),
	}
	resp := &MockNewResponser{}
	var req *msgservice.PtRequest
	mv := []meta.MoveState{meta.MovePreOffload, meta.MoveRollbackPreOffload, meta.MovePreAssign, meta.MoveOffload, meta.MoveAssign}
	for _, m := range mv {
		req = &msgservice.PtRequest{
			PtRequest: netdata.PtRequest{
				Pt: &proto2.DbPt{
					Db: proto.String("DB"),
					Pt: &proto2.PtInfo{
						Owner: &proto2.PtOwner{
							NodeID: proto.Uint64(4),
						},
						Status: proto.Uint32(0),
						PtId:   proto.Uint32(1),
					},
				},
				MigrateType: proto.Int32(int32(m)),
				OpId:        proto.Uint64(1234),
			},
		}
		err := h.Handle(resp, req)
		require.NoError(t, err)
	}
}

func Test_MigrationProcessor_Move_error1(t *testing.T) {
	h := &MigrationProcessor{
		store: NewMockStoreEngine(0),
		log:   logger.NewLogger(errno.ModuleHA),
	}
	resp := &MockNewResponser{}
	req := &msgservice.Requester{}

	err := h.Handle(resp, req)
	require.Errorf(t, err, "invalid data type, exp: *msgservice.PtRequest, got: *msgservice.Requester")
}

func Test_MigrationProcessor_Move_error2(t *testing.T) {
	h := &MigrationProcessor{
		store: NewMockStoreEngine(0),
		log:   logger.NewLogger(errno.ModuleHA),
	}
	resp := &MockNewResponser{}
	req := &msgservice.PtRequest{
		PtRequest: netdata.PtRequest{
			Pt: &proto2.DbPt{
				Db: proto.String(""),
				Pt: &proto2.PtInfo{
					Owner: &proto2.PtOwner{
						NodeID: proto.Uint64(4),
					},
					Status: proto.Uint32(0),
					PtId:   proto.Uint32(1),
				},
			},
			MigrateType: proto.Int32(int32(meta.MovePreAssign)),
			OpId:        proto.Uint64(1234),
		},
	}
	err := h.Handle(resp, req)
	require.EqualError(t, err, errno.NewError(errno.ErrMigrationRequestDB).Error())
}

func Test_MigrationProcessor_Move_error3(t *testing.T) {
	h := &MigrationProcessor{
		store: NewMockStoreEngine(0),
		log:   logger.NewLogger(errno.ModuleHA),
	}
	resp := &MockNewResponser{}
	req := &msgservice.PtRequest{
		PtRequest: netdata.PtRequest{
			Pt: &proto2.DbPt{
				Db: proto.String("0"),
				Pt: nil,
			},
			MigrateType: proto.Int32(int32(meta.MovePreAssign)),
			OpId:        proto.Uint64(1234),
		},
	}
	err := h.Handle(resp, req)
	require.EqualError(t, err, errno.NewError(errno.ErrMigrationRequestPt).Error())
}
