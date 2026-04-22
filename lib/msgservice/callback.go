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

package msgservice

import (
	"errors"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/spdy/transport"
)

type DDLCallback struct {
	data interface{}
}

func (c *DDLCallback) Handle(data interface{}) error {
	msg, ok := data.(*DDLMessage)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.DDLMessage", data)
	}

	c.data = msg.Data
	return nil
}

func (c *DDLCallback) GetCodec() transport.Codec {
	return &DDLMessage{}
}

func (c *DDLCallback) GetResponse() interface{} {
	return c.data
}

type SysCtrlCallback struct {
	data interface{}
}

func (c *SysCtrlCallback) Handle(data interface{}) error {
	msg, ok := data.(*SysCtrlResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.SysCtrlResponse", data)
	}

	c.data = msg
	return nil
}

func (c *SysCtrlCallback) GetCodec() transport.Codec {
	return &SysCtrlResponse{}
}

func (c *SysCtrlCallback) GetResponse() interface{} {
	return c.data
}

type WritePointsCallback struct {
	data *WritePointsResponse
}

func (c *WritePointsCallback) Handle(data interface{}) error {
	msg, ok := data.(*WritePointsResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.WritePointsResponse", data)
	}

	c.data = msg
	if c.data.Code == 0 {
		return nil
	} else if c.data.ErrCode != 0 {
		err := errno.NewError(c.data.ErrCode)
		err.SetMessage(c.data.Message)
		return err
	}
	return errors.New(c.data.Message)
}

func (c *WritePointsCallback) GetCodec() transport.Codec {
	return &WritePointsResponse{}
}

type WriteBlobsCallback struct {
	data *WriteBlobsResponse
}

func (c *WriteBlobsCallback) Handle(data interface{}) error {
	msg, ok := data.(*WriteBlobsResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.WriteBlobsResponse", data)
	}

	c.data = msg
	if c.data.Code == 0 {
		return nil
	} else if c.data.ErrCode != 0 {
		err := errno.NewError(c.data.ErrCode)
		err.SetMessage(c.data.Message)
		return err
	}
	return errors.New(c.data.Message)
}

func (c *WriteBlobsCallback) GetCodec() transport.Codec {
	return &WriteBlobsResponse{}
}

type WriteStreamPointsCallback struct {
	data *WriteStreamPointsResponse
}

func (c *WriteStreamPointsCallback) Handle(data interface{}) error {
	msg, ok := data.(*WriteStreamPointsResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.WriteStreamPointsResponse", data)
	}

	c.data = msg
	if c.data.Code == 0 {
		return nil
	} else if c.data.ErrCode != 0 {
		return errno.NewError(c.data.ErrCode, c.data.Message)
	}
	return errors.New(c.data.Message)
}

func (c *WriteStreamPointsCallback) GetCodec() transport.Codec {
	return &WriteStreamPointsResponse{}
}

type MigratePtCallback struct {
	data interface{}
	fn   func(err error)
}

func (c *MigratePtCallback) SetCallbackFn(fn func(err error)) {
	c.fn = fn
}

func (c *MigratePtCallback) Handle(data interface{}) error {
	msg, ok := data.(*PtResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.PtMessage", data)
	}

	c.data = msg
	c.fn(msg.Error())
	return nil
}

func (c *MigratePtCallback) GetCodec() transport.Codec {
	return &PtResponse{}
}

func (c *MigratePtCallback) GetResponse() interface{} {
	return c.data
}

func (c *MigratePtCallback) CallFn(err error) {
	c.fn(err)
}

type RaftMsgCallback struct {
	data interface{}
}

func (c *RaftMsgCallback) Handle(data interface{}) error {
	msg, ok := data.(*RaftMsgMessage)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.RaftMsgMessage", data)
	}

	c.data = msg.Data
	return nil
}

func (c *RaftMsgCallback) GetCodec() transport.Codec {
	return &RaftMsgMessage{}
}

func (c *RaftMsgCallback) GetResponse() interface{} {
	return c.data
}
