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

package netstorage

import (
	"errors"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
)

type DDLCallback struct {
	data interface{}
}

func (c *DDLCallback) Handle(data interface{}) error {
	msg, ok := data.(*DDLMessage)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.DDLMessage", data)
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
		return executor.NewInvalidTypeError("*netstorage.SysCtrlResponse", data)
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
		return executor.NewInvalidTypeError("*netstorage.WritePointsResponse", data)
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

type WriteStreamPointsCallback struct {
	data *WriteStreamPointsResponse
}

func (c *WriteStreamPointsCallback) Handle(data interface{}) error {
	msg, ok := data.(*WriteStreamPointsResponse)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.WriteStreamPointsResponse", data)
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
		return executor.NewInvalidTypeError("*netstorage.PtMessage", data)
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
