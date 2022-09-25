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
	return nil
}

func (c *WritePointsCallback) GetCodec() transport.Codec {
	return &WritePointsResponse{}
}

func (c *WritePointsCallback) Error() error {
	if c.data.Code == 0 {
		return nil
	}

	return errors.New(c.data.Message)
}
