/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package raftlog

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/util"
)

type DataType uint32

const (
	Normal DataType = iota
	Snapshot
)

type DataWrapper struct {
	Data      []byte
	DataType  DataType
	Identity  string // raftNode identity
	ProposeId uint64 // propose seq id
}

func (d *DataWrapper) GetDataType() DataType {
	return d.DataType
}

func (d *DataWrapper) GetData() []byte {
	return d.Data
}

func (d *DataWrapper) Marshal() []byte {
	var dst []byte
	// 1.dataType
	dst = encoding.MarshalUint32(dst, uint32(d.DataType))

	// 2.identity
	dst = append(dst, uint8(len(d.Identity)))
	dst = append(dst, []byte(d.Identity)...)

	// 3.proposeId
	dst = encoding.MarshalUint64(dst, d.ProposeId)

	// 4.data
	dst = append(dst, d.Data...)

	return dst
}

func Unmarshal(dst []byte) (*DataWrapper, error) {
	// 1.dataType
	dataType := encoding.UnmarshalUint32(dst[:4])
	dst = dst[4:]

	// 2.identity
	l := int(dst[0])
	if len(dst) <= l {
		return nil, fmt.Errorf("dataWrapper no data for database")
	}
	dst = dst[1:]
	identity := util.Bytes2str(dst[:l])
	dst = dst[l:]

	// 3.proposeId
	proposeId := encoding.UnmarshalUint64(dst)

	return &DataWrapper{
		DataType:  DataType(dataType),
		Data:      dst[8:],
		Identity:  identity,
		ProposeId: proposeId,
	}, nil
}
