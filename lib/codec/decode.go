// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package codec

import (
	"encoding/binary"
	"errors"

	"github.com/openGemini/openGemini/lib/util"
)

func DecodeUint32SliceBigEndian(dec *BinaryDecoder, key string, dst []uint32) ([]uint32, error) {
	n, err := DecodeUint32(dec, key)
	if err != nil {
		return dst, nil
	}

	size := int(n) * util.Uint32SizeBytes
	if err = dec.CheckSize(key, size); err != nil {
		return dst, err
	}

	buf := dec.BytesNoCopyN(size)
	for range n {
		dst = append(dst, binary.BigEndian.Uint32(buf))
		buf = buf[util.Uint32SizeBytes:]
	}

	return dst, nil
}

// DecodeSmallStringSlice Each character string cannot exceed 64 KB
func DecodeSmallStringSlice(dec *BinaryDecoder, key string, dst []string) ([]string, error) {
	n, err := DecodeUint32(dec, key)
	if err != nil {
		return dst, nil
	}

	for range n {
		if err = dec.CheckSize(key, util.Uint16SizeBytes); err != nil {
			return dst, err
		}
		size := dec.Uint16()
		if err = dec.CheckSize(key, int(size)); err != nil {
			return dst, err
		}
		dst = append(dst, util.Bytes2str(dec.BytesN(nil, int(size))))
	}
	return dst, nil
}

func DecodeString(dec *BinaryDecoder, key string) (string, error) {
	size, err := DecodeUint16(dec, key)
	if err != nil {
		return "", err
	}
	if err = dec.CheckSize(key, int(size)); err != nil {
		return "", err
	}
	return util.Bytes2str(dec.BytesN(nil, int(size))), nil
}

func DecodeUint64(dec *BinaryDecoder, key string) (uint64, error) {
	if err := dec.CheckSize(key, util.Uint64SizeBytes); err != nil {
		return 0, err
	}
	return dec.Uint64(), nil
}

func DecodeUint32(dec *BinaryDecoder, key string) (uint32, error) {
	if err := dec.CheckSize(key, util.Uint32SizeBytes); err != nil {
		return 0, err
	}
	return dec.Uint32(), nil
}

func DecodeUint16(dec *BinaryDecoder, key string) (uint16, error) {
	if err := dec.CheckSize(key, util.Uint16SizeBytes); err != nil {
		return 0, err
	}
	return dec.Uint16(), nil
}

func DecodeInt64(dec *BinaryDecoder, key string) (int64, error) {
	if err := dec.CheckSize(key, util.Int64SizeBytes); err != nil {
		return 0, err
	}
	return dec.Int64(), nil
}

func DecodeInt32(dec *BinaryDecoder, key string) (int32, error) {
	if err := dec.CheckSize(key, util.Int32SizeBytes); err != nil {
		return 0, err
	}
	return dec.Int32(), nil
}

func DecodeArray(dec *BinaryDecoder, key string, cb func(dec *BinaryDecoder) error) error {
	n, err := DecodeUint32(dec, key)
	if err != nil {
		return err
	}

	nInt := int(n)
	if nInt < 0 {
		return errors.New("array length overflow")
	}

	for range nInt {
		if err = cb(dec); err != nil {
			return err
		}
	}
	return nil
}
