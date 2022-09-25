package tsi

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

/*
Copyright 2019-2022 VictoriaMetrics, Inc.
This code is originally from: This code is originally from: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.67.0/lib/storage/metric_name.go and has been modified
Use marshalTagValue and unmarshalTagValue to marshal and unmarshal tagValue etc.
*/

import (
	"bytes"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// Tag represents a (key, value) tag for metric.
type Tag struct {
	Key   []byte
	Value []byte
}

// Reset resets the tag.
func (tag *Tag) Reset() {
	tag.Key = tag.Key[:0]
	tag.Value = tag.Value[:0]
}

// Equal returns true if tag equals t
func (tag *Tag) Equal(t *Tag) bool {
	return string(tag.Key) == string(t.Key) && string(tag.Value) == string(t.Value)
}

// Marshal appends marshaled tag to dst and returns the result.
func (tag *Tag) Marshal(dst []byte) []byte {
	dst = marshalTagValue(dst, tag.Key)
	dst = marshalTagValue(dst, tag.Value)
	return dst
}

// Unmarshal unmarshals tag from src and returns the remaining data from src.
func (tag *Tag) Unmarshal(src []byte) ([]byte, error) {
	var err error
	src, tag.Key, err = unmarshalTagValue(tag.Key[:0], src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal key: %w", err)
	}
	src, tag.Value, err = unmarshalTagValue(tag.Value[:0], src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal value: %w", err)
	}
	return src, nil
}

func marshalTagValue(dst, src []byte) []byte {
	n1 := bytes.IndexByte(src, escapeChar)
	n2 := bytes.IndexByte(src, tagSeparatorChar)
	n3 := bytes.IndexByte(src, kvSeparatorChar)
	if n1 < 0 && n2 < 0 && n3 < 0 {
		dst = append(dst, src...)
		dst = append(dst, tagSeparatorChar)
		return dst
	}

	for _, ch := range src {
		switch ch {
		case escapeChar:
			dst = append(dst, escapeChar, '0')
		case tagSeparatorChar:
			dst = append(dst, escapeChar, '1')
		case kvSeparatorChar:
			dst = append(dst, escapeChar, '2')
		default:
			dst = append(dst, ch)
		}
	}

	dst = append(dst, tagSeparatorChar)
	return dst
}

func unmarshalTagValue(dst, src []byte) ([]byte, []byte, error) {
	n := bytes.IndexByte(src, tagSeparatorChar)
	if n < 0 {
		return src, dst, fmt.Errorf("cannot find the end of tag value")
	}
	b := src[:n]
	src = src[n+1:]
	for {
		n := bytes.IndexByte(b, escapeChar)
		if n < 0 {
			dst = append(dst, b...)
			return src, dst, nil
		}
		dst = append(dst, b[:n]...)
		b = b[n+1:]
		if len(b) == 0 {
			return src, dst, fmt.Errorf("missing escaped char")
		}
		switch b[0] {
		case '0':
			dst = append(dst, escapeChar)
		case '1':
			dst = append(dst, tagSeparatorChar)
		case '2':
			dst = append(dst, kvSeparatorChar)
		default:
			return src, dst, fmt.Errorf("unsupported escaped char: %c", b[0])
		}
		b = b[1:]
	}
}

func marshalTagValueNoTrailingTagSeparator(dst, src []byte) []byte {
	dst = marshalTagValue(dst, src)
	// Remove trailing tagSeparatorChar
	return dst[:len(dst)-1]
}

func marshalCompositeTagKey(dst, name, key []byte) []byte {
	dst = append(dst, compositeTagKeyPrefix)
	dst = encoding.MarshalVarUint64(dst, uint64(len(name)))
	dst = append(dst, name...)
	dst = append(dst, key...)
	return dst
}

func unmarshalCompositeTagKey(src []byte) ([]byte, []byte, error) {
	if len(src) < 1 {
		return nil, nil, fmt.Errorf("invalid src: %q", src)
	}
	src = src[1:]
	tail, l, err := encoding.UnmarshalVarUint64(src)
	if err != nil {
		return nil, nil, err
	}

	name := tail[:l]
	return tail[l:], name, nil
}

func marshalCompositeNamePrefix(dst, name []byte) []byte {
	dst = append(dst, compositeTagKeyPrefix)
	dst = encoding.MarshalVarUint64(dst, uint64(len(name)))
	dst = append(dst, name...)
	return dst
}
