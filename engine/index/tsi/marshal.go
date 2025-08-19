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
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// Tag represents a (key, value) tag for metric.
type Tag struct {
	Key   []byte // Tag key bytes
	Value []byte // Tag value bytes
}

// Reset clears the tag's key and value while retaining underlying memory.
func (tag *Tag) Reset() {
	tag.Key = tag.Key[:0]
	tag.Value = tag.Value[:0]
}

// Equal checks if two tags are identical in both key and value.
func (tag *Tag) Equal(other *Tag) bool {
	return bytes.Equal(tag.Key, other.Key) && bytes.Equal(tag.Value, other.Value)
}

// Marshal appends the marshaled tag (key + value) to the destination buffer.
func (tag *Tag) Marshal(dst []byte) []byte {
	dst = marshalTagValue(dst, tag.Key)
	dst = marshalTagValue(dst, tag.Value)
	return dst
}

// Unmarshal parses a tag from the source buffer, updating the tag's key and value.
// Returns the remaining unparsed bytes and any error encountered.
func (tag *Tag) Unmarshal(src []byte) ([]byte, error) {
	var err error

	// Unmarshal tag key
	src, tag.Key, err = unmarshalTagValue(tag.Key[:0], src)
	if err != nil {
		return src, fmt.Errorf("unmarshal key failed: %w", err)
	}

	// Unmarshal tag value
	src, tag.Value, err = unmarshalTagValue(tag.Value[:0], src)
	if err != nil {
		return src, fmt.Errorf("unmarshal value failed: %w", err)
	}

	return src, nil
}

// marshalTagValue encodes a tag value (or key) into the destination buffer,
// escaping special characters and appending a trailing tag separator.
func marshalTagValue(dst, src []byte) []byte {
	// Check if any special characters exist to avoid unnecessary processing
	hasSpecialChars := bytes.IndexByte(src, escapeChar) != -1 ||
		bytes.IndexByte(src, tagSeparatorChar) != -1 ||
		bytes.IndexByte(src, kvSeparatorChar) != -1

	if !hasSpecialChars {
		dst = append(dst, src...)
		dst = append(dst, tagSeparatorChar)
		return dst
	}

	// Escape special characters
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

// unmarshalTagValue decodes a tag value (or key) from the source buffer,
// handling escaped characters and stopping at the tag separator.
// Returns the remaining bytes, the decoded value, and any error.
func unmarshalTagValue(dst, src []byte) ([]byte, []byte, error) {
	// Find end of tag value (tag separator)
	sepIdx := bytes.IndexByte(src, tagSeparatorChar)
	if sepIdx == -1 {
		return src, dst, fmt.Errorf("missing tag separator")
	}

	// Extract encoded value and advance source
	encoded := src[:sepIdx]
	src = src[sepIdx+1:]

	// Process escaped characters
	for len(encoded) > 0 {
		escapeIdx := bytes.IndexByte(encoded, escapeChar)
		if escapeIdx == -1 {
			// No more escapes - append remaining bytes
			dst = append(dst, encoded...)
			break
		}

		// Append bytes before escape
		dst = append(dst, encoded[:escapeIdx]...)
		encoded = encoded[escapeIdx+1:]

		// Check for valid escaped character
		if len(encoded) == 0 {
			return src, dst, fmt.Errorf("truncated escape sequence")
		}

		// Resolve escape code
		switch encoded[0] {
		case '0':
			dst = append(dst, escapeChar)
		case '1':
			dst = append(dst, tagSeparatorChar)
		case '2':
			dst = append(dst, kvSeparatorChar)
		default:
			return src, dst, fmt.Errorf("invalid escape code: %c", encoded[0])
		}

		encoded = encoded[1:]
	}

	return src, dst, nil
}

// marshalTagValueNoTrailingTagSeparator encodes a tag value without the trailing separator.
func marshalTagValueNoTrailingTagSeparator(dst, src []byte) []byte {
	dst = marshalTagValue(dst, src)
	// Remove the trailing tag separator added by marshalTagValue
	if len(dst) > 0 {
		dst = dst[:len(dst)-1]
	}
	return dst
}

// marshalCompositeTagKey encodes a composite tag key with the given name and key.
func marshalCompositeTagKey(dst, name, key []byte) []byte {
	dst = append(dst, compositeTagKeyPrefix)
	dst = encoding.MarshalVarUint64(dst, uint64(len(name)))
	dst = append(dst, name...)
	dst = append(dst, key...)
	return dst
}

// unmarshalCompositeTagKey decodes a composite tag key, returning the remaining bytes and the name.
func unmarshalCompositeTagKey(src []byte) ([]byte, []byte, error) {
	if len(src) < 1 {
		return nil, nil, fmt.Errorf("insufficient data for composite tag key")
	}

	// Skip prefix byte
	src = src[1:]

	// Decode name length
	l, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		return nil, nil, fmt.Errorf("unmarshal VarUint64 Fail")
	}
	tail := src[nSize:]

	name := tail[:l]
	return tail[l:], name, nil
}

// marshalCompositeNamePrefix encodes a prefix for composite names using the given name.
func marshalCompositeNamePrefix(dst, name []byte) []byte {
	dst = append(dst, compositeTagKeyPrefix)
	dst = encoding.MarshalVarUint64(dst, uint64(len(name)))
	dst = append(dst, name...)
	return dst
}

func ParseItem(item []byte) (res *util.Item, err error) {
	if len(item) < 9 {
		return res, fmt.Errorf("item format err. length [%d] is too short", len(item))
	}
	res = &util.Item{}
	header := item[0]
	switch header {
	case nsPrefixKeyToTSID:
		uStart := len(item) - 8
		uBytes := item[uStart:]
		tsid := encoding.UnmarshalUint64(uBytes)

		flagIndex := uStart - 1
		if flagIndex < 1 {
			return res, fmt.Errorf("item format err. can not find kvSeparatorChar. ")
		}
		flag := item[flagIndex]
		if flag != kvSeparatorChar {
			return res, fmt.Errorf("item format err. kvSeparatorChar is not in the expected position. ")
		}
		strBytes := item[1:flagIndex]
		strBytesCopy := append([]byte{}, strBytes...)
		mstName, _, err := influx.MeasurementName(strBytesCopy)
		if err != nil {
			return res, err
		}

		res.Name = string(mstName)
		res.Tsid = tsid
		res.SeriesKey = string(strBytes)
		return res, nil
	case nsPrefixTSIDToKey:
		uBytes := item[1:9]
		tsid := encoding.UnmarshalUint64(uBytes)
		strBytes := item[9:]
		strBytesCopy := append([]byte{}, strBytes...)
		mstName, _, err := influx.MeasurementName(strBytesCopy)
		if err != nil {
			return res, err
		}

		res.Name = string(mstName)
		res.Tsid = tsid
		res.SeriesKey = string(strBytes)
		return res, nil
	case nsPrefixTagToTSIDs:
		uStart := len(item) - 8
		uBytes := item[uStart:]
		tsid := encoding.UnmarshalUint64(uBytes)

		marshalTagValueBytes := item[1:uStart]
		if len(marshalTagValueBytes) == 0 {
			return res, fmt.Errorf("cannot find tag value")
		}
		tagValueBytesCopy := append([]byte{}, marshalTagValueBytes...)
		src, dst1, err := unmarshalTagValue([]byte{}, tagValueBytesCopy)
		if err != nil {
			return res, err
		}
		_, dst2, err := unmarshalTagValue([]byte{}, src)
		if err != nil {
			return res, err
		}
		keyBytes, nameBytes, err := unmarshalCompositeTagKey(dst1)
		if err != nil {
			return res, err
		}

		res.Tsid = tsid
		res.Name = string(nameBytes)
		res.Key = string(keyBytes)
		res.TagValue = string(dst2)
		return res, nil
	default:
		return res, nil
	}
}
