package influx

/*
Copyright 2019-2021 VictoriaMetrics, Inc.
This code is originally from: https://github.com/VictoriaMetrics/VictoriaMetrics/tree/v1.67.0/lib/protoparser/influx/parser.go and has been modified.

2022.01.23 Add parser for field of strings.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/pkg/errors"
	"github.com/valyala/fastjson/fastfloat"
)

var (
	// ErrPointMustHaveAField is returned when operating on a point that does not have any fields.
	ErrPointMustHaveAField   = errno.NewError(errno.WritePointMustHaveAField)
	ErrPointInvalidTimeField = errno.NewError(errno.WritePointInvalidTimeField)
	// ErrInvalidPoint is returned when a point cannot be parsed correctly.
	ErrInvalidPoint               = errno.NewError(errno.WriteInvalidPoint)
	ErrPointMustHaveAMeasurement  = errno.NewError(errno.WritePointMustHaveAMeasurement)
	ErrPointShouldHaveAllShardKey = errno.NewError(errno.WritePointShouldHaveAllShardKey)
)

var hasIndexOption byte
var hasNoIndexOption byte

func init() {
	hasIndexOption = 'y'
	hasNoIndexOption = 'n'
}

const (
	INDEXCOUNT = 1
)

var NoTimestamp = int64(-100)

// Rows contains parsed influx rows.
type PointRows struct {
	Rows []Row

	tagsPool   []Tag
	fieldsPool []Field
}

// Reset resets rs.
func (rs *PointRows) Reset() {
	// Reset rows, tags and fields in order to remove references to old data,
	// so GC could collect it.

	for i := range rs.Rows {
		rs.Rows[i].Reset()
	}
	rs.Rows = rs.Rows[:0]

	for i := range rs.tagsPool {
		rs.tagsPool[i].Reset()
	}
	rs.tagsPool = rs.tagsPool[:0]

	for i := range rs.fieldsPool {
		rs.fieldsPool[i].Reset()
	}
	rs.fieldsPool = rs.fieldsPool[:0]
}

// Unmarshal unmarshals influx line protocol rows from s.
//
// See https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/
//
// s shouldn't be modified when rs is in use.
func (rs *PointRows) Unmarshal(s string) error {
	var err error
	rs.Rows, rs.tagsPool, rs.fieldsPool, err = unmarshalRows(rs.Rows[:0], s, rs.tagsPool[:0], rs.fieldsPool[:0])
	return err
}

type Rows []Row

type WritePointsIn struct {
	Prs []Row
}

func (rs Rows) Len() int {
	return len(rs)
}

func (rs Rows) Less(i, j int) bool {
	return rs[i].Timestamp < rs[j].Timestamp
}

func (rs Rows) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// Row is a single influx row.
type Row struct {
	Name         string
	Version      uint16
	Tags         PointTags
	Fields       Fields
	ShardKey     []byte
	Timestamp    int64
	IndexKey     []byte
	SeriesId     uint64
	IndexOptions IndexOptions
}

func (r *Row) Reset() {
	r.Name = ""
	r.Tags = nil
	r.Fields = nil
	r.ShardKey = r.ShardKey[:0]
	r.Timestamp = 0
	r.IndexKey = nil
	r.SeriesId = 0
}

func (r *Row) CheckValid() error {
	if len(r.Name) == 0 {
		return ErrPointMustHaveAMeasurement
	}
	if len(r.Fields) == 0 {
		return ErrPointMustHaveAField
	}
	for i := range r.Fields {
		if r.Fields[i].Key == "time" {
			return ErrPointInvalidTimeField
		}
	}
	return nil
}

func (r *Row) Clone(rr *Row) {
	r.Name = rr.Name
	r.Tags = rr.Tags
	r.Fields = rr.Fields
	r.ShardKey = rr.ShardKey
	r.Timestamp = rr.Timestamp
	r.IndexKey = rr.IndexKey
	r.SeriesId = rr.SeriesId
	r.IndexOptions = rr.IndexOptions
}

func (r *Row) Copy(p *Row) {
	if cap(r.Fields) >= len(p.Fields) {
		r.Fields = r.Fields[:len(p.Fields)]
	} else {
		r.Fields = make([]Field, len(p.Fields))
	}
	for i := range p.Fields {
		r.Fields[i].NumValue = p.Fields[i].NumValue
		r.Fields[i].StrValue = p.Fields[i].StrValue
		r.Fields[i].Type = p.Fields[i].Type
		r.Fields[i].Key = p.Fields[i].Key
	}
}

func (r *Row) UnmarshalShardKeyByTag(tags []string) error {
	r.ShardKey = append(r.ShardKey[:0], r.Name...)
	if len(tags) == 0 {
		for j := range r.Tags {
			if err := r.checkDuplicateTag(j); err != nil {
				return err
			}
			r.appendShardKey(j)
		}
		return nil
	}
	i, j := 0, 0
	for i < len(tags) && j < len(r.Tags) {
		cmp := strings.Compare(tags[i], r.Tags[j].Key)
		if cmp < 0 {
			return ErrPointShouldHaveAllShardKey
		}

		if err := r.checkDuplicateTag(j); err != nil {
			return err
		}

		if cmp == 0 {
			r.appendShardKey(j)
			i++
		}

		j++
	}

	if i < len(tags) {
		return ErrPointShouldHaveAllShardKey
	}
	for j < len(r.Tags)-1 {
		if err := r.checkDuplicateTag(j); err != nil {
			return err
		}
		j++
	}
	return nil
}

func (r *Row) checkDuplicateTag(idx int) error {
	if idx < len(r.Tags)-1 {
		if string(r.Tags[idx].Key) == string(r.Tags[idx+1].Key) {
			return fmt.Errorf("duplicate tag %s", string(r.Tags[idx].Key))
		}
	}
	return nil
}

func (r *Row) appendShardKey(idx int) {
	r.ShardKey = append(r.ShardKey, ","...)
	r.ShardKey = append(r.ShardKey, r.Tags[idx].Key...)
	r.ShardKey = append(r.ShardKey, "="...)
	r.ShardKey = append(r.ShardKey, r.Tags[idx].Value...)
}

func (r *Row) FastMarshalBinary(dst []byte) ([]byte, error) {
	var err error
	name := bytesutil.ToUnsafeBytes(r.Name)
	if len(name) == 0 {
		return nil, ErrPointMustHaveAMeasurement
	}
	dst = append(dst, uint8(len(name)))
	dst = append(dst, name...)

	dst = encoding.MarshalUint32(dst, uint32(len(r.ShardKey)))
	dst = append(dst, r.ShardKey...)

	dst, err = r.marshalTags(dst)
	if err != nil {
		return nil, err
	}
	dst, err = r.marshalFields(dst)
	if err != nil {
		return nil, err
	}

	dst, err = r.marshalIndexOptions(dst)
	if err != nil {
		return nil, err
	}

	dst = encoding.MarshalInt64(dst, r.Timestamp)
	return dst, nil
}

func FastMarshalMultiRows(src []byte, rows []Row) ([]byte, error) {
	// point number
	var err error
	src = encoding.MarshalUint32(src, uint32(len(rows)))
	for i := 0; i < len(rows); i++ {
		src, err = rows[i].FastMarshalBinary(src)
		if err != nil {
			return src, err
		}
	}
	return src, nil
}

func FastUnmarshalMultiRows(src []byte, rows []Row, tagPool []Tag, fieldPool []Field, indexOptionPool []IndexOption,
	indexKeyPool []byte) ([]Row, []Tag, []Field, []IndexOption, []byte, error) {
	pointsN := int(encoding.UnmarshalUint32(src))
	src = src[4:]
	var err error
	for len(src) > 0 {
		if len(rows) < cap(rows) {
			rows = rows[:len(rows)+1]
		} else {
			rows = append(rows, Row{})
		}
		row := &rows[len(rows)-1]
		src, tagPool, fieldPool, indexOptionPool, indexKeyPool, err =
			row.FastUnmarshalBinary(src, tagPool, fieldPool, indexOptionPool, indexKeyPool)
		if err != nil {
			return rows[:0], tagPool, fieldPool, indexOptionPool, indexKeyPool, err
		}
	}

	if len(rows) != pointsN {
		return rows[:0], tagPool, fieldPool, indexOptionPool, indexKeyPool, errors.New("unmarshal error len(rows) != pointsN")
	}
	return rows, tagPool, fieldPool, indexOptionPool, indexKeyPool, nil
}

func (r *Row) FastUnmarshalBinary(src []byte, tagpool []Tag, fieldpool []Field, indexOptionPool []IndexOption, indexKeypool []byte) ([]byte, []Tag, []Field, []IndexOption, []byte, error) {
	if len(src) < 1 {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, errors.New("too small bytes for row binary")
	}
	var err error

	mLen := int(src[0])
	src = src[1:]
	if len(src) < mLen+4 {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, errors.New("too small bytes for row measurement")
	}
	r.Name = bytesutil.ToUnsafeString(src[:mLen])
	src = src[mLen:]

	skLen := encoding.UnmarshalUint32(src)
	src = src[4:]
	if len(src) < int(skLen+4) {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, errors.New("too small bytes for row shardKey")
	}
	r.ShardKey = append(r.ShardKey[:0], src[:skLen]...)
	src = src[skLen:]

	src, tagpool, err = r.unmarshalTags(src, tagpool)
	if err != nil {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, err
	}
	if len(src) < 4 {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, errors.New("too small bytes for row field count")
	}

	src, fieldpool, err = r.unmarshalFields(src, fieldpool)
	if err != nil {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, err
	}

	src, indexOptionPool, err = r.unmarshalIndexOptions(src, indexOptionPool)
	if err != nil {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, err
	}

	r.Timestamp = encoding.UnmarshalInt64(src[:8])
	if len(src) < 8 {
		return nil, tagpool, fieldpool, indexOptionPool, indexKeypool, errors.New("too small bytes for row timestamp")
	}

	indexKeypool = r.UnmarshalIndexKeys(indexKeypool)

	return src[8:], tagpool, fieldpool, indexOptionPool, indexKeypool, nil
}

func (r *Row) marshalTags(dst []byte) ([]byte, error) {
	if r.Tags == nil {
		dst = encoding.MarshalUint32(dst, 0)
		return dst, nil
	}
	dst = encoding.MarshalUint32(dst, uint32(len(r.Tags)))
	for i := range r.Tags {
		kl := len(r.Tags[i].Key)
		dst = encoding.MarshalUint16(dst, uint16(kl)) //append(dst, uint8(kl))
		dst = append(dst, r.Tags[i].Key...)
		vl := len(r.Tags[i].Value)
		dst = encoding.MarshalUint16(dst, uint16(vl)) //append(dst, uint8(vl))
		dst = append(dst, r.Tags[i].Value...)
	}
	return dst, nil
}

func (r *Row) unmarshalTags(src []byte, tagpool []Tag) ([]byte, []Tag, error) {
	tagN := int(encoding.UnmarshalUint32(src[:4]))
	src = src[4:]
	start := len(tagpool)
	for i := 0; i < tagN; i++ {
		if len(src) < 1 {
			return nil, tagpool, errors.New("too small bytes for row tag key len")
		}
		tl := int(encoding.UnmarshalUint16(src[:2])) //int(src[0])
		src = src[2:]
		if len(src) < tl+1 {
			return nil, tagpool, errors.New("too small bytes for row tag key")
		}
		if len(tagpool) < cap(tagpool) {
			tagpool = tagpool[:len(tagpool)+1]
		} else {
			tagpool = append(tagpool, Tag{})
		}
		tg := &tagpool[len(tagpool)-1]
		tg.Key = bytesutil.ToUnsafeString(src[:tl])
		src = src[tl:]
		vl := int(encoding.UnmarshalUint16(src[:2])) //int(src[0])
		if len(src) < vl {
			tagpool = tagpool[:len(tagpool)-1]
			return nil, tagpool, errors.New("too small bytes for row tag value")
		}
		src = src[2:]
		tg.Value = bytesutil.ToUnsafeString(src[:vl])
		src = src[vl:]
	}
	r.Tags = tagpool[start:]
	return src, tagpool, nil
}

func (r *Row) marshalFields(dst []byte) ([]byte, error) {
	if len(r.Fields) == 0 {
		return nil, ErrPointMustHaveAField
	}

	dst = encoding.MarshalUint32(dst, uint32(len(r.Fields)))

	for i := range r.Fields {
		dst = encoding.MarshalUint16(dst, uint16(len(r.Fields[i].Key)))
		dst = append(dst, r.Fields[i].Key...)
		if r.Fields[i].Type == Field_Type_Unknown {
			return nil, ErrInvalidPoint
		}

		dst = append(dst, uint8(r.Fields[i].Type))

		if r.Fields[i].Type == Field_Type_String {
			dst = encoding.MarshalUint64(dst, uint64(len(r.Fields[i].StrValue)))
			dst = append(dst, r.Fields[i].StrValue...)
		} else {
			dst = numberenc.MarshalFloat64(dst, r.Fields[i].NumValue)
		}

	}
	return dst, nil
}

func (r *Row) unmarshalFields(src []byte, fieldpool []Field) ([]byte, []Field, error) {
	fieldN := int(encoding.UnmarshalUint32(src[:4]))
	src = src[4:]
	start := len(fieldpool)
	for i := 0; i < fieldN; i++ {
		if len(src) < 1 {
			return nil, fieldpool, errors.New("too small for field key length")
		}
		l := int(encoding.UnmarshalUint16(src[:2])) //int(src[0])
		src = src[2:]
		if len(src) < l+1 {
			return nil, fieldpool, errors.New("too small for field key")
		}
		if len(fieldpool) < cap(fieldpool) {
			fieldpool = fieldpool[:len(fieldpool)+1]
		} else {
			fieldpool = append(fieldpool, Field{})
		}
		fd := &fieldpool[len(fieldpool)-1]
		fd.Key = bytesutil.ToUnsafeString(src[:l])
		src = src[l:]

		fd.Type = int32(src[0])
		if fd.Type <= Field_Type_Unknown || fd.Type >= Field_Type_Last {
			fieldpool = fieldpool[:len(fieldpool)-1]
			return nil, fieldpool, errors.New("error field type")
		}
		src = src[1:]

		if fd.Type == Field_Type_String {
			if len(src) < 8 {
				fieldpool = fieldpool[:len(fieldpool)-1]
				return nil, fieldpool, errors.New("too small for string field length")
			}
			l = int(encoding.UnmarshalUint64(src[:8]))
			src = src[8:]
			if len(src) < l {
				fieldpool = fieldpool[:len(fieldpool)-1]
				return nil, fieldpool, errors.New("too small for string field value")
			}
			fd.StrValue = bytesutil.ToUnsafeString(src[:l])
			src = src[l:]
		} else {
			if len(src) < 8 {
				fieldpool = fieldpool[:len(fieldpool)-1]
				return nil, fieldpool, errors.New("too small for field")
			}
			fd.NumValue = numberenc.UnmarshalFloat64(src[:8])
			src = src[8:]
		}
	}
	r.Fields = fieldpool[start:]
	return src, fieldpool, nil
}

func (r *Row) marshalIndexOptions(dst []byte) ([]byte, error) {
	if len(r.IndexOptions) == 0 {
		dst = append(dst, hasNoIndexOption)
		return dst, nil
	}

	dst = append(dst, hasIndexOption)
	dst = encoding.MarshalUint32(dst, uint32(len(r.IndexOptions)))

	for i := range r.IndexOptions {
		dst = encoding.MarshalUint32(dst, r.IndexOptions[i].Oid)
		dst = encoding.MarshalUint16(dst, uint16(len(r.IndexOptions[i].IndexList)))
		for j := range r.IndexOptions[i].IndexList {
			dst = encoding.MarshalUint16(dst, r.IndexOptions[i].IndexList[j])
		}
	}
	return dst, nil
}

func (r *Row) unmarshalIndexOptions(src []byte, indexOptionPool []IndexOption) ([]byte, []IndexOption, error) {
	isIndexOpt := src[:INDEXCOUNT]
	if isIndexOpt[0] == hasNoIndexOption {
		src = src[INDEXCOUNT:]
		return src, indexOptionPool, nil
	}
	src = src[INDEXCOUNT:]
	indexN := int(encoding.UnmarshalUint32(src[:4]))
	src = src[4:]
	start := len(indexOptionPool)
	for i := 0; i < indexN; i++ {
		if len(src) < 1 {
			return nil, indexOptionPool, errors.New("too small for indexOption key length")
		}

		if len(indexOptionPool) < cap(indexOptionPool) {
			indexOptionPool = indexOptionPool[:len(indexOptionPool)+1]
		} else {
			indexOptionPool = append(indexOptionPool, IndexOption{})
		}
		indexOpt := &indexOptionPool[len(indexOptionPool)-1]
		indexOpt.Oid = encoding.UnmarshalUint32(src[:4])
		src = src[4:]
		indexListLen := encoding.UnmarshalUint16(src[:2])
		indexOpt.IndexList = make([]uint16, indexListLen)
		src = src[2:]
		for i := 0; i < int(indexListLen); i++ {
			indexOpt.IndexList[i] = encoding.UnmarshalUint16(src[:2])
			src = src[2:]
		}
	}
	r.IndexOptions = indexOptionPool[start:]
	return src, indexOptionPool, nil
}

func MakeIndexKey(name string, tags PointTags, dst []byte) []byte {
	indexKl := 4 + // total length of indexkey
		2 + // measurment name length
		len(name) + // measurment name
		2 + // tag count
		4*len(tags) + // length of each tag key and value
		tags.TagsSize() // size of tag keys/values
	start := len(dst)

	// marshal total len
	dst = encoding.MarshalUint32(dst, uint32(indexKl))
	// marshal measurement
	dst = encoding.MarshalUint16(dst, uint16(len(name)))
	dst = append(dst, name...)
	// marshal tags
	dst = encoding.MarshalUint16(dst, uint16(len(tags)))
	for i := range tags {
		kl := len(tags[i].Key)
		dst = encoding.MarshalUint16(dst, uint16(kl))
		dst = append(dst, tags[i].Key...)
		vl := len(tags[i].Value)
		dst = encoding.MarshalUint16(dst, uint16(vl))
		dst = append(dst, tags[i].Value...)
	}
	return dst[start:]
}

// MakeGroupTagsKey converts a tag set to bytes for use as a lookup key.
func MakeGroupTagsKey(dims []string, tags PointTags, dst []byte) []byte {
	// precondition: keys is sorted
	// precondition: models.PointTags is sorted

	// Empty maps marshal to empty bytes.
	// we also return nil even if len(dims) is not zero and len(tags) is zero,
	// there is no need to return tagk1=,tagk2=, ...
	// since both 'nil' and all tag's value nil are the min element during sort
	if len(dims) == 0 || len(tags) == 0 {
		return nil
	}

	i, j := 0, 0
	for i < len(dims) && j < len(tags) {
		if dims[i] < tags[j].Key {
			dst = append(dst, bytesutil.ToUnsafeBytes(dims[i])...)
			dst = append(dst, []byte{'=', ','}...)
			i++
		} else if dims[i] > tags[j].Key {
			j++
		} else {
			dst = append(dst, dims[i]...)
			dst = append(dst, '=')
			dst = append(dst, bytesutil.ToUnsafeBytes(tags[j].Value)...)
			dst = append(dst, ',')

			i++
			j++
		}
	}
	// skip last ','
	if len(dst) > 1 {
		return dst[:len(dst)-1]
	}
	return dst
}

func (r *Row) UnmarshalIndexKeys(indexkeypool []byte) []byte {
	indexKl := 4 + // total length of indexkey
		2 + // measurment name length
		len(r.Name) + // measurment name
		2 + // tag count
		4*len(r.Tags) + // length of each tag key and value
		r.Tags.TagsSize() // size of tag keys/values
	start := len(indexkeypool)

	// marshal total len
	indexkeypool = encoding.MarshalUint32(indexkeypool, uint32(indexKl))
	// marshal measurement
	indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(len(r.Name)))
	indexkeypool = append(indexkeypool, r.Name...)
	// marshal tags
	indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(len(r.Tags)))
	for i := range r.Tags {
		kl := len(r.Tags[i].Key)
		indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(kl))
		indexkeypool = append(indexkeypool, r.Tags[i].Key...)
		vl := len(r.Tags[i].Value)
		indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(vl))
		indexkeypool = append(indexkeypool, r.Tags[i].Value...)
	}

	r.IndexKey = indexkeypool[start:]
	return indexkeypool
}

func IndexKeyToTags(src []byte, isCopy bool, dst *PointTags) (*PointTags, error) {
	_, data, err := MeasurementName(src)
	if err != nil {
		return nil, err
	}

	tagsN := int(encoding.UnmarshalUint16(data))
	data = data[2:]

	// increase len if slice do not have enough rooms
	if dif := tagsN - cap(*dst); dif > 0 {
		*dst = (*dst)[:cap(*dst)]
		*dst = append(*dst, make(PointTags, dif)...)
	}
	*dst = (*dst)[:tagsN]

	for i := 0; i < tagsN; i++ {
		l := int(encoding.UnmarshalUint16(data))
		data = data[2:]
		if l+2 > len(data) {
			return nil, fmt.Errorf("too small data for tag key")
		}
		if isCopy {
			(*dst)[i].Key = string(data[:l])
		} else {
			(*dst)[i].Key = bytesutil.ToUnsafeString(data[:l])
		}

		data = data[l:]

		l = int(encoding.UnmarshalUint16(data))
		data = data[2:]
		if l > len(data) {
			return nil, fmt.Errorf("too small data for tag value")
		}
		if isCopy {
			(*dst)[i].Value = string(data[:l])
		} else {
			(*dst)[i].Value = bytesutil.ToUnsafeString(data[:l])
		}

		data = data[l:]
	}

	return dst, nil
}

var bPool = &sync.Pool{}

func GetBytesBuffer() []byte {
	v := bPool.Get()
	if v != nil {
		return v.([]byte)
	}
	return make([]byte, 0, 64)
}

func PutBytesBuffer(buf []byte) {
	buf = buf[:0]
	bPool.Put(buf)
}

// Parse2SeriesKey parse encoded index key to line protocol series key
// encoded index key format: [total len][ms len][ms][tagk1 len][tagk1 val]...]
// parse to line protocol format: mst,tagkey1=tagv1,tagk2=tagv2...
func Parse2SeriesKey(key []byte, dst []byte) []byte {
	msName, src, _ := MeasurementName(key)

	dst = append(dst, msName...)
	dst = append(dst, ',')
	tagsN := encoding.UnmarshalUint16(src)
	src = src[2:]
	var i uint16
	for i = 0; i < tagsN; i++ {
		keyLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		dst = append(dst, src[:keyLen]...)
		dst = append(dst, '=')
		src = src[keyLen:]

		valLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		dst = append(dst, src[:valLen]...)
		dst = append(dst, ',')
		src = src[valLen:]
	}
	return dst[:len(dst)-1]
}

// MeasurementName extract measurement from series key,
// return measurement, tail, error
func MeasurementName(src []byte) ([]byte, []byte, error) {
	if len(src) < 4 {
		return nil, nil, fmt.Errorf("too small data for tags")
	}

	kl := int(encoding.UnmarshalUint32(src))
	if len(src) < kl {
		return nil, nil, fmt.Errorf("too small indexKey")
	}
	src = src[4:]

	mnl := int(encoding.UnmarshalUint16(src))
	src = src[2:]
	if mnl+2 > len(src) {
		return nil, nil, fmt.Errorf("too small data for measurement(%d: %d > %d)", kl, mnl, len(src))
	}
	mn := src[:mnl]
	src = src[mnl:]

	return mn, src, nil
}

func checkWhitespace(buf string, i int) int {
	for i < len(buf) {
		if buf[i] != ' ' && buf[i] != '\t' && buf[i] != 0 {
			break
		}
		i++
	}
	return i
}

func (r *Row) unmarshal(s string, tagsPool []Tag, fieldsPool []Field, noEscapeChars bool) ([]Tag, []Field, error) {
	r.Reset()
	start := checkWhitespace(s, 0)
	s = s[start:]
	n := nextUnescapedChar(s, ' ', noEscapeChars)
	if n < 0 {
		return tagsPool, fieldsPool, ErrPointMustHaveAField
	}
	measurementTags := s[:n]
	s = stripLeadingWhitespace(s[n+1:])

	// Parse measurement and tags
	var err error
	n = nextUnescapedChar(measurementTags, ',', noEscapeChars)
	if n >= 0 {
		tagsStart := len(tagsPool)
		tagsPool, err = unmarshalTags(tagsPool, measurementTags[n+1:], noEscapeChars)
		if err != nil {
			return tagsPool, fieldsPool, err
		}
		tags := tagsPool[tagsStart:]
		r.Tags = tags[:len(tags):len(tags)]
		sort.Sort(&r.Tags)
		measurementTags = measurementTags[:n]
	}
	r.Name = unescapeTagValue(measurementTags, noEscapeChars)
	// Allow empty r.Name. In this case metric name is constructed directly from field keys.

	// Parse fields
	fieldsStart := len(fieldsPool)
	hasQuotedFields := nextUnescapedChar(s, '"', noEscapeChars) >= 0
	n = nextUnquotedChar(s, ' ', noEscapeChars, hasQuotedFields)
	if n < 0 {
		// No timestamp.
		fieldsPool, err = unmarshalInfluxFields(fieldsPool, s, noEscapeChars, hasQuotedFields)
		if err != nil {
			return tagsPool, fieldsPool, err
		}
		fields := fieldsPool[fieldsStart:]
		r.Fields = fields[:len(fields):len(fields)]
		r.Timestamp = NoTimestamp
		return tagsPool, fieldsPool, nil
	}
	fieldsPool, err = unmarshalInfluxFields(fieldsPool, s[:n], noEscapeChars, hasQuotedFields)
	if err != nil {
		if strings.HasPrefix(s[n+1:], "HTTP/") {
			return tagsPool, fieldsPool, fmt.Errorf("please switch from tcp to http protocol for data ingestion; " +
				"do not set `-influxListenAddr` command-line flag, since it is needed for tcp protocol only")
		}
		return tagsPool, fieldsPool, err
	}
	fields := fieldsPool[fieldsStart:]
	r.Fields = fields[:len(fields):len(fields)]
	s = stripLeadingWhitespace(s[n+1:])

	// Parse timestamp
	timestamp, err := nextTimestamp(s)
	if err != nil {
		if strings.HasPrefix(s, "HTTP/") {
			return tagsPool, fieldsPool, fmt.Errorf("please switch from tcp to http protocol for data ingestion; " +
				"do not set `-influxListenAddr` command-line flag, since it is needed for tcp protocol only")
		}
		return tagsPool, fieldsPool, fmt.Errorf("cannot parse timestamp %q: %w", s, err)
	}
	r.Timestamp = timestamp

	return tagsPool, fieldsPool, nil
}

func (r *Row) TagsSize() int {
	var total int
	for i := range r.Tags {
		total += r.Tags[i].Size()
	}
	return total
}

// PointTag represents influx tag.
type Tag struct {
	Key   string
	Value string
}

func (tag *Tag) Reset() {
	tag.Key = ""
	tag.Value = ""
}

func (tag *Tag) unmarshal(s string, noEscapeChars bool) error {
	tag.Reset()
	n := nextUnescapedChar(s, '=', noEscapeChars)
	if n < 0 {
		return fmt.Errorf("missing tag value for %q", s)
	}
	tag.Key = unescapeTagValue(s[:n], noEscapeChars)
	tag.Value = unescapeTagValue(s[n+1:], noEscapeChars)
	return nil
}

func (tag *Tag) Size() int {
	return len(tag.Key) + len(tag.Value)
}

type PointTags []Tag

func (pts *PointTags) Less(i, j int) bool {
	x := *pts
	return x[i].Key < x[j].Key
}
func (pts *PointTags) Len() int { return len(*pts) }
func (pts *PointTags) Swap(i, j int) {
	x := *pts
	x[i], x[j] = x[j], x[i]
}

func (pts *PointTags) FindPointTag(tagName string) *Tag {
	tags := *pts
	left, right := 0, len(tags)
	for left < right {
		mid := (left + right) / 2
		if tagName == tags[mid].Key {
			return &tags[mid]
		} else if tagName > tags[mid].Key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return nil
}

func (pts PointTags) TagsSize() int {
	var total int
	for i := range pts {
		total += pts[i].Size()
	}
	return total
}

func (pts *PointTags) Reset() {
	for i := range *pts {
		(*pts)[i].Reset()
	}
}

const (
	Field_Type_Unknown = 0
	Field_Type_Int     = 1
	Field_Type_UInt    = 2
	Field_Type_Float   = 3
	Field_Type_String  = 4
	Field_Type_Boolean = 5
	Field_Type_Tag     = 6
	Field_Type_Last    = 7
)

var FieldTypeName = map[int]string{
	Field_Type_Unknown: "Unknown",
	Field_Type_Int:     "Integer",
	Field_Type_UInt:    "Unsigned",
	Field_Type_Float:   "Float",
	Field_Type_String:  "String",
	Field_Type_Boolean: "Boolean",
	Field_Type_Tag:     "Tag",
	Field_Type_Last:    "Unknown",
}

func FieldType2Val(fieldType int) (interface{}, error) {
	switch fieldType {
	case Field_Type_Float:
		return (*float64)(nil), nil
	case Field_Type_Int:
		return (*int64)(nil), nil
	case Field_Type_String:
		return (*string)(nil), nil
	case Field_Type_Boolean:
		return (*bool)(nil), nil
	default:
		return nil, fmt.Errorf("unsupported field type %d", fieldType)
	}
}

func FieldTypeString(fieldType int32) string {
	switch fieldType {
	case Field_Type_Int:
		return "integer"
	case Field_Type_UInt:
		return "unsigned"
	case Field_Type_Float:
		return "float"
	case Field_Type_String:
		return "string"
	case Field_Type_Boolean:
		return "boolean"
	case Field_Type_Tag:
		return "tag"
	default:
		return "unknown"
	}
}

// Field represents influx field.
type Field struct {
	Key      string
	NumValue float64
	StrValue string
	Type     int32
}

type Fields []Field

func (fs Fields) Less(i, j int) bool {
	return fs[i].Key < fs[j].Key
}

func (fs Fields) Len() int {
	return len(fs)
}

func (fs Fields) Swap(i, j int) {
	fs[i], fs[j] = fs[j], fs[i]
}

func (f *Field) Reset() {
	f.Key = ""
	f.NumValue = 0
	f.StrValue = ""
	f.Type = Field_Type_Unknown
}

func (f *Field) unmarshal(s string, noEscapeChars, hasQuotedFields bool) error {
	f.Reset()
	n := nextUnescapedChar(s, '=', noEscapeChars)
	if n < 0 {
		return ErrPointMustHaveAField
	}
	f.Key = unescapeTagValue(s[:n], noEscapeChars)
	if len(f.Key) == 0 {
		return fmt.Errorf("field key cannot be empty")
	}
	if hasQuotedFields && nextUnescapedChar(s[n:], '"', noEscapeChars) >= 0 {
		vstr, err := parseFieldStrValue(s[n+1:])
		if err != nil {
			return fmt.Errorf("cannot parse field value for %q: %w", f.Key, err)
		}
		f.StrValue = vstr
		f.Type = Field_Type_String
		return nil
	}
	v, t, err := parseFieldNumValue(s[n+1:])
	if err != nil {
		return fmt.Errorf("cannot parse field value for %q: %w", f.Key, err)
	}
	f.NumValue = v
	f.Type = t
	return nil
}

func unmarshalRows(dst []Row, s string, tagsPool []Tag, fieldsPool []Field) ([]Row, []Tag, []Field, error) {
	var err error
	noEscapeChars := strings.IndexByte(s, '\\') < 0
	for len(s) > 0 {
		n := strings.IndexByte(s, '\n')
		if n < 0 {
			// The last line.
			return unmarshalRow(dst, s, tagsPool, fieldsPool, noEscapeChars)
		}
		dst, tagsPool, fieldsPool, err = unmarshalRow(dst, s[:n], tagsPool, fieldsPool, noEscapeChars)
		s = s[n+1:]
	}
	return dst, tagsPool, fieldsPool, err
}

func unmarshalRow(dst []Row, s string, tagsPool []Tag, fieldsPool []Field, noEscapeChars bool) ([]Row, []Tag, []Field, error) {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		s = s[:len(s)-1]
	}
	if len(s) == 0 {
		// Skip empty line
		return dst, tagsPool, fieldsPool, nil
	}
	if s[0] == '#' {
		// Skip comment
		return dst, tagsPool, fieldsPool, nil
	}

	if cap(dst) > len(dst) {
		dst = dst[:len(dst)+1]
	} else {
		dst = append(dst, Row{})
	}
	r := &dst[len(dst)-1]
	var err error
	tagsPool, fieldsPool, err = r.unmarshal(s, tagsPool, fieldsPool, noEscapeChars)
	if err != nil {
		dst = dst[:len(dst)-1]
	}
	return dst, tagsPool, fieldsPool, err
}

func unmarshalTags(dst []Tag, s string, noEscapeChars bool) ([]Tag, error) {
	for {
		if cap(dst) > len(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, Tag{})
		}
		tag := &dst[len(dst)-1]
		n := nextUnescapedChar(s, ',', noEscapeChars)
		if n < 0 {
			if err := tag.unmarshal(s, noEscapeChars); err != nil {
				return dst[:len(dst)-1], err
			}
			if len(tag.Key) == 0 || len(tag.Value) == 0 {
				// Skip empty tag
				dst = dst[:len(dst)-1]
			}
			return dst, nil
		}
		if err := tag.unmarshal(s[:n], noEscapeChars); err != nil {
			return dst[:len(dst)-1], err
		}
		s = s[n+1:]
		if len(tag.Key) == 0 || len(tag.Value) == 0 {
			// Skip empty tag
			dst = dst[:len(dst)-1]
		}
	}
}

func unmarshalInfluxFields(dst []Field, s string, noEscapeChars, hasQuotedFields bool) ([]Field, error) {
	for {
		if cap(dst) > len(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, Field{})
		}
		f := &dst[len(dst)-1]
		n := nextUnquotedChar(s, ',', noEscapeChars, hasQuotedFields)
		if n < 0 {
			if err := f.unmarshal(s, noEscapeChars, hasQuotedFields); err != nil {
				return dst, err
			}
			return dst, nil
		}
		if err := f.unmarshal(s[:n], noEscapeChars, hasQuotedFields); err != nil {
			return dst, err
		}
		s = s[n+1:]
	}
}

func unescapeTagValue(s string, noEscapeChars bool) string {
	if noEscapeChars {
		// Fast path - no escape chars.
		return s
	}
	n := strings.IndexByte(s, '\\')
	if n < 0 {
		return s
	}

	// Slow path. Remove escape chars.
	dst := make([]byte, 0, len(s))
	for {
		dst = append(dst, s[:n]...)
		s = s[n+1:]
		if len(s) == 0 {
			return string(append(dst, '\\'))
		}
		ch := s[0]
		if ch != ' ' && ch != ',' && ch != '=' && ch != '\\' {
			dst = append(dst, '\\')
		}
		dst = append(dst, ch)
		s = s[1:]
		n = strings.IndexByte(s, '\\')
		if n < 0 {
			return string(append(dst, s...))
		}
	}
}

func parseFieldNumValue(s string) (float64, int32, error) {
	if len(s) == 0 {
		return 0, Field_Type_Unknown, fmt.Errorf("field value cannot be empty")
	}
	ch := s[len(s)-1]
	if ch == 'i' {
		// Integer value
		ss := s[:len(s)-1]
		n, err := fastfloat.ParseInt64(ss)
		if err != nil {
			return 0, Field_Type_Unknown, err
		}
		return float64(n), Field_Type_Int, nil
	}
	if ch == 'u' {
		// Unsigned integer value
		return 0, Field_Type_Unknown, fmt.Errorf("invalid number")
	}
	if ch == 'f' {
		// Unsigned integer value
		ss := s[:len(s)-1]
		n := fastfloat.ParseBestEffort(ss)
		return n, Field_Type_Float, nil
	}
	if s == "t" || s == "T" || s == "true" || s == "True" || s == "TRUE" {
		return 1, Field_Type_Boolean, nil
	}
	if s == "f" || s == "F" || s == "false" || s == "False" || s == "FALSE" {
		return 0, Field_Type_Boolean, nil
	}

	if !IsValidNumber(s) {
		return 0, Field_Type_Unknown, fmt.Errorf("invalid field value")
	}

	f := fastfloat.ParseBestEffort(s)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, Field_Type_Unknown, fmt.Errorf("invalid number")
	}

	return f, Field_Type_Float, nil
}

func parseFieldStrValue(s string) (string, error) {
	if len(s) == 0 {
		return "", fmt.Errorf("field value cannot be empty")
	}
	if s[0] == '"' {
		if len(s) < 2 || s[len(s)-1] != '"' {
			return "", fmt.Errorf("missing closing quote for quoted field value %s", s)
		}
		// Try converting quoted string to number, since sometimes Influx agents
		// send numbers as strings.
		s = s[1 : len(s)-1]
		return s, nil
	}
	return "", nil
}

func nextUnescapedChar(s string, ch byte, noEscapeChars bool) int {
	if noEscapeChars {
		// Fast path: just search for ch in s, since s has no escape chars.
		return strings.IndexByte(s, ch)
	}

	sOrig := s
again:
	n := strings.IndexByte(s, ch)
	if n < 0 {
		return -1
	}
	if n == 0 {
		return len(sOrig) - len(s) + n
	}
	if s[n-1] != '\\' {
		return len(sOrig) - len(s) + n
	}
	nOrig := n
	slashes := 0
	for n > 0 && s[n-1] == '\\' {
		slashes++
		n--
	}
	if slashes&1 == 0 {
		return len(sOrig) - len(s) + nOrig
	}
	s = s[nOrig+1:]
	goto again
}

func nextUnquotedChar(s string, ch byte, noEscapeChars, hasQuotedFields bool) int {
	if !hasQuotedFields {
		return nextUnescapedChar(s, ch, noEscapeChars)
	}
	sOrig := s
	for {
		n := nextUnescapedChar(s, ch, noEscapeChars)
		if n < 0 {
			return -1
		}
		if !isInQuote(s[:n], noEscapeChars) {
			return n + len(sOrig) - len(s)
		}
		s = s[n+1:]
		n = nextUnescapedChar(s, '"', noEscapeChars)
		if n < 0 {
			return -1
		}
		s = s[n+1:]
	}
}

func nextTimestamp(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return NoTimestamp, nil
	}
	for i := 0; i < len(s); i++ {
		// Timestamps should be integers, make sure they are, so we don't need
		// to actually parse the timestamp until needed.
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("bad timestamp")
		}
	}
	return fastfloat.ParseInt64(s)
}

func isInQuote(s string, noEscapeChars bool) bool {
	isQuote := false
	for {
		n := nextUnescapedChar(s, '"', noEscapeChars)
		if n < 0 {
			return isQuote
		}
		isQuote = !isQuote
		s = s[n+1:]
	}
}

func stripLeadingWhitespace(s string) string {
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	return s
}

type IndexOption struct {
	IndexList []uint16
	Oid       uint32
}

type IndexOptions []IndexOption

func (opt IndexOptions) Less(i, j int) bool {
	return opt[i].Oid < opt[j].Oid
}

func (opt IndexOptions) Len() int {
	return len(opt)
}

func (opt IndexOptions) Swap(i, j int) {
	opt[i], opt[j] = opt[j], opt[i]
}
