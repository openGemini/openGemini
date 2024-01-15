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

package meta

import (
	"bytes"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var escapeTable [256]byte

func init() {
	initEscapeTable()
}

func initEscapeTable() {
	escape := [][2]byte{
		{'a', '\a'}, {'b', '\b'}, {'f', '\f'}, {'n', '\n'}, {'r', '\r'}, {'t', '\t'}, {'v', '\v'},
		{'"', '"'}, {'\'', '\''}, {'\\', '\\'},
	}
	for i := 0; i < len(escape); i++ {
		escapeTable[escape[i][0]] = escape[i][1]
	}
}

func TransSplitChar(splitChar string) string {
	var buf bytes.Buffer
	i := 0
	inEscape := false
	for i < len(splitChar) {
		ch := splitChar[i]
		i++
		if !inEscape && ch == '\\' {
			inEscape = true
			continue
		}

		if !inEscape {
			buf.WriteByte(ch)
		} else {
			if escapeTable[ch] != 0 {
				buf.WriteByte(escapeTable[ch])
			} else {
				buf.WriteByte(ch)
			}
			inEscape = false
		}
	}
	return buf.String()
}

type Options struct {
	CaseInSensitive bool   `json:"case_insensitive"`
	AppendMeta      bool   `json:"append_meta"`
	WriteThreshold  int    `json:"write_threshold"`
	ReadThreshold   int    `json:"read_threshold"`
	StorageCapacity int    `json:"storage_capacity"`
	SplitChar       string `json:"split_char"`
	TagsSplit       string `json:"tag_split_char"`
	Ttl             int64  `json:"ttl"`
}

func (mo *Options) InitDefault() {
	mo.CaseInSensitive = false
	mo.Ttl = 0
	mo.WriteThreshold = 1
	mo.ReadThreshold = 1
	mo.StorageCapacity = 1
	mo.SplitChar = ""
	mo.TagsSplit = ""
	mo.AppendMeta = false
}

func (mo *Options) Marshal() *proto2.Options {
	if mo == nil {
		mo = &Options{}
	}
	return &proto2.Options{
		CaseInSensitive: proto.Bool(mo.CaseInSensitive),
		AppendMeta:      proto.Bool(mo.AppendMeta),
		WriteThreshold:  proto.Int(mo.WriteThreshold),
		ReadThreshold:   proto.Int(mo.ReadThreshold),
		StorageCapacity: proto.Int(mo.StorageCapacity),
		SplitChar:       proto.String(mo.SplitChar),
		TagsSplit:       proto.String(mo.TagsSplit),
		Ttl:             proto.Int64(mo.Ttl),
	}
}

func (mo *Options) Unmarshal(pb *proto2.Options) {
	mo.CaseInSensitive = pb.GetCaseInSensitive()
	mo.WriteThreshold = int(pb.GetWriteThreshold())
	mo.ReadThreshold = int(pb.GetReadThreshold())
	mo.StorageCapacity = int(pb.GetStorageCapacity())
	mo.SplitChar = pb.GetSplitChar()
	mo.TagsSplit = pb.GetTagsSplit()
	mo.AppendMeta = pb.GetAppendMeta()
	mo.Ttl = pb.GetTtl()
}

func (mo *Options) GetSplitChar() string {
	return TransSplitChar(mo.SplitChar)
}

func (mo *Options) GetTagSplitChar() string {
	return TransSplitChar(mo.TagsSplit)
}

type MeasurementInfo struct {
	Name          string // measurement name with version
	originName    string // cache original measurement name
	ShardKeys     []ShardKeyInfo
	Schema        map[string]int32
	IndexRelation influxql.IndexRelation
	ColStoreInfo  *ColStoreInfo
	MarkDeleted   bool
	EngineType    config.EngineType
	Options       *Options
	ObsOptions    *obs.ObsOptions // assign DatabaseInfo's ObsOptions to it when obatining MeasurementInfo
	tagKeysTotal  int
}

func NewMeasurementInfo(nameWithVer string) *MeasurementInfo {
	return &MeasurementInfo{
		Name:       nameWithVer,
		originName: influx.GetOriginMstName(nameWithVer),
		EngineType: config.TSSTORE,
	}
}

func (msti *MeasurementInfo) IsBlockCompact() bool {
	if msti.ColStoreInfo == nil {
		return false
	}
	return msti.ColStoreInfo.IsBlockCompact()
}

func (msti *MeasurementInfo) IsTimeSorted() bool {
	if msti.ColStoreInfo == nil || len(msti.ColStoreInfo.SortKey) == 0 {
		return false
	}
	return msti.ColStoreInfo.SortKey[0] == record.TimeField
}

func (msti *MeasurementInfo) IsDetachedWrite() bool {
	return msti.ObsOptions != nil
}

func (msti *MeasurementInfo) OriginName() string {
	return msti.originName
}

func (msti *MeasurementInfo) SetoriginName(originName string) {
	msti.originName = originName
}

func (msti *MeasurementInfo) walkSchema(fn func(fieldName string, fieldType int32)) {
	for fieldName := range msti.Schema {
		fn(fieldName, msti.Schema[fieldName])
	}
}

func (msti *MeasurementInfo) GetShardKey(ID uint64) *ShardKeyInfo {
	for i := len(msti.ShardKeys) - 1; i >= 0; i-- {
		if msti.ShardKeys[i].ShardGroup <= ID {
			return &msti.ShardKeys[i]
		}
	}
	return nil
}

func (msti *MeasurementInfo) marshal() *proto2.MeasurementInfo {
	pb := &proto2.MeasurementInfo{
		Name:        proto.String(msti.Name),
		MarkDeleted: proto.Bool(msti.MarkDeleted),
		EngineType:  proto.Uint32(uint32(msti.EngineType)),
	}

	if msti.ShardKeys != nil {
		pb.ShardKeys = make([]*proto2.ShardKeyInfo, len(msti.ShardKeys))
		for i := range msti.ShardKeys {
			pb.ShardKeys[i] = msti.ShardKeys[i].Marshal()
		}
	}

	if msti.Schema != nil {
		pb.Schema = make(map[string]int32, len(msti.Schema))
		for n, t := range msti.Schema {
			pb.Schema[n] = t
		}
	}

	pb.IndexRelation = EncodeIndexRelation(&msti.IndexRelation)
	if msti.ColStoreInfo != nil {
		pb.ColStoreInfo = msti.ColStoreInfo.Marshal()
	}

	if msti.Options != nil {
		pb.Options = msti.Options.Marshal()
	}
	if msti.ObsOptions != nil {
		pb.ObsOptions = MarshalObsOptions(msti.ObsOptions)
	}

	return pb
}

func (msti *MeasurementInfo) unmarshal(pb *proto2.MeasurementInfo) {
	msti.Name = pb.GetName()
	msti.originName = influx.GetOriginMstName(msti.Name)
	msti.MarkDeleted = pb.GetMarkDeleted()
	msti.EngineType = config.EngineType(pb.GetEngineType())
	if pb.GetShardKeys() != nil {
		msti.ShardKeys = make([]ShardKeyInfo, len(pb.GetShardKeys()))
		for i := range pb.GetShardKeys() {
			msti.ShardKeys[i].unmarshal(pb.GetShardKeys()[i])
		}
	}

	if len(pb.GetSchema()) > 0 {
		msti.Schema = make(map[string]int32, len(pb.GetSchema()))
	}

	for name, t := range pb.GetSchema() {
		msti.Schema[name] = t
		if t == influx.Field_Type_Tag {
			msti.tagKeysTotal++
		}
	}

	if pb.GetIndexRelation() != nil {
		msti.IndexRelation = *DecodeIndexRelation(pb.GetIndexRelation())
	}
	if pb.GetColStoreInfo() != nil {
		msti.ColStoreInfo = &ColStoreInfo{}
		msti.ColStoreInfo.Unmarshal(pb.GetColStoreInfo())
	}
	if pb.GetOptions() != nil {
		msti.Options = &Options{}
		msti.Options.Unmarshal(pb.GetOptions())
		msti.CompatibleForLogkeeper()
	}
	if pb.GetObsOptions() != nil {
		msti.ObsOptions = UnmarshalObsOptions(pb.GetObsOptions())
	}
}

func (msti *MeasurementInfo) MarshalBinary() ([]byte, error) {
	pb := msti.marshal()
	return proto.Marshal(pb)
}

func (msti *MeasurementInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.MeasurementInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	msti.unmarshal(pb)
	return nil
}

func (msti MeasurementInfo) clone() *MeasurementInfo {
	other := msti
	other.Schema = msti.cloneSchema()
	if msti.ShardKeys == nil {
		return &other
	}
	other.ShardKeys = make([]ShardKeyInfo, len(msti.ShardKeys))
	for i := range msti.ShardKeys {
		other.ShardKeys[i] = msti.ShardKeys[i].clone()
	}
	if msti.ColStoreInfo != nil {
		colStoreInfo := *msti.ColStoreInfo
		other.ColStoreInfo = &colStoreInfo
	}
	if msti.Options != nil {
		options := *msti.Options
		other.Options = &options
	}
	if msti.ObsOptions != nil {
		ObsOptions := *msti.ObsOptions
		other.ObsOptions = &ObsOptions
	}
	return &other
}

func (msti MeasurementInfo) cloneSchema() map[string]int32 {
	if msti.Schema == nil {
		return nil
	}

	schema := make(map[string]int32, len(msti.Schema))
	for name, info := range msti.Schema {
		schema[name] = info
	}
	return schema
}

func (msti MeasurementInfo) FieldKeys(ret map[string]map[string]int32) {
	for key := range msti.Schema {
		if msti.Schema[key] == influx.Field_Type_Tag {
			continue
		}
		ret[msti.OriginName()][key] = msti.Schema[key]
	}
}

func (msti MeasurementInfo) MatchTagKeys(cond influxql.Expr, ret map[string]map[string]struct{}) {
	for key, typ := range msti.Schema {
		if typ != influx.Field_Type_Tag {
			continue
		}
		valMap := map[string]interface{}{
			"_tagKey": key,
			"_name":   msti.OriginName(),
		}
		if cond == nil || influxql.EvalBool(cond, valMap) {
			ret[msti.Name][key] = struct{}{}
		}
	}
}

func (msti *MeasurementInfo) TagKeysTotal() int {
	return msti.tagKeysTotal
}

type MeasurementsInfo struct {
	MstsInfo []*MeasurementInfo
}

func (mstsi *MeasurementsInfo) marshal() *proto2.MeasurementsInfo {
	pb := &proto2.MeasurementsInfo{
		MeasurementsInfo: make([]*proto2.MeasurementInfo, len(mstsi.MstsInfo)),
	}

	for i := range mstsi.MstsInfo {
		pb.MeasurementsInfo[i] = mstsi.MstsInfo[i].marshal()
	}
	return pb
}

func (mstsi *MeasurementsInfo) unmarshal(pb *proto2.MeasurementsInfo) {
	mstsi.MstsInfo = make([]*MeasurementInfo, len(pb.GetMeasurementsInfo()))
	for i := range pb.MeasurementsInfo {
		mstsi.MstsInfo[i] = &MeasurementInfo{}
		mstsi.MstsInfo[i].unmarshal(pb.GetMeasurementsInfo()[i])
	}
}

func (mstsi *MeasurementsInfo) MarshalBinary() ([]byte, error) {
	pb := mstsi.marshal()
	return proto.Marshal(pb)
}

func (mstsi *MeasurementsInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.MeasurementsInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	mstsi.unmarshal(pb)
	return nil
}

// only useful in the logkeeper products
func (msti *MeasurementInfo) CompatibleForLogkeeper() {
	if !config.IsLogKeeper() || msti.Options == nil {
		return
	}
	contentSplit := msti.Options.GetSplitChar()
	if contentSplit == "" {
		contentSplit = tokenizer.CONTENT_SPLITTER
	}
	tagsSplitChar := msti.Options.GetTagSplitChar()
	if tagsSplitChar == "" {
		tagsSplitChar = tokenizer.TAGS_SPLITTER_BEFORE
	}

	msti.IndexRelation = influxql.IndexRelation{
		Rid:        0,
		Oids:       []uint32{4}, // bloomfilter oid
		IndexNames: []string{"bloomfilter"},
		IndexList: []*influxql.IndexList{
			{
				IList: []string{"tags", "content"},
			},
		},
		IndexOptions: map[string][]*influxql.IndexOption{
			"tags": {
				{Tokens: tagsSplitChar, Tokenizers: "standard"},
			},
			"content": {
				{Tokens: contentSplit, Tokenizers: "standard"},
			},
		},
	}
}

type ShardKeyInfo struct {
	ShardKey   []string
	Type       string
	ShardGroup uint64
}

func (ski *ShardKeyInfo) EqualsToAnother(other *ShardKeyInfo) bool {
	if len(ski.ShardKey) != len(other.ShardKey) {
		return false
	}

	if ski.Type != other.Type {
		return false
	}

	// shardKey is sorted
	for i := range ski.ShardKey {
		if ski.ShardKey[i] != other.ShardKey[i] {
			return false
		}
	}

	return true
}

func (ski *ShardKeyInfo) Marshal() *proto2.ShardKeyInfo {
	pb := &proto2.ShardKeyInfo{ShardKey: ski.ShardKey, Type: proto.String(ski.Type)}
	if ski.ShardGroup > 0 {
		pb.SgID = proto.Uint64(ski.ShardGroup)
	}
	return pb
}

func (ski *ShardKeyInfo) unmarshal(pb *proto2.ShardKeyInfo) {
	ski.ShardKey = pb.GetShardKey()
	ski.Type = pb.GetType()
	if pb.GetSgID() > 0 {
		ski.ShardGroup = pb.GetSgID()
	}
}

func (ski ShardKeyInfo) clone() ShardKeyInfo {
	if ski.ShardKey == nil {
		return ski
	}

	shardKey := make([]string, len(ski.ShardKey))
	copy(shardKey, ski.ShardKey)

	ski.ShardKey = shardKey
	return ski
}

func (msti *MeasurementInfo) ContainIndexRelation(ID uint64) bool {
	return true
}

func (msti *MeasurementInfo) GetIndexRelation() influxql.IndexRelation {
	return msti.IndexRelation
}

func (msti *MeasurementInfo) FindMstInfos(dataTypes []int64) []*MeasurementTypeFields {
	infos := make([]*MeasurementTypeFields, 0, len(dataTypes))
	for _, d := range dataTypes {
		info := &MeasurementTypeFields{
			Fields: make([]string, 0),
		}
		switch influxql.DataType(d) {
		case influxql.Float:
			info.Type = int64(influxql.Float)
			for name, ty := range msti.Schema {
				if ty == influx.Field_Type_Float {
					info.Fields = append(info.Fields, name)
				}
			}
		case influxql.Integer:
			info.Type = int64(influxql.Integer)
			for name, ty := range msti.Schema {
				if ty == influx.Field_Type_Int {
					info.Fields = append(info.Fields, name)
				}
			}
		case influxql.String:
			info.Type = int64(influxql.String)
			for name, ty := range msti.Schema {
				if ty == influx.Field_Type_String {
					info.Fields = append(info.Fields, name)
				}
			}
		case influxql.Boolean:
			info.Type = int64(influxql.Boolean)
			for name, ty := range msti.Schema {
				if ty == influx.Field_Type_Boolean {
					info.Fields = append(info.Fields, name)
				}
			}
		}
		if len(info.Fields) > 0 {
			infos = append(infos, info)
		}
	}
	return infos
}

func EncodeIndexOption(o *influxql.IndexOption) *proto2.IndexOption {
	pb := &proto2.IndexOption{
		Tokens:              proto.String(o.Tokens),
		Tokenizers:          proto.String(o.Tokenizers),
		TimeClusterDuration: proto.Int64(int64(o.TimeClusterDuration)),
	}

	return pb
}

func DecodeIndexOption(pb *proto2.IndexOption) *influxql.IndexOption {
	o := &influxql.IndexOption{
		Tokens:              pb.GetTokens(),
		Tokenizers:          pb.GetTokenizers(),
		TimeClusterDuration: time.Duration(pb.GetTimeClusterDuration()),
	}
	return o
}

func EncodeIndexRelation(indexR *influxql.IndexRelation) *proto2.IndexRelation {
	pb := &proto2.IndexRelation{Rid: proto.Uint32(indexR.Rid),
		Oid:       indexR.Oids,
		IndexName: indexR.IndexNames}

	pb.IndexLists = make([]*proto2.IndexList, len(indexR.IndexList))
	for i, IList := range indexR.IndexList {
		indexList := &proto2.IndexList{
			IList: IList.IList,
		}
		pb.IndexLists[i] = indexList
	}
	if indexR.IndexOptions != nil {
		pb.IndexOptions = make(map[string]*proto2.IndexOptions, len(indexR.IndexOptions))
		for i, indexOptions := range indexR.IndexOptions {
			if indexOptions != nil {
				pb.IndexOptions[i] = &proto2.IndexOptions{
					Infos: make([]*proto2.IndexOption, len(indexOptions)),
				}
				for j, o := range indexOptions {
					pb.IndexOptions[i].Infos[j] = EncodeIndexOption(o)
				}
			}
		}
	}

	return pb
}

func DecodeIndexRelation(pb *proto2.IndexRelation) *influxql.IndexRelation {
	indexR := &influxql.IndexRelation{}
	indexR.Rid = pb.GetRid()
	indexR.Oids = pb.GetOid()
	indexR.IndexNames = pb.GetIndexName()
	indexLists := pb.GetIndexLists()
	indexR.IndexList = make([]*influxql.IndexList, len(indexLists))
	for i, iList := range indexLists {
		indexR.IndexList[i] = &influxql.IndexList{
			IList: iList.GetIList(),
		}
	}
	indexOptions := pb.GetIndexOptions()
	if indexOptions != nil {
		indexR.IndexOptions = make(map[string][]*influxql.IndexOption, len(indexOptions))
		for i, idxOptions := range indexOptions {
			infos := idxOptions.GetInfos()
			if infos != nil {
				indexR.IndexOptions[i] = make([]*influxql.IndexOption, len(infos))
				for j, o := range infos {
					indexR.IndexOptions[i][j] = DecodeIndexOption(o)
				}
			}
		}
	}
	return indexR
}

type ColStoreInfo struct {
	PrimaryKey          []string
	SortKey             []string
	PropertyKey         []string
	PropertyValue       []string
	TimeClusterDuration time.Duration
	CompactionType      config.CompactionType
}

func NewColStoreInfo(PrimaryKey []string, SortKey []string, Property [][]string, Duration time.Duration,
	CompactType string) *ColStoreInfo {
	h := &ColStoreInfo{
		PrimaryKey:          PrimaryKey,
		SortKey:             SortKey,
		TimeClusterDuration: Duration,
		CompactionType:      config.Str2CompactionType(CompactType),
	}
	if Property != nil {
		h.PropertyKey = Property[0]
		h.PropertyValue = Property[1]
	}
	return h
}

func (h *ColStoreInfo) IsBlockCompact() bool {
	return h.CompactionType == config.BLOCK
}

func (h *ColStoreInfo) Marshal() *proto2.ColStoreInfo {
	pb := &proto2.ColStoreInfo{
		PrimaryKey:          h.PrimaryKey,
		SortKey:             h.SortKey,
		PropertyKey:         h.PropertyKey,
		PropertyValue:       h.PropertyValue,
		TimeClusterDuration: (*int64)(&h.TimeClusterDuration),
		CompactionType:      (*int32)(&h.CompactionType),
	}
	return pb
}

func (h *ColStoreInfo) Unmarshal(pb *proto2.ColStoreInfo) {
	h.PrimaryKey = pb.GetPrimaryKey()
	h.SortKey = pb.GetSortKey()
	h.PropertyKey = pb.GetPropertyKey()
	h.PropertyValue = pb.GetPropertyValue()
	h.TimeClusterDuration = time.Duration(pb.GetTimeClusterDuration())
	h.CompactionType = config.CompactionType(pb.GetCompactionType())
}

func NewSchemaInfo(tags, fields map[string]int32) []*proto2.FieldSchema {
	if len(tags)+len(fields) == 0 {
		return nil
	}
	var s []*proto2.FieldSchema
	for k, v := range tags {
		s = append(s, &proto2.FieldSchema{FieldName: proto.String(k), FieldType: proto.Int32(v)})
	}
	for k, v := range fields {
		s = append(s, &proto2.FieldSchema{FieldName: proto.String(k), FieldType: proto.Int32(v)})
	}
	return s
}
