package query

/*
Copyright (c) 2018 InfluxData

This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/iterator.go


2022.01.23 remove other codes except for encode functions and decode functions for Options, Measurement, Internal, VarRef
2022.01.23 Add encode and decode functions for QuerySchema.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"fmt"
	"regexp"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	internal "github.com/openGemini/openGemini/lib/util/lifted/influx/query/proto"
	"google.golang.org/protobuf/proto"
)

// MarshalBinary encodes opt into a binary format.
func (opt *ProcessorOptions) MarshalBinary() ([]byte, error) {
	return proto.Marshal(encodeProcessorOptions(opt))
}

// UnmarshalBinary decodes from a binary format in to opt.
func (opt *ProcessorOptions) UnmarshalBinary(buf []byte) error {
	var pb internal.ProcessorOptions
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	other, err := decodeProcessorOptions(&pb)
	if err != nil {
		return err
	}
	*opt = *other

	return nil
}

func encodeProcessorOptions(opt *ProcessorOptions) *internal.ProcessorOptions {
	mc := hybridqp.MapConvert{}

	pb := &internal.ProcessorOptions{
		Name:                  opt.Name,
		Aux:                   encodeVarRefs(opt.Aux),
		Interval:              encodeInterval(opt.Interval),
		Dimensions:            opt.Dimensions,
		Fill:                  int32(opt.Fill),
		StartTime:             opt.StartTime,
		EndTime:               opt.EndTime,
		Ascending:             opt.Ascending,
		Limit:                 int64(opt.Limit),
		Offset:                int64(opt.Offset),
		SLimit:                int64(opt.SLimit),
		SOffset:               int64(opt.SOffset),
		StripName:             opt.StripName,
		Dedupe:                opt.Dedupe,
		MaxSeriesN:            int64(opt.MaxSeriesN),
		Ordered:               opt.Ordered,
		GroupBy:               mc.StructToBool(opt.GroupBy),
		ChunkSize:             int64(opt.ChunkSize),
		MaxParallel:           int64(opt.MaxParallel),
		Query:                 opt.Query,
		HintType:              int64(opt.HintType),
		EnableBinaryTreeMerge: opt.EnableBinaryTreeMerge,
		QueryId:               opt.QueryId,
		SeriesKey:             opt.SeriesKey,
		GroupByAllDims:        opt.GroupByAllDims,
		HasFieldWildcard:      opt.HasFieldWildcard,
		LogQueryCurrId:        opt.LogQueryCurrId,
		IncQuery:              opt.IncQuery,
		IterID:                opt.IterID,
	}

	// Set expression, if set.
	if opt.Expr != nil {
		pb.Expr = opt.Expr.String()
	}

	// Set the location, if set.
	if opt.Location != nil {
		pb.Location = opt.Location.String()
	}

	// Convert and encode sources to measurements.
	if opt.Sources != nil {
		sources := make([]*internal.Measurement, len(opt.Sources))
		for i, source := range opt.Sources {
			mm := source.(*influxql.Measurement)
			sources[i] = encodeMeasurement(mm)
		}
		pb.Sources = sources
	}

	// Fill value can only be a number. Set it if available.
	if v, ok := opt.FillValue.(float64); ok {
		pb.FillValue = v
	}

	// Set condition, if set.
	if opt.Condition != nil {
		pb.Condition = opt.Condition.String()
	}

	// set the sort fields
	if len(opt.SortFields) > 0 {
		pb.SortFields = opt.SortFields.String()
	}

	return pb
}

func decodeProcessorOptions(pb *internal.ProcessorOptions) (*ProcessorOptions, error) {
	mc := hybridqp.MapConvert{}
	opt := &ProcessorOptions{
		Name:                  pb.Name,
		Aux:                   decodeVarRefs(pb.Aux),
		Interval:              decodeInterval(pb.GetInterval()),
		Dimensions:            pb.GetDimensions(),
		Fill:                  influxql.FillOption(pb.GetFill()),
		StartTime:             pb.GetStartTime(),
		EndTime:               pb.GetEndTime(),
		Ascending:             pb.GetAscending(),
		Limit:                 int(pb.GetLimit()),
		Offset:                int(pb.GetOffset()),
		SLimit:                int(pb.GetSLimit()),
		SOffset:               int(pb.GetSOffset()),
		StripName:             pb.GetStripName(),
		Dedupe:                pb.GetDedupe(),
		MaxSeriesN:            int(pb.GetMaxSeriesN()),
		Ordered:               pb.GetOrdered(),
		GroupBy:               mc.BoolToStruct(pb.GroupBy),
		ChunkSize:             int(pb.GetChunkSize()),
		MaxParallel:           int(pb.GetMaxParallel()),
		Query:                 pb.GetQuery(),
		HintType:              hybridqp.HintType(pb.HintType),
		EnableBinaryTreeMerge: pb.GetEnableBinaryTreeMerge(),
		QueryId:               pb.GetQueryId(),
		SeriesKey:             pb.GetSeriesKey(),
		GroupByAllDims:        pb.GetGroupByAllDims(),
		HasFieldWildcard:      pb.HasFieldWildcard,
		LogQueryCurrId:        pb.LogQueryCurrId,
		IncQuery:              pb.IncQuery,
		IterID:                pb.IterID,
	}

	// Set expression, if set.
	if pb.Expr != "" {
		expr, err := influxql.ParseExpr(pb.GetExpr())
		if err != nil {
			return nil, err
		}
		opt.Expr = expr
	}

	if pb.Location != "" {
		loc, err := time.LoadLocation(pb.GetLocation())
		if err != nil {
			return nil, err
		}
		opt.Location = loc
	}

	// Convert and decode sources to measurements.
	if pb.Sources != nil {
		sources := make([]influxql.Source, len(pb.GetSources()))
		for i, source := range pb.GetSources() {
			mm, err := decodeMeasurement(source)
			if err != nil {
				return nil, err
			}
			sources[i] = mm
		}
		opt.Sources = sources
	}

	// Set the fill value, if set.
	opt.FillValue = pb.GetFillValue()

	// Set condition, if set.
	if pb.Condition != "" {
		expr, err := influxql.ParseExpr(pb.GetCondition())
		if err != nil {
			return nil, err
		}
		opt.Condition = expr
	}

	if pb.SortFields != "" {
		sortFields, err := influxql.ParseSortFields(pb.GetSortFields())
		if err != nil {
			return nil, err
		}
		opt.SortFields = sortFields
	}

	return opt, nil
}

func encodeMeasurement(mm *influxql.Measurement) *internal.Measurement {
	pb := &internal.Measurement{
		Database:        mm.Database,
		RetentionPolicy: mm.RetentionPolicy,
		Name:            mm.Name,
		SystemIterator:  mm.SystemIterator,
		IsTarget:        mm.IsTarget,
		EngineType:      uint32(mm.EngineType),
		IsTimeSorted:    mm.IsTimeSorted,
	}

	if mm.IndexRelation != nil {
		pb.IndexRelation = encodeIndexRelation(mm.IndexRelation)
	}

	if mm.ObsOptions != nil {
		pb.ObsOptions = encodeObsOptions(mm.ObsOptions)
	}

	if mm.Regex != nil {
		pb.Regex = mm.Regex.Val.String()
	}
	return pb
}

func encodeIndexOption(io *influxql.IndexOption) *internal.IndexOption {
	pb := &internal.IndexOption{
		Tokens:              io.Tokens,
		Tokenizers:          io.Tokenizers,
		TimeClusterDuration: int64(io.TimeClusterDuration),
	}
	return pb
}

func encodeIndexRelation(indexR *influxql.IndexRelation) *internal.IndexRelation {
	pb := &internal.IndexRelation{
		Rid:        indexR.Rid,
		Oids:       indexR.Oids,
		IndexNames: indexR.IndexNames,
	}

	pb.IndexLists = make([]*internal.IndexList, len(indexR.IndexList))
	for i, IList := range indexR.IndexList {
		indexList := &internal.IndexList{
			IList: IList.IList,
		}
		pb.IndexLists[i] = indexList
	}

	pb.IndexOptions = make([]*internal.IndexOptions, len(indexR.IndexOptions))
	for i, indexOptions := range indexR.IndexOptions {
		if indexOptions != nil {
			pb.IndexOptions[i] = &internal.IndexOptions{
				Infos: make([]*internal.IndexOption, len(indexOptions.Options)),
			}
			for j, o := range indexOptions.Options {
				pb.IndexOptions[i].Infos[j] = encodeIndexOption(o)
			}
		}
	}

	return pb
}

func encodeObsOptions(opt *obs.ObsOptions) *internal.ObsOptions {
	return &internal.ObsOptions{
		Enabled:    opt.Enabled,
		BucketName: opt.BucketName,
		Ak:         opt.Ak,
		Sk:         opt.Sk,
		Endpoint:   opt.Endpoint,
		BasePath:   opt.BasePath,
	}
}

func decodeMeasurement(pb *internal.Measurement) (*influxql.Measurement, error) {
	mm := &influxql.Measurement{
		Database:        pb.GetDatabase(),
		RetentionPolicy: pb.GetRetentionPolicy(),
		Name:            pb.GetName(),
		SystemIterator:  pb.GetSystemIterator(),
		IsTarget:        pb.GetIsTarget(),
		EngineType:      config.EngineType(pb.GetEngineType()),
		IsTimeSorted:    pb.GetIsTimeSorted(),
	}

	if pb.GetIndexRelation() != nil {
		mm.IndexRelation = decodeIndexRelation(pb.GetIndexRelation())
	}

	if pb.GetObsOptions() != nil {
		mm.ObsOptions = decodeObsOptions(pb.GetObsOptions())
	}

	if pb.Regex != "" {
		regex, err := regexp.Compile(pb.GetRegex())
		if err != nil {
			return nil, fmt.Errorf("invalid binary measurement regex: value=%q, err=%s", pb.GetRegex(), err)
		}
		mm.Regex = &influxql.RegexLiteral{Val: regex}
	}

	return mm, nil
}

func decodeIndexOption(pb *internal.IndexOption) *influxql.IndexOption {
	io := &influxql.IndexOption{
		Tokens:              pb.GetTokens(),
		Tokenizers:          pb.GetTokenizers(),
		TimeClusterDuration: time.Duration(pb.GetTimeClusterDuration()),
	}
	return io
}

func decodeIndexRelation(pb *internal.IndexRelation) *influxql.IndexRelation {
	indexR := &influxql.IndexRelation{}
	indexR.Rid = pb.GetRid()
	indexR.Oids = pb.GetOids()
	indexR.IndexNames = pb.GetIndexNames()
	indexLists := pb.GetIndexLists()
	indexR.IndexList = make([]*influxql.IndexList, len(indexLists))
	for i, iList := range indexLists {
		indexR.IndexList[i] = &influxql.IndexList{
			IList: iList.GetIList(),
		}
	}
	indexOptions := pb.GetIndexOptions()
	indexR.IndexOptions = make([]*influxql.IndexOptions, len(indexOptions))
	for i, idxOptions := range indexOptions {
		if idxOptions != nil {
			infos := idxOptions.GetInfos()
			indexR.IndexOptions[i] = &influxql.IndexOptions{
				Options: make([]*influxql.IndexOption, len(infos)),
			}
			for j, o := range infos {
				indexR.IndexOptions[i].Options[j] = decodeIndexOption(o)
			}
		}
	}

	return indexR
}

func decodeObsOptions(pb *internal.ObsOptions) *obs.ObsOptions {
	return &obs.ObsOptions{
		Enabled:    pb.GetEnabled(),
		BucketName: pb.GetBucketName(),
		Endpoint:   pb.GetEndpoint(),
		Ak:         pb.GetAk(),
		Sk:         pb.GetSk(),
		BasePath:   pb.GetBasePath(),
	}
}

func encodeInterval(i hybridqp.Interval) *internal.Interval {
	return &internal.Interval{
		Duration: i.Duration.Nanoseconds(),
		Offset:   i.Offset.Nanoseconds(),
	}
}

func decodeInterval(pb *internal.Interval) hybridqp.Interval {
	return hybridqp.Interval{
		Duration: time.Duration(pb.GetDuration()),
		Offset:   time.Duration(pb.GetOffset()),
	}
}

func encodeVarRefs(refs influxql.VarRefs) []*internal.VarRef {
	ret := make([]*internal.VarRef, 0, len(refs))
	for _, ref := range refs {
		ret = append(ret, encodeVarRef(ref))
	}
	return ret
}

func decodeVarRefs(pb []*internal.VarRef) influxql.VarRefs {
	ret := make(influxql.VarRefs, 0, len(pb))
	for _, ref := range pb {
		ret = append(ret, decodeVarRef(ref))
	}
	return ret
}

func encodeVarRef(ref influxql.VarRef) *internal.VarRef {
	return &internal.VarRef{
		Val:  ref.Val,
		Type: int32(ref.Type),
	}
}

func decodeVarRef(pb *internal.VarRef) influxql.VarRef {
	return influxql.VarRef{
		Val:  pb.GetVal(),
		Type: influxql.DataType(pb.GetType()),
	}
}

func encodeUnnests(iUnnests influxql.Unnests) []*internal.Unnest {
	unnests := make([]*internal.Unnest, len(iUnnests))
	for i, u := range iUnnests {
		unnest := &internal.Unnest{
			Expr:    u.Expr.String(),
			Aliases: u.Aliases,
		}
		unnest.DstType = make([]int32, len(u.DstType))
		for j, t := range u.DstType {
			unnest.DstType[j] = int32(t)
		}
		unnests[i] = unnest
	}
	return unnests
}

func EncodeQuerySchema(schema hybridqp.Catalog) *internal.QuerySchema {
	if schema == nil {
		return nil
	}

	pb := &internal.QuerySchema{
		ColumnNames: schema.GetColumnNames(),
		QueryFields: schema.GetQueryFields().String(),
		Unnests:     encodeUnnests(schema.GetUnnests()),
	}

	return pb
}

func decodeUnnests(iUnnests []*internal.Unnest) ([]*influxql.Unnest, error) {
	unnests := make([]*influxql.Unnest, len(iUnnests))
	for i, u := range iUnnests {
		unnest := &influxql.Unnest{
			Aliases: u.GetAliases(),
		}

		expr, err := influxql.ParseExpr(u.Expr)
		if err != nil {
			return nil, err
		}
		unnest.Expr = expr

		dstType := u.GetDstType()
		unnest.DstType = make([]influxql.DataType, len(dstType))
		for j, t := range dstType {
			unnest.DstType[j] = influxql.DataType(t)
		}
		unnests[i] = unnest
	}
	return unnests, nil
}

func DecodeQuerySchema(pb *internal.QuerySchema, opt hybridqp.Options) (hybridqp.Catalog, error) {
	if pb == nil {
		return nil, nil
	}

	queryFields, err := hybridqp.ParseFields(pb.QueryFields)
	if err != nil {
		return nil, err
	}

	unnests, err := decodeUnnests(pb.Unnests)
	if err != nil {
		return nil, err
	}

	schema := hybridqp.GetCatalogFactoryInstance().Create(queryFields, pb.ColumnNames, opt)
	schema.SetUnnests(unnests)

	return schema, nil
}
