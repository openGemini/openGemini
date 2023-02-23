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
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	internal "github.com/openGemini/openGemini/open_src/influx/query/proto"
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
		TraceId:               opt.Traceid,
		SeriesKey:             opt.SeriesKey,
		GroupByAllDims:        opt.GroupByAllDims,
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
		Traceid:               pb.GetTraceId(),
		SeriesKey:             pb.GetSeriesKey(),
		GroupByAllDims:        pb.GetGroupByAllDims(),
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

	return opt, nil
}

func encodeMeasurement(mm *influxql.Measurement) *internal.Measurement {
	pb := &internal.Measurement{
		Database:        mm.Database,
		RetentionPolicy: mm.RetentionPolicy,
		Name:            mm.Name,
		SystemIterator:  mm.SystemIterator,
		IsTarget:        mm.IsTarget,
	}
	if mm.Regex != nil {
		pb.Regex = mm.Regex.Val.String()
	}
	return pb
}

func decodeMeasurement(pb *internal.Measurement) (*influxql.Measurement, error) {
	mm := &influxql.Measurement{
		Database:        pb.GetDatabase(),
		RetentionPolicy: pb.GetRetentionPolicy(),
		Name:            pb.GetName(),
		SystemIterator:  pb.GetSystemIterator(),
		IsTarget:        pb.GetIsTarget(),
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

func EncodeQuerySchema(schema hybridqp.Catalog) *internal.QuerySchema {
	if schema == nil {
		return nil
	}

	pb := &internal.QuerySchema{
		ColumnNames: schema.GetColumnNames(),
		QueryFields: schema.GetQueryFields().String(),
		Opt:         encodeProcessorOptions(schema.Options().(*ProcessorOptions)),
	}

	return pb
}

func DecodeQuerySchema(pb *internal.QuerySchema) (hybridqp.Catalog, error) {
	if pb == nil {
		return nil, nil
	}

	queryFields, err := hybridqp.ParseFields(pb.QueryFields)
	if err != nil {
		return nil, err
	}

	opt, err := decodeProcessorOptions(pb.Opt)
	if err != nil {
		return nil, err
	}

	schema := hybridqp.GetCatalogFactoryInstance().Create(queryFields, pb.ColumnNames, opt)

	return schema, nil
}
