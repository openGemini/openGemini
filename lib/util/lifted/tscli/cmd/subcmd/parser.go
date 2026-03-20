package subcmd

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/opengemini-client-go/opengemini"
)

type parser interface {
	parse2SeriesKeyWithoutVersion(key []byte, dst []byte, splitWithNull bool, point *opengemini.Point) ([]byte, error)
	appendFields(rec record.Record, buf []byte, point *opengemini.Point) ([]byte, error)
	writeMstInfoFromTssp(metaWriter io.Writer, outputWriter io.Writer, filePath string, isOrder bool, index *tsi.MergeSetIndex) error
	writeMstInfoFromWal(metaWriter io.Writer, outputWriter io.Writer, row influx.Row, curDatabase string) error
	writeMetaInfo(metaWriter io.Writer, infoType InfoType, info string)
	writeOutputInfo(outputWriter io.Writer, info string)
	getRowBuf(buf []byte, measurementName string, row influx.Row, point *opengemini.Point) ([]byte, error)
}

type txtParser struct{}

func newTxtParser() *txtParser {
	return &txtParser{}
}

// parse2SeriesKeyWithoutVersion parse encoded index key to line protocol series key,without version and escape special characters
// encoded index key format: [total len][ms len][ms][tagkey1 len][tagkey1 val]...]
// parse to line protocol format: mst,tagkey1=tagval1,tagkey2=tagval2...
func (t *txtParser) parse2SeriesKeyWithoutVersion(key []byte, dst []byte, splitWithNull bool, point *opengemini.Point) ([]byte, error) {
	msName, src, err := influx.MeasurementName(key)
	originMstName := influx.GetOriginMstName(string(msName))
	originMstName = EscapeMstName(originMstName)
	if err != nil {
		return []byte{}, err
	}
	var split [2]byte
	if splitWithNull {
		split[0], split[1] = influx.ByteSplit, influx.ByteSplit
	} else {
		split[0], split[1] = '=', ','
	}
	point.Measurement = originMstName
	dst = append(dst, originMstName...)
	dst = append(dst, ',')
	tagsN := encoding.UnmarshalUint16(src)
	src = src[2:]
	var i uint16
	for i = 0; i < tagsN; i++ {
		keyLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		tagKey := EscapeTagKey(string(src[:keyLen]))
		dst = append(dst, tagKey...)
		dst = append(dst, split[0])
		src = src[keyLen:]

		valLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		tagVal := EscapeTagValue(string(src[:valLen]))
		dst = append(dst, tagVal...)
		dst = append(dst, split[1])
		src = src[valLen:]

		point.AddTag(tagKey, tagVal)
	}
	return dst[:len(dst)-1], nil
}

func (t *txtParser) appendFields(rec record.Record, buf []byte, point *opengemini.Point) ([]byte, error) {
	buf = append(buf, ' ')
	for i, field := range rec.Schema {
		if field.Name == "time" {
			continue
		}
		buf = append(buf, EscapeFieldKey(field.Name)+"="...)
		switch field.Type {
		case influx.Field_Type_Float:
			buf = strconv.AppendFloat(buf, rec.Column(i).FloatValues()[0], 'g', -1, 64)
			point.AddField(EscapeFieldKey(field.Name), strconv.FormatFloat(rec.Column(i).FloatValues()[0], 'g', -1, 64))
		case influx.Field_Type_Int:
			buf = strconv.AppendInt(buf, rec.Column(i).IntegerValues()[0], 10)
			point.AddField(EscapeFieldKey(field.Name), strconv.FormatInt(rec.Column(i).IntegerValues()[0], 10))
		case influx.Field_Type_Boolean:
			buf = strconv.AppendBool(buf, rec.Column(i).BooleanValues()[0])
			point.AddField(EscapeFieldKey(field.Name), strconv.FormatBool(rec.Column(i).BooleanValues()[0]))
		case influx.Field_Type_String:
			var str []string
			str = rec.Column(i).StringValues(str)
			buf = append(buf, '"')
			buf = append(buf, EscapeStringFieldValue(str[0])...)
			buf = append(buf, '"')
			point.AddField(EscapeFieldKey(field.Name), str[0])
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", rec.Column(i))...)
			point.AddField(EscapeFieldKey(field.Name), fmt.Sprintf("%v", rec.Column(i)))
		}
		if i != rec.Len()-2 {
			buf = append(buf, ',')
		} else {
			buf = append(buf, ' ')
		}
	}
	buf = strconv.AppendInt(buf, rec.Times()[0], 10)
	buf = append(buf, '\n')
	point.Timestamp = rec.Times()[0] // point.Time = time.Unix(0, rec.Times()[0])
	return buf, nil
}

func (t *txtParser) writeMstInfoFromTssp(_ io.Writer, _ io.Writer, _ string, _ bool, _ *tsi.MergeSetIndex) error {
	return nil
}

func (t *txtParser) writeMstInfoFromWal(_ io.Writer, _ io.Writer, _ influx.Row, _ string) error {
	return nil
}

func (t *txtParser) getRowBuf(buf []byte, measurementName string, row influx.Row, point *opengemini.Point) ([]byte, error) {
	point.Measurement = measurementName
	tags := row.Tags
	fields := row.Fields
	tm := row.Timestamp

	buf = append(buf, measurementName...)
	buf = append(buf, ',')
	for i, tag := range tags {
		buf = append(buf, EscapeTagKey(tag.Key)+"="...)
		buf = append(buf, EscapeTagValue(tag.Value)...)
		if i != len(tags)-1 {
			buf = append(buf, ',')
		} else {
			buf = append(buf, ' ')
		}
		point.AddTag(EscapeTagKey(tag.Key), EscapeTagValue(tag.Value))
	}
	for i, field := range fields {
		buf = append(buf, EscapeFieldKey(field.Key)+"="...)
		switch field.Type {
		case influx.Field_Type_Float:
			buf = strconv.AppendFloat(buf, field.NumValue, 'g', -1, 64)
			point.AddField(EscapeFieldKey(field.Key), strconv.FormatFloat(field.NumValue, 'g', -1, 64))
		case influx.Field_Type_Int:
			buf = strconv.AppendInt(buf, int64(field.NumValue), 10)
			point.AddField(EscapeFieldKey(field.Key), strconv.FormatInt(int64(field.NumValue), 10))
		case influx.Field_Type_Boolean:
			buf = strconv.AppendBool(buf, field.NumValue == 1)
			point.AddField(EscapeFieldKey(field.Key), strconv.FormatBool(field.NumValue == 1))
		case influx.Field_Type_String:
			buf = append(buf, '"')
			buf = append(buf, EscapeStringFieldValue(field.StrValue)...)
			buf = append(buf, '"')
			point.AddField(EscapeFieldKey(field.Key), field.StrValue)
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", field)...)
			point.AddField(EscapeFieldKey(field.Key), fmt.Sprintf("%v", field))
		}
		if i != len(fields)-1 {
			buf = append(buf, ',')
		} else {
			buf = append(buf, ' ')
		}
	}
	buf = strconv.AppendInt(buf, tm, 10)
	buf = append(buf, '\n')
	point.Timestamp = tm // point.Time = time.Unix(0, tm)
	return buf, nil
}

type InfoType int

const (
	InfoTypeDatabase InfoType = 1 + iota
	InfoTypeRetentionPolicy
	InfoTypeMeasurement
)

func (t *txtParser) writeMetaInfo(metaWriter io.Writer, infoType InfoType, info string) {
	switch infoType {
	case InfoTypeDatabase:
		fmt.Fprintf(metaWriter, "# CONTEXT-DATABASE: %s\n", info)
	case InfoTypeRetentionPolicy:
		fmt.Fprintf(metaWriter, "# CONTEXT-RETENTION-POLICY: %s\n", info)
	case InfoTypeMeasurement:
		fmt.Fprintf(metaWriter, "# CONTEXT-MEASUREMENT: %s\n", info)
	default:
		fmt.Fprintf(metaWriter, "%s\n", info)
	}
}

func (t *txtParser) writeOutputInfo(outputWriter io.Writer, info string) {
	fmt.Fprint(outputWriter, info)
}

type csvParser struct {
	fieldsName     map[string]map[string][]string // database -> measurement -> []field
	curDatabase    string
	curMeasurement string
}

func newCsvParser() *csvParser {
	return &csvParser{
		fieldsName: make(map[string]map[string][]string),
	}
}

// parse2SeriesKeyWithoutVersion parse encoded index key to csv series key,without version and escape special characters
// encoded index key format: [total len][ms len][ms][tagkey1 len][tagkey1 val]...]
// parse to csv format: mst,tagval1,tagval2...
func (c *csvParser) parse2SeriesKeyWithoutVersion(key []byte, dst []byte, splitWithNull bool, _ *opengemini.Point) ([]byte, error) {
	_, src, err := influx.MeasurementName(key)
	if err != nil {
		return []byte{}, err
	}
	var split [2]byte
	if splitWithNull {
		split[0], split[1] = influx.ByteSplit, influx.ByteSplit
	} else {
		split[0], split[1] = '=', ','
	}

	tagsN := encoding.UnmarshalUint16(src)
	src = src[2:]
	var i uint16
	for i = 0; i < tagsN; i++ {
		keyLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		src = src[keyLen:]

		valLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		tagVal := EscapeTagValue(string(src[:valLen]))
		dst = append(dst, tagVal...)
		dst = append(dst, split[1])
		src = src[valLen:]
	}
	return dst, nil

}

func (c *csvParser) appendFields(rec record.Record, buf []byte, _ *opengemini.Point) ([]byte, error) {
	curFieldsName := c.fieldsName[c.curDatabase][c.curMeasurement]
	for _, fieldName := range curFieldsName {
		if fieldName == "time" {
			continue
		}
		k, ok := getFieldNameIndexFromRecord(rec.Schema, fieldName)
		if !ok {
			buf = append(buf, ',')
		} else {
			switch rec.Schema[k].Type {
			case influx.Field_Type_Float:
				buf = strconv.AppendFloat(buf, rec.Column(k).FloatValues()[0], 'g', -1, 64)
			case influx.Field_Type_Int:
				buf = strconv.AppendInt(buf, rec.Column(k).IntegerValues()[0], 10)
			case influx.Field_Type_Boolean:
				buf = strconv.AppendBool(buf, rec.Column(k).BooleanValues()[0])
			case influx.Field_Type_String:
				var str []string
				str = rec.Column(k).StringValues(str)
				buf = append(buf, '"')
				buf = append(buf, EscapeStringFieldValue(str[0])...)
				buf = append(buf, '"')
			default:
				// This shouldn't be possible, but we'll format it anyway.
				buf = append(buf, fmt.Sprintf("%v", rec.Column(k))...)
			}
			if k != rec.Len()-1 {
				buf = append(buf, ',')
			}
		}
	}
	buf = strconv.AppendInt(buf, rec.Times()[0], 10)
	buf = append(buf, '\n')
	return buf, nil
}

func (c *csvParser) writeMstInfoFromTssp(metaWriter io.Writer, outputWriter io.Writer, filePath string, isOrder bool, index *tsi.MergeSetIndex) error {
	tsspPathSplits := strings.Split(filePath, string(byte(os.PathSeparator)))
	measurementDirWithVersion := tsspPathSplits[len(tsspPathSplits)-2]
	measurementName := influx.GetOriginMstName(measurementDirWithVersion)
	dbName := tsspPathSplits[len(tsspPathSplits)-7]
	lockPath := ""
	tsspFile, err := immutable.OpenTSSPFile(filePath, &lockPath, isOrder)
	defer util.MustClose(tsspFile)
	if err != nil {
		return err
	}
	// search tags
	fiTag := immutable.NewFileIterator(tsspFile, immutable.CLog)
	itrTag := immutable.NewChunkIterator(fiTag)
	itrTag.Next()
	sid := itrTag.GetSeriesID()
	if sid == 0 {
		return fmt.Errorf("series ID is zero")
	}
	var combineKey []byte
	var seriesKeys [][]byte
	var isExpectSeries []bool
	// Use sid get series key's []byte
	if seriesKeys, _, _, err = index.SearchSeriesWithTagArray(sid, seriesKeys, nil, combineKey, isExpectSeries, nil); err != nil {
		return err
	}
	_, src, err := influx.MeasurementName(seriesKeys[0])
	if err != nil {
		return err
	}
	tagsN := encoding.UnmarshalUint16(src)
	src = src[2:]
	var i uint16
	var tags, fields, tagsType, fieldsType []string
	for i = 0; i < tagsN; i++ {
		keyLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		tagKey := EscapeTagKey(string(src[:keyLen]))
		tags = append(tags, tagKey)
		src = src[keyLen:]

		valLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		src = src[valLen:]
	}
	for i := 0; i < len(tags); i++ {
		tagsType = append(tagsType, "tag")
	}
	// search fields
	fiField := immutable.NewFileIterator(tsspFile, immutable.CLog)
	itrField := immutable.NewChunkIterator(fiField)
	itrField.NextChunkMeta()
	for _, colMeta := range fiField.GetCurtChunkMeta().GetColMeta() {
		fields = append(fields, colMeta.Name())
		if colMeta.Name() == "time" {
			fieldsType = append(fieldsType, "dateTime:timeStamp")
		} else {
			fieldsType = append(fieldsType, influx.FieldTypeString(int32(colMeta.Type())))
		}
	}
	c.fieldsName[dbName] = make(map[string][]string)
	c.fieldsName[dbName][measurementName] = fields
	c.curDatabase = dbName
	c.curMeasurement = measurementName
	// write datatype
	fmt.Fprintf(metaWriter, "#datatype %s,%s\n", strings.Join(tagsType, ","), strings.Join(fieldsType, ","))
	// write tags and fields name
	buf := influx.GetBytesBuffer()
	defer influx.PutBytesBuffer(buf)
	buf = append(buf, strings.Join(tags, ",")...)
	buf = append(buf, ',')
	buf = append(buf, strings.Join(fields, ",")...)
	buf = append(buf, '\n')
	_, err = outputWriter.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (c *csvParser) writeMstInfoFromWal(metaWriter io.Writer, outputWriter io.Writer, row influx.Row, currentDatabase string) error {
	tagsN := row.Tags
	fieldsN := row.Fields
	var tags, fields, tagsType, fieldsType []string
	for _, tag := range tagsN {
		tags = append(tags, tag.Key)
		tagsType = append(tagsType, "tag")
	}
	for _, field := range fieldsN {
		fields = append(fields, field.Key)
		fieldsType = append(fieldsType, influx.FieldTypeString(field.Type))
	}
	fieldsType = append(fieldsType, "dateTime:timeStamp")
	measurementWithVersion := row.Name
	measurementName := influx.GetOriginMstName(measurementWithVersion)
	measurementName = EscapeMstName(measurementName)
	c.fieldsName[currentDatabase] = make(map[string][]string)
	c.fieldsName[currentDatabase][measurementName] = fields
	c.curDatabase = currentDatabase
	c.curMeasurement = measurementName
	// write datatype
	fmt.Fprintf(metaWriter, "#datatype %s,%s\n", strings.Join(tagsType, ","), strings.Join(fieldsType, ","))
	// write tags and fields name
	buf := influx.GetBytesBuffer()
	defer influx.PutBytesBuffer(buf)
	buf = append(buf, strings.Join(tags, ",")...)
	buf = append(buf, ',')
	buf = append(buf, strings.Join(fields, ",")...)
	buf = append(buf, ',')
	buf = append(buf, "time"...)
	buf = append(buf, '\n')
	_, err := outputWriter.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (c *csvParser) getRowBuf(buf []byte, measurementName string, row influx.Row, _ *opengemini.Point) ([]byte, error) {
	tags := row.Tags
	fields := row.Fields
	tm := row.Timestamp

	for _, tag := range tags {
		buf = append(buf, EscapeTagValue(tag.Value)...)
		buf = append(buf, ',')
	}
	curFieldsName := c.fieldsName[c.curDatabase][c.curMeasurement]
	for _, fieldName := range curFieldsName {
		if fieldName == "time" {
			continue
		}
		k, ok := getFieldNameIndexFromRow(fields, fieldName)
		if !ok {
			buf = append(buf, ',')
		} else {
			switch fields[k].Type {
			case influx.Field_Type_Float:
				buf = strconv.AppendFloat(buf, fields[k].NumValue, 'g', -1, 64)
			case influx.Field_Type_Int:
				buf = strconv.AppendInt(buf, int64(fields[k].NumValue), 10)
			case influx.Field_Type_Boolean:
				buf = strconv.AppendBool(buf, fields[k].NumValue == 1)
			case influx.Field_Type_String:
				buf = append(buf, '"')
				buf = append(buf, EscapeStringFieldValue(fields[k].StrValue)...)
				buf = append(buf, '"')
			default:
				// This shouldn't be possible, but we'll format it anyway.
				buf = append(buf, fmt.Sprintf("%v", fields[k])...)
			}
			buf = append(buf, ',')
		}
	}
	buf = strconv.AppendInt(buf, tm, 10)
	buf = append(buf, '\n')
	return buf, nil
}

func (c *csvParser) writeMetaInfo(metaWriter io.Writer, infoType InfoType, info string) {
	switch infoType {
	case InfoTypeDatabase:
		fmt.Fprintf(metaWriter, "#constant database,%s\n", info)
	case InfoTypeRetentionPolicy:
		fmt.Fprintf(metaWriter, "#constant retention_policy,%s\n", info)
	case InfoTypeMeasurement:
		fmt.Fprintf(metaWriter, "#constant measurement,%s\n", info)
	default:
		return
	}
}

func (c *csvParser) writeOutputInfo(_ io.Writer, _ string) {
}

// getFieldNameIndexFromRecord returns the index of a field in a slice
func getFieldNameIndexFromRecord(slice []record.Field, str string) (int, bool) {
	for i, v := range slice {
		if v.Name == str {
			return i, true
		}
	}
	return 0, false
}

func getFieldNameIndexFromRow(slice []influx.Field, str string) (int, bool) {
	for i, v := range slice {
		if v.Key == str {
			return i, true
		}
	}
	return 0, false
}

var escapeFieldKeyReplacer = strings.NewReplacer(`,`, `\,`, `=`, `\=`, ` `, `\ `)
var escapeTagKeyReplacer = strings.NewReplacer(`,`, `\,`, `=`, `\=`, ` `, `\ `)
var escapeTagValueReplacer = strings.NewReplacer(`,`, `\,`, `=`, `\=`, ` `, `\ `)
var escapeMstNameReplacer = strings.NewReplacer(`=`, `\=`, ` `, `\ `)
var escapeStringFieldReplacer = strings.NewReplacer(`"`, `\"`, `\`, `\\`)

// EscapeFieldKey returns a copy of in with any comma or equal sign or space
// with escaped values.
func EscapeFieldKey(in string) string {
	return escapeFieldKeyReplacer.Replace(in)
}

// EscapeStringFieldValue returns a copy of in with any double quotes or
// backslashes with escaped values.
func EscapeStringFieldValue(in string) string {
	return escapeStringFieldReplacer.Replace(in)
}

// EscapeTagKey returns a copy of in with any "comma" or "equal sign" or "space"
// with escaped values.
func EscapeTagKey(in string) string {
	return escapeTagKeyReplacer.Replace(in)
}

// EscapeTagValue returns a copy of in with any "comma" or "equal sign" or "space"
// with escaped values
func EscapeTagValue(in string) string {
	return escapeTagValueReplacer.Replace(in)
}

// EscapeMstName returns a copy of in with any "equal sign" or "space"
// with escaped values.
func EscapeMstName(in string) string {
	return escapeMstNameReplacer.Replace(in)
}
