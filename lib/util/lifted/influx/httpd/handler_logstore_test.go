package httpd

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
	"github.com/stretchr/testify/assert"
)

func mockLogWriteRequest(mapping string) *LogWriteRequest {
	req := &LogWriteRequest{
		repository:     "test",
		logStream:      "test",
		printFailLog:   getPrintFailLog(),
		timeMultiplier: 1e6,
		mstSchema:      &meta.CleanSchema{},
	}
	if len(mapping) > 0 {
		req.mapping, _ = parseMapping(mapping)
	}
	return req
}

func TestParseJsonTimeColumn(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}

	now := time.Now().UnixMilli()
	s := fmt.Sprintf("{\"time\":%v, \"http\":\"127.0.0.1\", \"cnt\":4}", now)

	req := mockLogWriteRequest(`{"timestamp":"time", "content": ["http", "cnt"]}`)
	scanner := bufio.NewScanner(strings.NewReader(s))
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)
	scanner.Split(bufio.ScanLines)

	ld := 7 * 24 * time.Hour
	req.expiredTime = time.Now().UnixNano() - ld.Nanoseconds()
	req.logSchema = logSchema
	rows := record.NewRecord(logSchema, false)
	failRows := record.NewRecord(failLogSchema, false)
	_ = h.parseJson(scanner, req, rows, failRows)
	expect := fmt.Sprintf("time")
	res := rows.Schema[len(rows.Schema)-1].Name
	if expect != res {
		t.Fatal("unexpect", res)
	}
}

func TestParseJsonNoMapping(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}

	s := fmt.Sprintf("{\"time\":1719862212771, \"http\":\"127.0.0.1\", \"cnt\":4}")
	req := mockLogWriteRequest("{\"timestamp\":\"time\"}")
	scanner := bufio.NewScanner(strings.NewReader(s))
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)

	rows := record.NewRecord(logSchema, false)
	failRows := record.NewRecord(failLogSchema, false)
	req.printFailLog = getPrintFailLog()
	req.logSchema = logSchema
	_ = h.parseJson(scanner, req, rows, failRows)
	assert.Equal(t, float64(4), rows.ColVals[1].FloatValues()[0])
	assert.Equal(t, "127.0.0.1", string(rows.ColVals[2].Val))
	assert.Equal(t, int64(1719862212771000000), rows.ColVals[rows.ColNums()-1].IntegerValues()[0])
}

func TestParseJsonFailLog(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}
	request := &LogWriteRequest{
		repository:     "test",
		logStream:      "test",
		failTag:        "failLog",
		retry:          false,
		timeMultiplier: 1000,
		requestTime:    123,
		mstSchema:      &meta.CleanSchema{},
	}
	request.mapping = &JsonMapping{
		timestamp: "timestamp",
	}
	request.printFailLog = getPrintFailLog()

	s := fmt.Sprintf("{\"timestamp\":1234}")
	scanner := bufio.NewScanner(strings.NewReader(s))
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)

	rows := record.NewRecord(logSchema, false)
	failRows := record.NewRecord(failLogSchema, false)
	_ = h.parseJson(scanner, request, rows, failRows)
	assert.Equal(t, "failLog", string(failRows.ColVals[1].Val))
	assert.Equal(t, "{\"timestamp\":1234}", string(failRows.ColVals[0].Val))
	assert.Equal(t, int64(123), failRows.ColVals[failRows.ColNums()-1].IntegerValues()[0])

	// Test parse big log
	bigLog := make([]byte, MaxContentLen+1)
	for i := range bigLog {
		bigLog[i] = 97
	}
	scanner = bufio.NewScanner(strings.NewReader(string(bigLog)))
	scanner.Buffer(make([]byte, 100*1024), ScannerBufferSize*2)

	rows = record.NewRecord(logSchema, false)
	failRows = record.NewRecord(failLogSchema, false)
	_ = h.parseJson(scanner, request, rows, failRows)
	res := failRows.ColVals[1].StringValues(nil)
	assert.Equal(t, BigLogTag+"_1", res[0])
	assert.Equal(t, BigLogTag+"_2", res[1])

}

func TestParseLogTags(t *testing.T) {
	logTags := `{"tag1":"this is tag1","tag2":"this is tag2","tag3":1715065030012,"tag4":{"ss":"this is string"}}`
	req := &LogWriteRequest{logTagString: &logTags, mstSchema: &meta.CleanSchema{}}
	req.mapping = &JsonMapping{discardFields: map[string]bool{}}
	tagsMap, err := parseLogTags(req)
	assert.Equal(t, nil, err)

	assert.Equal(t, `this is tag1`, string(tagsMap["tag1"]))
	assert.Equal(t, `this is tag2`, string(tagsMap["tag2"]))
	assert.Equal(t, `1715065030012`, string(tagsMap["tag3"]))
	assert.Equal(t, `{"ss":"this is string"}`, string(tagsMap["tag4"]))
}

func TestClearFailRow(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}

	s1 := "{\"time\":1719862212771, \"strType\":\"127\", \"numType\":4,\"boolType\":true,\"numType\":4 }\n"
	s2 := "{\"time\":1719862212772, \"strType\":\"127\", \"numType\":4,\"boolType\":true}\n"
	s := fmt.Sprintf(s1 + s2)
	req := mockLogWriteRequest("{\"timestamp\":\"time\"}")

	scanner := bufio.NewScanner(strings.NewReader(s))
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)

	rows := record.NewRecord(logSchema, false)
	failRows := record.NewRecord(failLogSchema, false)
	req.printFailLog = getPrintFailLog()
	req.logSchema = logSchema
	_ = h.parseJson(scanner, req, rows, failRows)
	assert.Equal(t, 1, rows.RowNums())
	assert.Equal(t, 1, failRows.RowNums())
}

func TestJsonHighlight(t *testing.T) {
	parser := &logparser.YyParser{Query: influxql.Query{}}
	parser.Scanner = logparser.NewScanner(strings.NewReader("json"))
	parser.ParseTokens()
	q, _ := parser.GetQuery()
	query := map[string]map[string]bool{}
	if q != nil {
		expr := &(q.Statements[0].(*influxql.LogPipeStatement).Cond)
		query = getHighlightWords(expr, query)
	}

	// test string field
	res := getJsonHighlight("key", `this is json`, query)
	assert.Equal(t, "json", res.Value[1].Fragment)
	assert.Equal(t, true, res.Value[1].Highlight)

	// test bool field
	res = getJsonHighlight("key", false, query)
	assert.Equal(t, "false", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test int64 field
	res = getJsonHighlight("key", int64(45), query)
	assert.Equal(t, "45", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test float64 field
	res = getJsonHighlight("key", 45.6, query)
	assert.Equal(t, "45.6", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test nil field
	res = getJsonHighlight("key", nil, query)
	assert.Equal(t, "null", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test inner json
	res = getJsonHighlight("key", `{"field": "this is json"}`, query)
	innerJsonRes := res.InnerJson["segments"].([]*JsonHighlightFragment)[0]
	assert.Equal(t, "json", innerJsonRes.Value[1].Fragment)
	assert.Equal(t, true, res.Value[1].Highlight)

	// test without content
	res = getJsonHighlight("key", ``, query)
	assert.Equal(t, 0, len(res.Value))
}

func TestConvertToString(t *testing.T) {
	res := convertToString(nil)
	assert.Equal(t, "null", res)

	res = convertToString("string")
	assert.Equal(t, "string", res)

	res = convertToString(45.6)
	assert.Equal(t, "45.6", res)

	res = convertToString(45)
	assert.Equal(t, "45", res)

	res = convertToString(false)
	assert.Equal(t, "false", res)

	res = convertToString(true)
	assert.Equal(t, "true", res)
}

func TestParsePPlQuery(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}
	query, err := h.parsePplAndSqlQuery("* | SELECT COUNT(cnt) AS pv GROUP BY time(5s)", &measurementInfo{
		name:            "xx",
		database:        "xx",
		retentionPolicy: "xx",
	})
	if err != nil {
		t.Fatal(err)
	}
	if query.String() != "SELECT count(cnt) AS pv GROUP BY time(5s)" {
		t.Fatal("unexpect query str", query.String())
	}
	rewriteQueryForStream(query)
	if query.String() != "SELECT count(pv) AS pv GROUP BY time(5s)" {
		t.Fatal("unexpect query str", query.String())
	}
	query1, err := h.parsePplAndSqlQuery("* | SELECT COUNT(cnt) GROUP BY time(5s)", &measurementInfo{
		name:            "xx",
		database:        "xx",
		retentionPolicy: "xx",
	})
	if err != nil {
		t.Fatal(err)
	}
	if query1.String() != "SELECT count(cnt) GROUP BY time(5s)" {
		t.Fatal("unexpect query str", query1.String())
	}
	rewriteQueryForStream(query1)
	if query1.String() != "SELECT count(count_cnt) GROUP BY time(5s)" {
		t.Fatal("unexpect query str", query1.String())
	}
}

func TestRewriteLogStream(t *testing.T) {
	rp := "auto"
	logStream := "log"
	newLogStream := rewriteLogStream(rp, logStream)
	expect := "view-auto-log"
	if newLogStream != expect {
		t.Fatal("unexpect", newLogStream)
	}
}

func TestParseJsonArray(t *testing.T) {
	jsonArray := `[{"field1":"aa","field2":"bb"},{"field1":"cc","field2":"dd"}]`
	var jsonMapSlice []map[string]interface{}

	err := sonic.UnmarshalString(jsonArray, &jsonMapSlice)
	if err != nil {
		t.Fatal("unmarshal string err")
	}
	assert.Equal(t, 2, len(jsonMapSlice))

	// Test stream read
	jsonCompact := `{"int_v":15,"float_v":15.1,"bool_v":true,"string_v":"this is string","nest_json":{"v":"ii"}}`
	var jsonMap map[string]interface{}
	var r = strings.NewReader(jsonCompact)
	var dec = sonic.ConfigDefault.NewDecoder(r)

	for {
		err = dec.Decode(&jsonMap)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
	}
	assert.Equal(t, 5, len(jsonMap))

	// Test null value
	res, ok := jsonMap["wrong"].(string)
	assert.Equal(t, false, ok)
	assert.Equal(t, "", res)
}

func TestInterface2str(t *testing.T) {
	b := []byte("hello, byte")
	s := Interface2str(b)
	assert.Equal(t, string(b), s)

	m := map[string]interface{}{}
	b, err := sonic.Marshal(m)
	assert.Equal(t, nil, err)
	assert.Equal(t, string(b), `{}`)

	m["first"] = true
	m["second"] = "hello"
	s = Interface2str(m)
	assert.Equal(t, true, strings.Contains(s, `"first":true`))

	failStr := "hello, string"
	s = Interface2str(failStr)
	assert.Equal(t, failStr, s)

	res := util.Str2bytes(failStr)
	assert.Equal(t, []byte(failStr), res)
}

func TestParseTime(t *testing.T) {
	// Test the wrong format
	s := "2017-12-08 20:05:30"
	_, err := time.Parse("yyyy-MM-dd HH:mm:ss", s)
	assert.NotEqual(t, nil, err)

	// Test parse null value
	_, err = time.Parse("2006/01/02 15:04:05", "")
	assert.NotEqual(t, nil, err)

	// Test the normal format
	s = "2017/12/08 20:05:30"
	_, err = time.Parse("2006/01/02 15:04:05", s)
	assert.Equal(t, nil, err)

	// Test the redundant format
	s = "2017/5/8T20:05:30Z+UTC0800"
	_, err = time.Parse("2006/1/2T15:04:05Z+UTC0800", s)
	assert.Equal(t, nil, err)
}

func TestGetTimeFormat(t *testing.T) {
	// Test the normal time format
	mapping := `{"time_format":"yyyy-MM-ddTHH:mm:ssZ","time_zone":"UTC+8"}`
	jm := JsonMapping{}
	p := parserPool.Get()
	defer parserPool.Put(p)
	v, err := p.Parse(mapping)
	assert.Equal(t, nil, err)
	err = getTimeFormat(v, &jm)
	assert.Equal(t, nil, err)
	assert.Equal(t, convertTimeFormat("yyyy-MM-ddTHH:mm:ssZ"), jm.timeFormat)
	assert.Equal(t, int64(8), jm.timeZone)

	mapping = `{"time_format":"yyyy-MM-ddTHH:mm:ssZ","time_zone":"UTC-6"}`
	v, err = p.Parse(mapping)
	assert.Equal(t, nil, err)
	err = getTimeFormat(v, &jm)
	assert.Equal(t, nil, err)
	assert.Equal(t, convertTimeFormat("yyyy-MM-ddTHH:mm:ssZ"), jm.timeFormat)
	assert.Equal(t, int64(-6), jm.timeZone)

	// Test time format is null
	mapping = `{}`
	jm = JsonMapping{}
	v, err = p.Parse(mapping)
	assert.Equal(t, nil, err)
	err = getTimeFormat(v, &jm)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(jm.timeFormat))

	// Test the wrong time format
	mapping = `{"time_format":123,"time_zone":"UTC+8"}`
	v, err = p.Parse(mapping)
	assert.Equal(t, nil, err)
	err = getTimeFormat(v, &jm)
	assert.Equal(t, errno.NewError(errno.InvalidMappingTimeFormatVal), err)

	mapping = `{"time_format":"yyyy-MM-ddTHH:mm:ssZ"}`
	v, err = p.Parse(mapping)
	assert.Equal(t, nil, err)
	err = getTimeFormat(v, &jm)
	assert.Equal(t, errno.NewError(errno.InvalidMappingTimeZone), err)

	mapping = `{"time_format":"yyyy-MM-ddTHH:mm:ssZ","time_zone":123}`
	v, err = p.Parse(mapping)
	assert.Equal(t, nil, err)
	err = getTimeFormat(v, &jm)
	assert.Equal(t, errno.NewError(errno.InvalidMappingTimeZoneVal), err)

	mapping = `{"time_format":"yyyy-MM-ddTHH:mm:ssZ","time_zone":"UTC"}`
	v, err = p.Parse(mapping)
	assert.Equal(t, nil, err)
	err = getTimeFormat(v, &jm)
	assert.Equal(t, errno.NewError(errno.InvalidMappingTimeZoneVal), err)
}

func TestAppendValueOrNull(t *testing.T) {
	cv := &record.ColVal{}
	str1 := "test1"
	str2 := []byte("test2")
	appendValueOrNull(cv, str1)
	assert.Equal(t, str1, string(cv.Val))

	appendValueOrNull(cv, str2)
	assert.Equal(t, str2, cv.Val[len(str1):])
	assert.Equal(t, 0, cv.NilCount)

	appendValueOrNull(cv, 12)
	assert.Equal(t, str1+string(str2), string(cv.Val))
	assert.Equal(t, 1, cv.NilCount)

	appendValueOrNull(cv, "")
	assert.Equal(t, str1+string(str2), string(cv.Val))
	assert.Equal(t, 2, cv.NilCount)
}

func TestGetBulkRecords(t *testing.T) {
	rows := record.GetRecordFromPool(record.LogStoreRecordPool, logSchema)
	failRows := record.GetRecordFromPool(record.LogStoreRecordPool, logSchema)
	req := &LogWriteRequest{
		minTime:   1717025800001000000,
		maxTime:   1717025800002000000,
		mstSchema: &meta.CleanSchema{},
	}

	rows.ColVals[0].AppendBoolean(true)
	rows.ColVals[1].AppendInteger(2)
	rows.ColVals[0].AppendBoolean(true)
	rows.ColVals[1].AppendInteger(1)
	bulk, _ := getBulkRecords(rows, failRows, req, 0, 24*time.Hour)
	assert.Equal(t, int64(2), bulk.Rec.Times()[0])

	req.minTime = 1717025800001000000
	req.maxTime = 1717035800001000000
	bulk, _ = getBulkRecords(rows, failRows, req, 0, 24*time.Hour)
	assert.Equal(t, int64(1), bulk.Rec.Times()[0])
}

func TestCheckField(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}

	res := h.isNeedReply("test", nil)
	assert.Equal(t, false, res)

	res = h.isNeedReply("test", 12)
	assert.Equal(t, true, res)

	res = h.isNeedReply(RetryTag, false)
	assert.Equal(t, false, res)

	res = h.isNeedReply(RetryTag, true)
	assert.Equal(t, true, res)
}

func TestAppendFieldScopes(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}

	content := map[string]interface{}{}
	content["int_field"] = 12
	content["float_field"] = 12.5
	content["string_field"] = "this is string"
	content["json_field"] = `{"field1":"hello","field2":{"nest_field":12.8}}`
	content["bool_field"] = true

	fieldSlice := []string{"bool_field", "float_field", "int_field", "json_field", "string_field"}
	var fieldScopes []marshalFieldScope
	for _, key := range fieldSlice {
		fieldScopes = h.appendFieldScopes(fieldScopes, key, content[key])
	}

	b, err := json.Marshal(content)
	assert.Equal(t, nil, err)
	for _, fieldScope := range fieldScopes {
		switch v := content[fieldScope.key].(type) {
		case bool:
			assert.Equal(t, util.Bool2str(v), string(b[fieldScope.start:fieldScope.end]))
		case int:
			assert.Equal(t, strconv.Itoa(v), string(b[fieldScope.start:fieldScope.end]))
		case float64:
			assert.Equal(t, strconv.FormatFloat(v, 'f', -1, 64), string(b[fieldScope.start:fieldScope.end]))
		case string:
			assert.Equal(t, byte('"'), b[fieldScope.start])
			assert.Equal(t, byte('"'), b[fieldScope.end-1])
		}
	}
}

func TestAddLogTagsField(t *testing.T) {
	failRows := record.GetRecordFromPool(record.LogStoreFailRecordPool, failLogSchema)
	logTagsMap := map[string][]byte{
		"tag1": []byte("this is tag1"),
		"tag2": []byte("this is tag2"),
		"tag3": []byte("this is tag3"),
	}
	req := &LogWriteRequest{logSchema: logSchema}

	assert.Equal(t, failLogSchema.Len(), failRows.ColNums())
	assert.Equal(t, logSchema.Len(), req.logSchema.Len())

	logTagsSlice, logTagsKey := addLogTagsField(failRows, logTagsMap, req)
	assert.Equal(t, failLogSchema.Len()+len(logTagsMap), failRows.ColNums())
	assert.Equal(t, logSchema.Len()+len(logTagsMap), req.logSchema.Len())
	assert.Equal(t, len(logTagsMap), len(logTagsSlice))
	for k := range logTagsMap {
		assert.Equal(t, true, logTagsKey[k])
	}
}

func TestParseFirstRowSchema(t *testing.T) {
	schema := append(logSchema, record.Field{Type: 4, Name: "tag1"})
	req := mockLogWriteRequest(`{"timestamp":"time"}`)
	req.logSchema = schema
	rows := record.LogStoreRecordPool.Get()
	pf := getParseField(schema.Len())
	p := parserPool.Get()
	defer parserPool.Put(p)

	s := `{"field1":"one","field2":"two","field2":"three"}`
	v, err := p.ParseBytes([]byte(s))
	assert.Equal(t, nil, err)
	pf.object, err = v.Object()
	assert.Equal(t, nil, err)
	err = parseFirstRowSchema(req, pf)
	assert.Equal(t, errno.NewError(errno.ErrFieldDuplication, "field2"), err)

	pf = getParseField(schema.Len())
	req.logSchema = req.logSchema[:logSchema.Len()+1]
	s = `{"field1":"one","field2":"two","__retry_tag__":"three"}`
	v, _ = p.ParseBytes([]byte(s))
	pf.object, _ = v.Object()
	err = parseFirstRowSchema(req, pf)
	assert.Equal(t, errno.NewError(errno.ErrReservedFieldDuplication, RetryTag), err)

	pf = getParseField(schema.Len())
	req.logSchema = req.logSchema[:logSchema.Len()+1]
	s = `{"field1":"one","field2":"two","field3":"three"}`
	v, _ = p.ParseBytes([]byte(s))
	pf.object, _ = v.Object()
	err = parseFirstRowSchema(req, pf)
	assert.Equal(t, nil, err)
	reuseRecordSchema(rows, req)
	assert.Equal(t, schema.Len()+3, rows.ColNums())
	for i := 0; i < schema.Len(); i++ {
		assert.Equal(t, schema[i].Name, rows.Schema[i].Name)
	}
	assert.Equal(t, "field3", rows.Schema[schema.Len()+2].Name)
}
