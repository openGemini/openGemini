package httpd

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
	"github.com/stretchr/testify/assert"
)

func mockLogWriteRequest(mapping string) *LogWriteRequest {
	req := &LogWriteRequest{
		repository:       "test",
		logStream:        "test",
		printFailLog:     getPrintFailLog(),
		maxUnixTimestamp: MaxUnixTimestampNs / 1e6,
		minUnixTimestamp: MinUnixTimestampNs / 1e6,
		timeMultiplier:   1e6,
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
	assert.Equal(t, "failLog", string(failRows.ColVals[0].Val))
	assert.Equal(t, "{\"timestamp\":1234}", string(failRows.ColVals[1].Val))
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
	res := failRows.ColVals[0].StringValues(nil)
	assert.Equal(t, BigLogTag+"_1", res[0])
	assert.Equal(t, BigLogTag+"_2", res[1])

}

func TestParseLogTags(t *testing.T) {
	logTags := `{"tag1":"this is tag1","tag2":"this is tag2","tag3":1715065030012,"tag4":{"ss":"this is string"}}`
	req := &LogWriteRequest{logTagString: &logTags}
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
	_ = h.parseJson(scanner, req, rows, failRows)
	assert.Equal(t, 1, rows.RowNums())
	assert.Equal(t, 1, failRows.RowNums())
}

func TestJsonHighlight(t *testing.T) {
	parser := &logparser.YyParser{Query: influxql.Query{}}
	parser.Scanner = logparser.NewScanner(strings.NewReader("json"))
	parser.ParseTokens()
	q, _ := parser.GetQuery()

	// test string field
	res := getJsonHighlight("key", `this is json`, q)
	assert.Equal(t, "json", res.Value[1].Fragment)
	assert.Equal(t, true, res.Value[1].Highlight)

	// test bool field
	res = getJsonHighlight("key", false, q)
	assert.Equal(t, "false", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test int64 field
	res = getJsonHighlight("key", int64(45), q)
	assert.Equal(t, "45", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test float64 field
	res = getJsonHighlight("key", 45.6, q)
	assert.Equal(t, "45.6", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test nil field
	res = getJsonHighlight("key", nil, q)
	assert.Equal(t, "null", res.Value[0].Fragment)
	assert.Equal(t, false, res.Value[0].Highlight)

	// test inner json
	res = getJsonHighlight("key", `{"field": "this is json"}`, q)
	innerJsonRes := res.InnerJson["segments"].([]*JsonHighlightFragment)[0]
	assert.Equal(t, "json", innerJsonRes.Value[1].Fragment)
	assert.Equal(t, true, res.Value[1].Highlight)

	// test without content
	res = getJsonHighlight("key", ``, q)
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
