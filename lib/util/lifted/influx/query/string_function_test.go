// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package query_test

import (
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestStringFunctionStr(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "str"
	inputArgs := []interface{}{"abc", "bcd", "cde"}
	subStr := "bc"
	expects := []interface{}{true, true, false}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, subStr}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)
}

func TestStringFunctionReverse(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "reverse"
	inputArgs := []interface{}{"abc", "bcd", "cde"}
	expects := []interface{}{"cba", "dcb", "edc"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionStrLen(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "strlen"
	inputArgs := []interface{}{"", "abc", "bcde", "cdefg"}
	expects := []interface{}{int64(0), int64(3), int64(4), int64(5)}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)
}

func TestStringFunctionPosition(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "position"
	inputArgs := []interface{}{"", "abc", "bcd", "cde"}
	subStr := "bc"
	expects := []interface{}{int64(0), int64(2), int64(1), int64(0)}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, subStr}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionSubStrOnePara(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "substr"
	inputArgs := []interface{}{"", "abc", "bcde", "cdefg"}
	start := int64(1)
	expects := []interface{}{"", "bc", "cde", "defg"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, start}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)
}

func TestStringFunctionSubStrTwoPara(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "substr"
	inputArgs := []interface{}{"", "abc", "bcde", "cdefg"}
	start := int64(1)
	subLen := int64(2)
	expects := []interface{}{"", "bc", "cd", "de"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, start, subLen}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)

}

func TestStringFunctionSplit(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "split"
	inputArgs := []interface{}{"abc", "ab,c", "a,b,c"}
	expects := []interface{}{"[\"abc\"]", "[\"ab\",\"c\"]", "[\"a\",\"b\",\"c\"]"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, ","}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionSplitPart(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "split_part"
	inputArgs := []interface{}{"abc", "ab,c", "a,b,c"}
	expects := []interface{}{"", "c", "b"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, ",", int64(2)}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionSplitToMap(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "split_to_map"
	inputArgs := []interface{}{"k1:v1,k2:v2,k3:v3", "k1:v1", "k", "k1"}
	expects := []interface{}{`{"k1":"v1","k2":"v2","k3":"v3"}`, `{"k1":"v1"}`, `k`, `{"k1":""}`}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, ",", ":"}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionLpad(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "lpad"
	inputArgs := []interface{}{"abc", "this is string", "xx"}
	expects := []interface{}{"hahahahabc", "this is st", "hahahahaxx"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, int64(10), "ha"}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionReplace(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "replace"
	inputArgs := []interface{}{"abc", "this is a", "aa"}
	expects := []interface{}{"bbc", "this is b", "bb"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, "a", "b"}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionRpad(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "rpad"
	inputArgs := []interface{}{"abc", "this is abc", "aa"}
	expects := []interface{}{"abchahahah", "this is ab", "aahahahaha"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, int64(10), "ha"}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestStringFunctionStrPos(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "strpos"
	inputArgs := []interface{}{"abc", "a,bc", "ac"}
	expects := []interface{}{"2", "3", "0"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, "b"}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionDecode(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_decode"
	inputArgs := []interface{}{"https%3A%2F%2Fwww.xxx.com%3A80%2Fproduct%2Flog"}
	expects := []interface{}{"https://www.xxx.com:80/product/log"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionEncode(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_encode"
	inputArgs := []interface{}{"https://www.xxx.com:80/product/log"}
	expects := []interface{}{"https%3A%2F%2Fwww.xxx.com%3A80%2Fproduct%2Flog"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionExtractFragment(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_extract_fragment"
	inputArgs := []interface{}{"https://www.example.com:8080/#/path?query=value"}
	expects := []interface{}{"/path?query=value"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionExtractHost(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_extract_host"
	inputArgs := []interface{}{"https://www.example.com:8080/#/path?query=value"}
	expects := []interface{}{"www.example.com:8080"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionExtractParameter(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_extract_parameter"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"https://www.example.com", 15})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"https://www.example.com/path?key=5", "key0"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "https://www.example.com/path/test1?key0=12"
	inputArgs[1] = "https://www.xxx.com:8081/path/test2?key0=12&key1=15"
	inputArgs[2] = "https://www.example.com:8082/path/test3?key2=18"
	expects := make([]interface{}, 3)
	expects[0] = "12"
	expects[1] = "15"
	expects[2] = "18"
	outputs := make([]interface{}, 0, len(expects))
	for i, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, "key" + strconv.Itoa(i)}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionExtractPath(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_extract_path"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"https://www.example.com"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "https://www.example.com/path/test1"
	inputArgs[1] = "https://www.xxx.com:8081/path/test2"
	inputArgs[2] = "https://www.example.com:8082/path/test3"
	expects := make([]interface{}, 3)
	expects[0] = "/path/test1"
	expects[1] = "/path/test2"
	expects[2] = "/path/test3"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionExtractPort(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_extract_port"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"www.example.com"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "https://www.example.com:8080"
	inputArgs[1] = "http://www.xxx.com:8081/path"
	inputArgs[2] = "https://www.example.com:8082"
	expects := make([]interface{}, 3)
	expects[0] = "8080"
	expects[1] = "8081"
	expects[2] = "8082"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionExtractProtocol(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_extract_protocol"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"://www.example.com:8080"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"xx://www.example.com:8080"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "xx", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "https://www.example.com:8080"
	inputArgs[1] = "http://www.xxx.com:8080/path"
	inputArgs[2] = "https://www.example.com:8080"
	expects := make([]interface{}, 3)
	expects[0] = "https"
	expects[1] = "http"
	expects[2] = "https"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestUrlFunctionExtractQuery(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "url_extract_query"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"string", 15})
	assert.Equal(t, true, ok)
	assert.Equal(t, "", out)

	out, ok = urlValuer.Call(inputName, []interface{}{"https://www.example.com:8080"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "https://www.example.com:8080/path?query=value"
	inputArgs[1] = "https://www.xxx.com:8080/path?type=value&limit=10"
	inputArgs[2] = "https://www.example.com:8080/path?query=value&highlight=true"
	expects := make([]interface{}, 3)
	expects[0] = "query=value"
	expects[1] = "type=value&limit=10"
	expects[2] = "query=value&highlight=true"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestJsonFunctionJsonExtract(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "json_extract"

	// Correct test cases
	inputArgs := make([]interface{}, 4)
	inputArgs[0] = `[{"EndTime":1626314920},{"FireResult":2}]`
	inputArgs[1] = `{"EndTime":1626314921, "nest":{"field1":"field1", "array":["number","int"]}}`
	inputArgs[2] = `{"EndTime":1626314922, "nest":{"field1":"field1", "field2":{"nest_field":12}}}`
	inputArgs[3] = `{"EndTime":1626314922, "nest":{"array":[{"type":[1,2]},"int"]}}`
	jsonPath := make([]interface{}, 4)
	jsonPath[0] = `$.0.EndTime`
	jsonPath[1] = "$.nest.array[1]"
	jsonPath[2] = "$.nest.field2"
	jsonPath[3] = "$.nest.array[0].type[1]"

	expects := make([]interface{}, 4)
	expects[0] = "1626314920"
	expects[1] = "int"
	expects[2] = `{"nest_field":12}`
	expects[3] = `2`
	outputs := make([]interface{}, 0, len(expects))
	for i, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, jsonPath[i]}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{"this is not json", "$.0"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{`{"field1":"value"}`, "$.wrong_field"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "null", out)

	out, ok = urlValuer.Call(inputName, []interface{}{`{"field1":"value"}`, "$.field1.order[0]"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)
}

func TestGetJsonValue(t *testing.T) {
	value := map[string]interface{}{"field1": "value", "field2": 12}
	pathParts := []string{"field2"}
	res, err := query.GetJsonValue(value, pathParts)
	assert.Equal(t, res, `12`)
	assert.Equal(t, err, nil)

	pathParts = []string{"field3"}
	res, err = query.GetJsonValue(value, pathParts)
	assert.Equal(t, res, `null`)
	assert.Equal(t, err, nil)

	pathParts = []string{"field2", "xx"}
	res, err = query.GetJsonValue(value, pathParts)
	assert.Equal(t, res, ``)
	assert.Equal(t, err, errno.NewError(errno.JsonPathIllegal))

	value = map[string]interface{}{"field1": "value", "field2": map[string]interface{}{"nest": 15}}
	pathParts = []string{"field2", "nest"}
	res, err = query.GetJsonValue(value, pathParts)
	assert.Equal(t, res, `15`)
	assert.Equal(t, err, nil)

	value = map[string]interface{}{"field1": "value", "field2": []interface{}{map[string]interface{}{"nest": 12}, "nest2"}}
	pathParts = []string{"field2[0]", "nest"}
	res, err = query.GetJsonValue(value, pathParts)
	assert.Equal(t, res, `12`)
	assert.Equal(t, err, nil)

	value = map[string]interface{}{"field1": "value", "field2": []interface{}{map[string]interface{}{"nest": []interface{}{1, 2}}, "nest2"}}
	pathParts = []string{"field2[0]", "nest[1]"}
	res, err = query.GetJsonValue(value, pathParts)
	assert.Equal(t, res, `2`)
	assert.Equal(t, err, nil)
}

func TestJsonExtract(t *testing.T) {
	jsonStr := `{"field1":{"nest1":12}, "field2":"word"}`
	jsonPath := "$.field1.nest1"
	res, err := query.JsonExtract(jsonStr, jsonPath)
	assert.Equal(t, res, "12")
	assert.Equal(t, err, nil)

	jsonStr = `{"field1":["nest1", "nest2"], "field2":"word"}`
	jsonPath = "$.field1[1]"
	res, err = query.JsonExtract(jsonStr, jsonPath)
	assert.Equal(t, res, "nest2")
	assert.Equal(t, err, nil)

	jsonStr = `[{"json1":"value1"}, {"json2":"value1"}]`
	jsonPath = "$.0.json1"
	res, err = query.JsonExtract(jsonStr, jsonPath)
	assert.Equal(t, res, "value1")
	assert.Equal(t, err, nil)

	jsonStr = `[{"json1":{"nest1":[12,15]}}, {"json2":"value1"}]`
	jsonPath = "$.0.json1.nest1[0]"
	res, err = query.JsonExtract(jsonStr, jsonPath)
	assert.Equal(t, res, "12")
	assert.Equal(t, err, nil)

	jsonStr = `{"field1":15,"field2":[{"nest1":[12,15]},{"nest2":{"number":18}}]}`
	jsonPath = "$.field2[1].nest2.number"
	res, err = query.JsonExtract(jsonStr, jsonPath)
	assert.Equal(t, res, "18")
	assert.Equal(t, err, nil)

	jsonStr = `this is not json`
	jsonPath = "$.field1.nest1"
	res, err = query.JsonExtract(jsonStr, jsonPath)
	assert.Equal(t, res, "")
	assert.NotEqual(t, err, nil)

	jsonStr = `[{"json1":"value1"}, {"json2":"value2"}]`
	jsonPath = "$.5.json1"
	res, err = query.JsonExtract(jsonStr, jsonPath)
	assert.Equal(t, res, "")
	assert.Equal(t, err, errno.NewError(errno.JsonPathIllegal))
}

func TestJsonFunctionJsonObject(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "json_object"

	rows := make([][]interface{}, 2)
	cols := make([]interface{}, 10)
	cols[0] = "strVal"
	cols[1] = "val1"
	cols[2] = "intVal"
	cols[3] = 1
	cols[4] = "floatVal"
	cols[5] = 1.1
	cols[6] = "boolVal"
	cols[7] = true
	cols[8] = "time"
	cols[9] = 1629129602000000000
	rows[0] = cols
	cols = make([]interface{}, 10)
	cols[0] = "strVal"
	cols[1] = "val2"
	cols[2] = "intVal"
	cols[3] = 2
	cols[4] = "floatVal"
	cols[5] = 2.2
	cols[6] = "boolVal"
	cols[7] = false
	cols[8] = "time"
	cols[9] = 1629129608000000000
	rows[1] = cols

	expects := make([]interface{}, 2)
	expects[0] = `{"strVal":"val1","intVal":1,"floatVal":1.1,"boolVal":true,"time":1629129602000000000}`
	expects[1] = `{"strVal":"val2","intVal":2,"floatVal":2.2,"boolVal":false,"time":1629129608000000000}`
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range rows {
		if out, ok := urlValuer.Call(inputName, arg); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestSplitPath(t *testing.T) {
	jsonPath := "$.field1.nest1"
	res, err := query.SplitPath(jsonPath)
	assert.Equal(t, res, []string{"field1", "nest1"})
	assert.Equal(t, err, nil)

	jsonPath = "$.0.nest1"
	res, err = query.SplitPath(jsonPath)
	assert.Equal(t, res, []string{"0", "nest1"})
	assert.Equal(t, err, nil)

	jsonPath = "$.field1[2].nest1[1]"
	res, err = query.SplitPath(jsonPath)
	assert.Equal(t, res, []string{"field1[2]", "nest1[1]"})
	assert.Equal(t, err, nil)

	jsonPath = ".field1.nest1"
	res, err = query.SplitPath(jsonPath)
	assert.Equal(t, len(res), 0)
	assert.Equal(t, err, errno.NewError(errno.JsonPathIllegal))

	jsonPath = "$."
	res, err = query.SplitPath(jsonPath)
	assert.Equal(t, len(res), 0)
	assert.Equal(t, err, errno.NewError(errno.JsonPathIllegal))
}

func TestIpFunctionIpMaskFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "ip_mask"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{int64(12), "112.170.22.41"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"112.170.22.41", "a"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"112.170.22.41", int64(12), "12"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"112.170.22.41", int64(64)})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"2001:db8:85a3::", int64(10), int64(200)})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"322.170.22.41", int64(12)})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"q001:0db8:85a3::", int64(12), int64(64)})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	//// Correct test cases
	inputArgs := make([]interface{}, 10)
	inputArgs2 := [10]int{16, 8, 12, 0, 32, 0, 12, 24, 68, 128}
	inputArgs3 := [10]int{8, 12, 16, 24, 0, 64, 24, 32, 88, 40}
	inputArgs[0] = "192.168.1.1"
	inputArgs[1] = "10.0.0.1"
	inputArgs[2] = "172.16.254.1"
	inputArgs[3] = "8.8.8.8"
	inputArgs[4] = "203.0.113.5"
	inputArgs[5] = "1a2b:3c4d:5e6f:7036:9ef5:ffff:0521:424e"
	inputArgs[6] = "6789:abcd:ef01:2345:6789:abcd:ef01:2345"
	inputArgs[7] = "0:0:0:0:0:0:0:1"
	inputArgs[8] = "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
	inputArgs[9] = "fe80::1ff:fe23:4567:890a"
	expects := make([]interface{}, 20)
	expects[0] = "192.168.0.0"
	expects[1] = "10.0.0.0"
	expects[2] = "172.16.0.0"
	expects[3] = "0.0.0.0"
	expects[4] = "203.0.113.5"
	expects[5] = "::"
	expects[6] = "6780::"
	expects[7] = "::"
	expects[8] = "2001:db8:85a3::"
	expects[9] = "fe80::1ff:fe23:4567:890a"
	expects[10] = "192.168.0.0"
	expects[11] = "10.0.0.0"
	expects[12] = "172.16.0.0"
	expects[13] = "0.0.0.0"
	expects[14] = "203.0.113.5"
	expects[15] = "1a2b:3c4d:5e6f:7036::"
	expects[16] = "6789:ab00::"
	expects[17] = "::"
	expects[18] = "2001:db8:85a3::8a00:0:0"
	expects[19] = "fe80::"
	outputs := make([]interface{}, 0, len(expects))
	for i, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, int64(inputArgs2[i])}); ok {
			outputs = append(outputs, out)
		}
	}
	for i, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, int64(inputArgs2[i]), int64(inputArgs3[i])}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestIpFunctionIpToDomainFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "ip_to_domain"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"string", 15})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	out, ok = urlValuer.Call(inputName, []interface{}{"https://www.example.com:8080"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "112.170.22.41"
	inputArgs[1] = "172.16.0.0"
	inputArgs[2] = "162.16.0.1"
	expects := make([]interface{}, 3)
	expects[0] = "internet"
	expects[1] = "intranet"
	expects[2] = "internet"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestIpFunctionIpPrefixFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "ip_prefix"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"112.170.22.41", int64(64)})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"invalid ip", int64(24)})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "192.168.1.5"
	inputArgs[1] = "192.168.1.10"
	inputArgs[2] = "162.16.0.1"
	expects := make([]interface{}, 3)
	expects[0] = "192.168.1.0/24"
	expects[1] = "192.168.1.0/24"
	expects[2] = "162.16.0.0/24"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, int64(24)}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestIsSubnetOfFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "is_subnet_of"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"112.170.22.41", 24})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"invalid network", "192.168.1.0"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid CIDR address", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "192.168.1.5"
	inputArgs[1] = "192.168.1.10"
	inputArgs[2] = "162.16.0.1"
	expects := make([]interface{}, 3)
	expects[0] = "true"
	expects[1] = "true"
	expects[2] = "false"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{"192.168.1.0/24", arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestIpSubnetMinFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "ip_subnet_min"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"invalid ip"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	out, ok = urlValuer.Call(inputName, []interface{}{"192.168.1.5"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "192.168.1.5/24"
	inputArgs[1] = "192.168.1.10/24"
	inputArgs[2] = "162.16.0.12/24"
	expects := make([]interface{}, 3)
	expects[0] = "192.168.1.0"
	expects[1] = "192.168.1.0"
	expects[2] = "162.16.0.0"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestIpSubnetMaxFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "ip_subnet_max"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"invalid ip"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	out, ok = urlValuer.Call(inputName, []interface{}{"192.168.1.5"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "192.168.1.5/24"
	inputArgs[1] = "192.168.1.10/24"
	inputArgs[2] = "162.16.0.12/24"
	expects := make([]interface{}, 3)
	expects[0] = "192.168.1.255"
	expects[1] = "192.168.1.255"
	expects[2] = "162.16.0.255"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestIpSubnetRangeFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "ip_subnet_range"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"invalid ip"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	out, ok = urlValuer.Call(inputName, []interface{}{"192.168.1.5"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "invalid ip", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "192.168.1.5/24"
	inputArgs[1] = "192.168.1.10/24"
	inputArgs[2] = "162.16.0.12/24"
	expects := make([]interface{}, 3)
	expects[0] = `["192.168.1.0", "192.168.1.255"]`
	expects[1] = `["192.168.1.0", "192.168.1.255"]`
	expects[2] = `["162.16.0.0", "162.16.0.255"]`
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestMobileCarrierFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "mobile_carrier"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"invalid phone"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "未知运营商", out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "13200000000"
	inputArgs[1] = "18800000000"
	inputArgs[2] = "17300000000"
	expects := make([]interface{}, 3)
	expects[0] = `中国联通`
	expects[1] = `中国移动`
	expects[2] = `中国电信`
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestLevenshteinDistanceFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "levenshtein_distance"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{12, 15})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{"str1", 15})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{12, "str2"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "str1"
	inputArgs[1] = "before str1"
	inputArgs[2] = "str1 last"
	expects := make([]interface{}, 3)
	expects[0] = "1"
	expects[1] = "8"
	expects[2] = "6"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, "str2"}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestTypeofFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "typeof"

	inputArgs := make([]interface{}, 3)
	inputArgs[0] = "str1"
	inputArgs[1] = true
	inputArgs[2] = 12.5
	expects := make([]interface{}, 3)
	expects[0] = "string"
	expects[1] = "bool"
	expects[2] = "float64"
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, "str2"}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestRegexpExtract(t *testing.T) {
	// Wrong test case
	input := "HTTP/2.0"
	regex := `(\d).(\d)`
	group := int64(0)
	res, err := query.RegexpExtract(input, regex, group+3)
	assert.Equal(t, "capture group 3 out of bounds", err.Error())
	assert.Equal(t, "", res)

	input = "/request/path-3/file-5"
	regex = `.*/(file.*)`
	res, err = query.RegexpExtract(input, regex, group)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, "file-5", res)

	input = "/request/path-3/file-5"
	regex = `**`
	res, err = query.RegexpExtract(input, regex, group)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, "", res)

	// Correct test cases
	input = "HTTP/2.0"
	regex = `\d+`
	res, err = query.RegexpExtract(input, regex, group)
	assert.Equal(t, nil, err)
	assert.Equal(t, "2", res)

	input = "/request/path-3/file-5"
	regex = `(\d)/file-(\d)`
	res, err = query.RegexpExtract(input, regex, group+2)
	assert.Equal(t, nil, err)
	assert.Equal(t, "5", res)

	input = "error'1232"
	regex = `\d+`
	res, err = query.RegexpExtract(input, regex, group)
	assert.Equal(t, nil, err)
	assert.Equal(t, "1232", res)
}

func TestJsonExtractScalarFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "json_extract_scalar"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{"this is not json", "$.0"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	out, ok = urlValuer.Call(inputName, []interface{}{`{"field1":"value"}`, "$.wrong_field"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "null", out)

	out, ok = urlValuer.Call(inputName, []interface{}{`{"field1":"value"}`, "$.field1"})
	assert.Equal(t, true, ok)
	assert.NotEqual(t, `"value"`, out)
	assert.Equal(t, `value`, out)

	// Correct test cases
	inputArgs := make([]interface{}, 4)
	inputArgs[0] = `[{"EndTime":1626314920},{"FireResult":2}]`
	inputArgs[1] = `{"EndTime":1626314921, "nest":{"field1":"field1", "array":["number","int"]}}`
	inputArgs[2] = `{"EndTime":1626314922, "nest":{"field1":"field1", "field2":{"nest_field":12}}}`
	inputArgs[3] = `{"EndTime":1626314922, "nest":{"array":[{"type":[1,2]},"int"]}}`
	jsonPath := make([]interface{}, 4)
	jsonPath[0] = `$.0.EndTime`
	jsonPath[1] = "$.nest.array[1]"
	jsonPath[2] = "$.nest.field2"
	jsonPath[3] = "$.nest.array[0].type[1]"

	expects := make([]interface{}, 4)
	expects[0] = "1626314920"
	expects[1] = `int`
	expects[2] = `{"nest_field":12}`
	expects[3] = `2`
	outputs := make([]interface{}, 0, len(expects))
	for i, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, jsonPath[i]}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestRegexpExtractFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "regexp_extract"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{"There's no number here.", `\d`})
	assert.Equal(t, false, ok)
	assert.Equal(t, "there is no matching regular expression", out)

	out, ok = urlValuer.Call(inputName, []interface{}{"wrong regex", "**"})
	assert.Equal(t, false, ok)
	assert.Equal(t, "error parsing regexp: missing argument to repetition operator: `*`", out)

	// Correct test cases
	inputArgs := make([]interface{}, 4)
	inputArgs[0] = "HTTP/2.0"
	inputArgs[1] = "number is 15"
	inputArgs[2] = "/request/path-3/file-5"
	inputArgs[3] = "error'1232"

	regex := make([]interface{}, 4)
	regex[0] = `\d+`
	regex[1] = `number is (\d)`
	regex[2] = `(\d)/file-(\d)`
	regex[3] = `(\d)(\d)(\d)(\d)`

	expects := make([]interface{}, 4)
	expects[0] = "2"
	expects[1] = "1"
	expects[2] = "5"
	expects[3] = "3"
	outputs := make([]interface{}, 0, len(expects))
	for i, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, regex[i], int64(i)}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestRegexpLikeFunc(t *testing.T) {
	urlValuer := query.StringValuer{}
	inputName := "regexp_like"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{"There's no number here.", `\d`})
	assert.Equal(t, true, ok)
	assert.Equal(t, "false", out)

	out, ok = urlValuer.Call(inputName, []interface{}{"wrong regex", "**"})
	assert.Equal(t, true, ok)
	assert.Equal(t, "false", out)

	// Correct test cases
	inputArgs := make([]interface{}, 4)
	inputArgs[0] = "HTTP/2.0"
	inputArgs[1] = "number is 15"
	inputArgs[2] = "/request/path-3/file-5"
	inputArgs[3] = "error'1232"

	regex := make([]interface{}, 4)
	regex[0] = `\d+`
	regex[1] = `number is (\d)`
	regex[2] = `(\d)/file-(\d)`
	regex[3] = `(\d)(\d)(\d)(\d)`

	expects := make([]interface{}, 4)
	expects[0] = "true"
	expects[1] = "true"
	expects[2] = "true"
	expects[3] = "true"
	outputs := make([]interface{}, 0, len(expects))
	for i, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg, regex[i]}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}
