package query

// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	json "github.com/json-iterator/go"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"go.uber.org/zap"
)

var (
	_ = RegistryMaterializeFunction("levenshtein_distance", &levenshteinDistanceFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("lpad", &lpadFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("regexp_extract", &regexpExtractFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("regexp_like", &regexpLikeFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("replace", &replaceFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("reverse", &reverseFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("rpad", &rpadFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("split", &splitFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("split_part", &splitPartFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("split_to_map", &splitToMapFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("str", &strFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("strlen", &strLenFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("strpos", &strPosFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("substr", &subStrFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("typeof", &typeofFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
)

// URL
var (
	_ = RegistryMaterializeFunction("url_decode", &urlDecodeFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_encode", &urlEncodeFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_extract_fragment", &urlExtractFragmentFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_extract_host", &urlExtractHostFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_extract_parameter", &urlExtractParameterFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_extract_path", &urlExtractPathFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_extract_port", &urlExtractPortFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_extract_protocol", &urlExtractProtocolFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("url_extract_query", &urlExtractQueryFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
)

// JSON
var (
	_ = RegistryMaterializeFunction("json_extract", &jsonExtractFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("json_extract_scalar", &jsonExtractScalarFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
)

// IP
var (
	_ = RegistryMaterializeFunction("ip_to_domain", &ipToDomainFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("ip_prefix", &ipPrefixFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("is_subnet_of", &isSubnetOfFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("ip_subnet_min", &ipSubnetMinFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("ip_subnet_max", &ipSubnetMaxFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("ip_subnet_range", &ipSubnetRangeFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
)

// PHONE
var (
	_ = RegistryMaterializeFunction("mobile_carrier", &mobileCarrierFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
)

func GetStringFunction(name string) MaterializeFunc {
	materialize, ok := GetFunctionFactoryInstance().FindMaterFunc(name)
	if ok && materialize.GetFuncType() == STRING {
		return materialize
	}
	return nil
}

func compileAllStringArgs(expr *influxql.Call, c *compiledField) error {
	// Compile all the argument expressions that are not just literals.
	for _, arg := range expr.Args {
		if _, ok := arg.(influxql.Literal); ok {
			continue
		}
		if err := c.compileExpr(arg); err != nil {
			return err
		}
	}
	return nil
}

type levenshteinDistanceFunc struct {
	BaseInfo
}

func (s *levenshteinDistanceFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if got := len(expr.Args); len(expr.Args) != 2 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "2", got)
	}

	return compileAllStringArgs(expr, c)
}

func (s *levenshteinDistanceFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	if args[1] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, args[1])
	}

	return influxql.String, nil
}

func (s *levenshteinDistanceFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := args[0].(string)
	if !ok {
		return nil, false
	}
	arg1, ok := args[1].(string)
	if !ok {
		return nil, false
	}
	return minDistance(arg0, arg1), true
}

func minDistance(str1 string, str2 string) string {
	m, n := len(str1), len(str2)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	for i := 0; i <= m; i++ {
		dp[i][0] = i
	}
	for j := 0; j <= n; j++ {
		dp[0][j] = j
	}

	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if str1[i-1] == str2[j-1] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = min(min(dp[i-1][j], dp[i][j-1]), dp[i-1][j-1]) + 1
			}
		}
	}

	return strconv.Itoa(dp[m][n])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type lpadFunc struct {
	BaseInfo
}

func (s *lpadFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "lpad" {
		return fmt.Errorf("invalid name, expected %s, got %s", "lpad", expr.Name)
	}

	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) != 3 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "3", got)
	}

	if _, ok := expr.Args[1].(*influxql.IntegerLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}
	if _, ok := expr.Args[2].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the third argument in %s(): %s", expr.Name, expr.Args[2])
	}

	return compileAllStringArgs(expr, c)
}

func (s *lpadFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *lpadFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	return LpadString(args[0].(string), args[2].(string), int(args[1].(int64))), true
}

func LpadString(s, padStr string, length int) string {
	if length <= 0 || padStr == "" {
		return s
	}

	// Calculate the number of padding characters we need
	numPadChars := length - len(s)
	if numPadChars <= 0 {
		return s[:length]
	}

	// Repeat the padding string until we reach the desired length
	padding := strings.Repeat(padStr, (numPadChars+len(padStr)-1)/len(padStr))
	return padding[:numPadChars] + s
}

type regexpExtractFunc struct {
	BaseInfo
}

func (s *regexpExtractFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "regexp_extract" {
		return fmt.Errorf("invalid name, expected %s, got %s", "regexp_extract", expr.Name)
	}

	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) < 2 || len(expr.Args) > 3 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "2-3", got)
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}

	if len(expr.Args) == 3 {
		if _, ok := expr.Args[2].(*influxql.IntegerLiteral); !ok {
			return fmt.Errorf("invalid argument type for the third argument in %s(): %s", expr.Name, expr.Args[2])
		}
	}

	return compileAllStringArgs(expr, c)
}

func (s *regexpExtractFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.String, nil
}

func (s *regexpExtractFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	group := int64(0)
	if len(args) == 3 {
		group = args[2].(int64)
	}

	res, err := RegexpExtract(args[0].(string), args[1].(string), group)
	if err != nil {
		logger.GetLogger().Error("regular extraction error", zap.Error(err))
		return err.Error(), false
	}
	return res, true
}

func RegexpExtract(input string, regex string, group int64) (string, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return "", err
	}

	matches := re.FindStringSubmatch(input)
	if len(matches) == 0 {
		return "", fmt.Errorf("there is no matching regular expression")
	}
	if len(matches) <= int(group) || group < 0 {
		return "", fmt.Errorf("capture group %d out of bounds", group)
	}

	return matches[group], nil
}

type regexpLikeFunc struct {
	BaseInfo
}

func (s *regexpLikeFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "regexp_like" {
		return fmt.Errorf("invalid name, expected %s, got %s", "regexp_like", expr.Name)
	}

	// Did we get the expected number of args?
	if got := len(expr.Args); got != 2 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "2", got)
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}

	return compileAllStringArgs(expr, c)
}

func (s *regexpLikeFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.String, nil
}

func (s *regexpLikeFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	like := RegexpLike(args[0].(string), args[1].(string))
	if like {
		return "true", true
	} else {
		return "false", true
	}
}

func RegexpLike(input string, regex string) bool {
	re, err := regexp.Compile(regex)
	if err != nil {
		return false
	}

	return re.MatchString(input)
}

type replaceFunc struct {
	BaseInfo
}

func (s *replaceFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "replace" {
		return fmt.Errorf("invalid name, expected %s, got %s", "replace", expr.Name)
	}

	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) < 2 || len(expr.Args) > 3 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "2-3", got)
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}
	if len(expr.Args) == 3 {
		if _, ok := expr.Args[2].(*influxql.StringLiteral); !ok {
			return fmt.Errorf("invalid argument type for the third argument in %s(): %s", expr.Name, expr.Args[2])
		}
	}

	return compileAllStringArgs(expr, c)
}

func (s *replaceFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *replaceFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) == 2 {
		return strings.ReplaceAll(args[0].(string), args[1].(string), ""), true
	}

	return strings.ReplaceAll(args[0].(string), args[1].(string), args[2].(string)), true
}

type reverseFunc struct {
	BaseInfo
}

func (s *reverseFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *reverseFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *reverseFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		return ReverseString(arg0), true
	}
	return nil, false
}

func ReverseString(s string) string {
	runeSlice := []rune(s)
	for i, j := 0, len(runeSlice)-1; i < j; i, j = i+1, j-1 {
		runeSlice[i], runeSlice[j] = runeSlice[j], runeSlice[i]
	}
	return string(runeSlice)
}

type rpadFunc struct {
	BaseInfo
}

func (s *rpadFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "rpad" {
		return fmt.Errorf("invalid name, expected %s, got %s", "rpad", expr.Name)
	}

	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) != 3 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "3", got)
	}

	if _, ok := expr.Args[1].(*influxql.IntegerLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}
	if _, ok := expr.Args[2].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the third argument in %s(): %s", expr.Name, expr.Args[2])
	}

	return compileAllStringArgs(expr, c)
}

func (s *rpadFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *rpadFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	return RpadString(args[0].(string), args[2].(string), int(args[1].(int64))), true
}

func RpadString(s, padStr string, length int) string {
	if length <= 0 || padStr == "" {
		return s
	}

	// Calculate the number of padding characters we need
	numPadChars := length - len(s)
	if numPadChars <= 0 {
		return s[:length]
	}

	// Repeat the padding string until we reach the desired length
	padding := strings.Repeat(padStr, (numPadChars+len(padStr)-1)/len(padStr))
	return s + padding[:numPadChars]
}

type splitFunc struct {
	BaseInfo
}

func (s *splitFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "split" {
		return fmt.Errorf("invalid name, expected %s, got %s", "split", expr.Name)
	}

	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) < 2 || len(expr.Args) > 3 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "2-3", got)
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}
	if len(expr.Args) == 3 {
		if _, ok := expr.Args[2].(*influxql.IntegerLiteral); !ok {
			return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[2])
		}
	}

	return compileAllStringArgs(expr, c)
}

func (s *splitFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *splitFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) == 2 {
		return SplitString(args[0].(string), args[1].(string), -1), true
	}

	return SplitString(args[0].(string), args[1].(string), int(args[2].(int64))), true
}

func SplitString(s, seq string, n int) string {
	res, _ := json.MarshalToString(strings.SplitN(s, seq, n))
	return res
}

type splitPartFunc struct {
	BaseInfo
}

func (s *splitPartFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "split_part" {
		return fmt.Errorf("invalid name, expected %s, got %s", "split_part", expr.Name)
	}

	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) != 3 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "3", got)
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}
	if _, ok := expr.Args[2].(*influxql.IntegerLiteral); !ok {
		return fmt.Errorf("invalid argument type for the third argument in %s(): %s", expr.Name, expr.Args[2])
	}

	return compileAllStringArgs(expr, c)
}

func (s *splitPartFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *splitPartFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	return SplitPart(args[0].(string), args[1].(string), int(args[2].(int64))), true
}

func SplitPart(str, delimiter string, partIndex int) string {
	parts := strings.Split(str, delimiter)
	if partIndex >= 1 && partIndex <= len(parts) {
		return parts[partIndex-1]
	}
	return ""
}

type splitToMapFunc struct {
	BaseInfo
}

func (s *splitToMapFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) != 3 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "3", got)
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}
	if _, ok := expr.Args[2].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the third argument in %s(): %s", expr.Name, expr.Args[2])
	}

	return compileAllStringArgs(expr, c)
}

func (s *splitToMapFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *splitToMapFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	return SplitToMap(args[0].(string), args[1].(string), args[2].(string)), true
}

func SplitToMap(str, delimiter1, delimiter2 string) string {
	if len(str) < 2 {
		return str
	}

	res := make([]byte, 0, len(str))
	res = append(res, '{')
	parts := strings.Split(str, delimiter1)
	isFirst := true
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		if !isFirst {
			res = append(res, ',')
		}
		kv := strings.SplitN(part, delimiter2, 2)
		if len(kv) == 1 {
			res = append(res, `"`+kv[0]+`":""`...)
		} else {
			res = append(res, `"`+kv[0]+`":"`+kv[1]+`"`...)
		}
		isFirst = false
	}
	res = append(res, '}')

	return util.Bytes2str(res)
}

type strFunc struct {
	BaseInfo
}

func (s *strFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if got := len(expr.Args); got != 2 {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, 2, got)
	}
	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("expected string argument in str()")
	}
	return compileAllStringArgs(expr, c)
}

func (s *strFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0, arg1 influxql.DataType
	if len(args) != 2 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0, arg1 = args[0], args[1]

	switch arg0 {
	case influxql.String:
		// Pass through to verify the second argument.
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}

	switch arg1 {
	case influxql.String:
		return influxql.Boolean, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg0)
	}
}

func (s *strFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) != 2 {
		return nil, false
	}
	arg0, ok := args[0].(string)
	if !ok {
		return nil, true
	}
	arg1, ok := args[1].(string)
	if !ok {
		return nil, true
	}
	return strings.Contains(arg0, arg1), true
}

type strLenFunc struct {
	BaseInfo
}

func (s *strLenFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileAllStringArgs(expr, c)
}

func (s *strLenFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0 influxql.DataType
	if len(args) != 1 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0 = args[0]
	switch arg0 {
	case influxql.String:
		return influxql.Integer, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}
}

func (s *strLenFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) != 1 {
		return nil, false
	}
	if arg0, ok := args[0].(string); ok {
		return int64(len(arg0)), true
	}
	return nil, true
}

type strPosFunc struct {
	BaseInfo
}

func (s *strPosFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if got := len(expr.Args); len(expr.Args) != 2 {
		return fmt.Errorf("invalid number of arguments for %s, expected %s, got %d", expr.Name, "2", got)
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}

	return compileAllStringArgs(expr, c)
}

func (s *strPosFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *strPosFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	return strconv.Itoa(strings.Index(args[0].(string), args[1].(string)) + 1), true
}

// subStr
type subStrFunc struct {
	BaseInfo
}

func (s *subStrFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	const NARGS = 1
	// Did we get the expected number of args?
	if got := len(expr.Args); expr.Name == "substr" && (len(expr.Args) < 2 || len(expr.Args) > 3) {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, NARGS, got)
	}

	if got := len(expr.Args); expr.Name != "substr" && got != 1 {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, NARGS, got)
	}

	if second, ok := expr.Args[1].(*influxql.IntegerLiteral); !ok || second.Val < 0 {
		return fmt.Errorf("expected non-gegative integer argument in substr()")
	}
	if len(expr.Args) == 3 {
		if third, ok := expr.Args[2].(*influxql.IntegerLiteral); !ok || third.Val < 0 {
			return fmt.Errorf("expected non-gegative integer argument in substr()")
		}
	}
	return compileAllStringArgs(expr, c)
}

func (s *subStrFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0, arg1, arg2 influxql.DataType
	if len(args) < 2 || len(args) > 3 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0, arg1 = args[0], args[1]
	if len(args) == 3 {
		arg2 = args[2]
	}

	switch arg0 {
	case influxql.String:
		// Pass through to verify the second argument.
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}

	switch arg1 {
	case influxql.Integer:
		if len(args) == 2 {
			return influxql.String, nil
		}
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg0)
	}

	switch arg2 {
	case influxql.Integer:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the third argument in %s(): %s", name, arg0)
	}
}

func (s *subStrFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	var (
		arg0 string
		arg1 int64
		ok   bool
	)
	arg0, ok = args[0].(string)
	if !ok {
		return nil, true
	}
	arg1, ok = args[1].(int64)
	if !ok {
		return nil, true
	}
	if len(args) == 2 {
		return SubStrTwoParaFunc(arg0, arg1), true
	}
	if len(args) == 3 {
		arg2, ok := args[2].(int64)
		if !ok {
			return nil, true
		}
		return SubStrThreeParaFunc(arg0, arg1, arg2), true
	}
	return nil, false
}

type typeofFunc struct {
	BaseInfo
}

func (s *typeofFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *typeofFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.String, nil
}

func (s *typeofFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch args[0].(type) {
	case string:
		return "string", true
	case int64:
		return "int64", true
	case float64:
		return "float64", true
	case bool:
		return "bool", true
	}

	return nil, false
}

func SubStrTwoParaFunc(srcStr string, start int64) string {
	if start > int64(len(srcStr)) {
		return ""
	}
	oriStr := srcStr[start:]
	newStr := make([]byte, len(oriStr))
	copy(newStr, oriStr)
	return util.Bytes2str(newStr)
}

func SubStrThreeParaFunc(srcStr string, start, subStrLen int64) string {
	if start >= int64(len(srcStr)) {
		return ""
	}
	var oriStr string
	if start+subStrLen >= int64(len(srcStr)) {
		oriStr = srcStr[start:]
	} else {
		oriStr = srcStr[start : start+subStrLen]
	}
	newStr := make([]byte, len(oriStr))
	copy(newStr, oriStr)
	return util.Bytes2str(newStr)
}

// URL
type urlDecodeFunc struct {
	BaseInfo
}

func (u *urlDecodeFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlDecodeFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlDecodeFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		res, err := url.QueryUnescape(arg0)
		if err != nil {
			return nil, false
		} else {
			return res, true
		}
	}
	return nil, false
}

type urlEncodeFunc struct {
	BaseInfo
}

func (u *urlEncodeFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlEncodeFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlEncodeFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		return url.QueryEscape(arg0), true
	}
	return nil, false
}

type urlExtractFragmentFunc struct {
	BaseInfo
}

func (u *urlExtractFragmentFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlExtractFragmentFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlExtractFragmentFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		s, err := url.Parse(arg0)
		if err != nil {
			return nil, false
		}
		return s.Fragment, true
	}

	return nil, false
}

type urlExtractHostFunc struct {
	BaseInfo
}

func (u *urlExtractHostFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlExtractHostFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlExtractHostFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		s, err := url.Parse(arg0)
		if err != nil {
			return nil, false
		}
		return s.Host, true
	}

	return nil, false
}

type urlExtractParameterFunc struct {
	BaseInfo
}

func (u *urlExtractParameterFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 2 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlExtractParameterFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	if args[1] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(), "+
			"the expected parameter type is string, and the string parameter needs to be quoted in single quotes", name)
	}

	return influxql.String, nil
}

func (u *urlExtractParameterFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		s, err := url.Parse(arg0)
		if err != nil {
			return nil, false
		}
		arg1, ok := args[1].(string)
		if !ok {
			return nil, false
		}

		return s.Query().Get(arg1), true
	}

	return nil, false
}

type urlExtractPathFunc struct {
	BaseInfo
}

func (u *urlExtractPathFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlExtractPathFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlExtractPathFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		s, err := url.Parse(arg0)
		if err != nil {
			return nil, false
		}
		return s.Path, true
	}

	return nil, false
}

type urlExtractPortFunc struct {
	BaseInfo
}

func (u *urlExtractPortFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlExtractPortFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlExtractPortFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		s, err := url.Parse(arg0)
		if err != nil {
			return nil, false
		}
		return s.Port(), true
	}

	return nil, false
}

type urlExtractProtocolFunc struct {
	BaseInfo
}

func (u *urlExtractProtocolFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlExtractProtocolFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlExtractProtocolFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		s, err := url.Parse(arg0)
		if err != nil {
			return nil, false
		}
		return s.Scheme, true
	}

	return nil, false
}

type urlExtractQueryFunc struct {
	BaseInfo
}

func (u *urlExtractQueryFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *urlExtractQueryFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (u *urlExtractQueryFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		s, err := url.Parse(arg0)
		if err != nil {
			return nil, false
		}
		return s.RawQuery, true
	}

	return nil, false
}

// JSON
type jsonExtractFunc struct {
	BaseInfo
}

func (u *jsonExtractFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 2 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (u *jsonExtractFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	if args[1] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, args[1])
	}

	return influxql.String, nil
}

func (u *jsonExtractFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		arg1, ok := args[1].(string)
		if !ok {
			return nil, false
		}
		result, err := JsonExtract(arg0, arg1)
		if err != nil {
			return nil, false
		}
		return result, true
	}

	return nil, false
}

func JsonExtract(jsonStr, jsonPath string) (string, error) {
	pathParts, err := SplitPath(jsonPath)
	if err != nil {
		return "", err
	}

	if jsonStr[0] == '[' && jsonStr[len(jsonStr)-1] == ']' {
		var data []interface{}
		err = json.UnmarshalFromString(jsonStr, &data)
		if err != nil {
			return "", err
		}
		loc, err := strconv.Atoi(pathParts[0])
		if err != nil || loc >= len(data) {
			return "", errno.NewError(errno.JsonPathIllegal)
		}
		return GetJsonValue(data[loc], pathParts[1:])
	}

	var data map[string]interface{}
	err = json.UnmarshalFromString(jsonStr, &data)
	if err != nil {
		return "", err
	}

	return GetJsonValue(data, pathParts)
}

func GetJsonValue(value interface{}, pathParts []string) (string, error) {
	if len(pathParts) == 0 {
		b, err := json.MarshalToString(value)
		return b, err
	}

	v, ok := value.(map[string]interface{})
	if !ok {
		return "", errno.NewError(errno.JsonPathIllegal)
	}

	key := pathParts[0]
	if key[len(key)-1] == ']' {
		split := strings.Split(key, "[")
		if len(split) != 2 {
			return "", errno.NewError(errno.JsonPathIllegal)
		}
		i, err := strconv.Atoi(split[1][:len(split[1])-1])
		if err != nil {
			return "", errno.NewError(errno.JsonPathIllegal)
		}

		vv, ok := v[split[0]].([]interface{})
		if !ok {
			return "", errno.NewError(errno.JsonPathIllegal)
		} else {
			if len(vv) > i {
				return GetJsonValue(vv[i], pathParts[1:])
			}
			return "", errno.NewError(errno.JsonPathIllegal)
		}
	}

	return GetJsonValue(v[key], pathParts[1:])
}

func SplitPath(jsonPath string) ([]string, error) {
	pathParts := strings.Split(jsonPath, ".")
	if len(pathParts) < 2 || pathParts[0] != "$" || pathParts[1] == "" {
		return nil, errno.NewError(errno.JsonPathIllegal)
	}
	return pathParts[1:], nil
}

type jsonExtractScalarFunc struct {
	BaseInfo
}

func (u *jsonExtractScalarFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Did we get the expected number of args?
	if len(expr.Args) != 2 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("invalid argument type for the second argument in %s(): %s", expr.Name, expr.Args[1])
	}

	return compileAllStringArgs(expr, c)
}

func (u *jsonExtractScalarFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.String, nil
}

func (u *jsonExtractScalarFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		arg1, ok := args[1].(string)
		if !ok {
			return nil, false
		}
		result, err := JsonExtract(arg0, arg1)
		if err != nil {
			return nil, false
		}

		if len(result) >= 2 && result[0] == '"' && result[len(result)-1] == '"' {
			result = result[1 : len(result)-1]
		}
		return result, true
	}

	return nil, false
}

// IP
type ipToDomainFunc struct {
	BaseInfo
}

func (s *ipToDomainFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *ipToDomainFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *ipToDomainFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := args[0].(string); ok {
		return ipToDomain(arg0), true
	}
	return nil, false
}

func ipToDomain(ip string) string {
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"169.254.0.0/16",
		"127.0.0.0/8",
	}

	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return "invalid ip"
	}
	for _, privateRange := range privateRanges {
		_, network, _ := net.ParseCIDR(privateRange)
		if network.Contains(ipAddr) {
			return "intranet"
		}
	}

	return "internet"
}

type ipPrefixFunc struct {
	BaseInfo
}

func (s *ipPrefixFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 2 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *ipPrefixFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	if args[1] != influxql.Integer {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, args[1])
	}

	return influxql.String, nil
}

func (s *ipPrefixFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := args[0].(string)
	if !ok {
		return nil, false
	}
	arg1, ok := args[1].(int64)
	if !ok {
		return nil, false
	}

	res, err := ipPrefix(arg0, int(arg1))
	if err != nil {
		return nil, false
	}
	return res, true
}

func ipPrefix(ipAddress string, prefixLen int) (string, error) {
	if prefixLen < 0 || prefixLen > 32 {
		return "", errors.New("prefix out of range (0-32)")
	}

	ip := net.ParseIP(ipAddress)
	if ip == nil {
		return "", errors.New("invalid ip")
	}
	mask := net.CIDRMask(prefixLen, 32)
	ipMask := ip.Mask(mask)
	res := fmt.Sprintf("%s/%d", ipMask, prefixLen)
	return res, nil
}

type isSubnetOfFunc struct {
	BaseInfo
}

func (s *isSubnetOfFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 2 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *isSubnetOfFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	if args[1] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, args[1])
	}

	return influxql.String, nil
}

func (s *isSubnetOfFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := args[0].(string)
	if !ok {
		return nil, false
	}
	arg1, ok := args[1].(string)
	if !ok {
		return nil, false
	}
	res, err := isSubnetOf(arg0, arg1)
	if err != nil {
		logger.GetLogger().Error("isSubnetOf operator get invalid CIDR address", zap.Error(err))
		return "invalid CIDR address", true
	} else {
		return util.Bool2str(res), true
	}
}

func isSubnetOf(network, ip string) (bool, error) {
	ipAddr := net.ParseIP(ip)
	_, networkAddr, err := net.ParseCIDR(network)
	if err != nil {
		return false, err
	}
	return networkAddr.Contains(ipAddr), nil
}

type ipSubnetMinFunc struct {
	BaseInfo
}

func (s *ipSubnetMinFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *ipSubnetMinFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *ipSubnetMinFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := args[0].(string)
	if !ok {
		return nil, false
	}

	return ipSubnetMin(arg0), true
}

func ipSubnetMin(network string) string {
	_, ipNet, err := net.ParseCIDR(network)
	if err != nil {
		return "invalid ip"
	}

	var min net.IP
	if len(ipNet.IP) == net.IPv4len {
		min = ipNet.IP.Mask(ipNet.Mask)
	}
	return min.String()
}

type ipSubnetMaxFunc struct {
	BaseInfo
}

func (s *ipSubnetMaxFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *ipSubnetMaxFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *ipSubnetMaxFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := args[0].(string)
	if !ok {
		return nil, false
	}

	return ipSubnetMax(arg0), true
}

func ipSubnetMax(network string) string {
	_, ipNet, err := net.ParseCIDR(network)
	if err != nil {
		return "invalid ip"
	}

	ip := ipNet.IP.Mask(ipNet.Mask).To4()
	ip[len(ip)-1] = 255
	return ip.String()
}

type ipSubnetRangeFunc struct {
	BaseInfo
}

func (s *ipSubnetRangeFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *ipSubnetRangeFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *ipSubnetRangeFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := args[0].(string)
	if !ok {
		return nil, false
	}

	return ipSubnetRange(arg0), true
}

func ipSubnetRange(network string) string {
	min := ipSubnetMin(network)
	if min == "invalid ip" {
		return min
	}

	max := ipSubnetMax(network)
	return fmt.Sprint(`["`, min, `", "`, max, `"]`)
}

// PHONE
type mobileCarrierFunc struct {
	BaseInfo
}

func (s *mobileCarrierFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileAllStringArgs(expr, c)
}

func (s *mobileCarrierFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *mobileCarrierFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := args[0].(string)
	if !ok {
		return nil, false
	}

	return getMobileCarrier(arg0), true
}

func getMobileCarrier(phone string) string {
	ChinaTelecom := `^133|^149|^153|^173|^177|^180|^181|^189|^190|^191|^193|^199|^1700|^1701|^1702|^162`
	reg := regexp.MustCompile(ChinaTelecom)
	if reg.MatchString(phone) {
		return "中国电信"
	}
	ChinaUnicom := `^130|^131|^132|^145|^155|^156|^166|^167|^171|^175|^176|^185|^186|^196|^1704|^1707|^1708|^1709`
	reg = regexp.MustCompile(ChinaUnicom)
	if reg.MatchString(phone) {
		return "中国联通"
	}
	ChinaMobile := `^134|^135|^136|^137|^138|^139|^147|^148|^150|^151|^152|^157|^158|^159|^172|^178|^182|^183|` +
		`^184|^187|^188|^195|^197|^198|^1440|^1703|^1705|^1706|^165`
	reg = regexp.MustCompile(ChinaMobile)
	if reg.MatchString(phone) {
		return "中国移动"
	}

	ChinaBroadcastNetwork := `^192`
	reg = regexp.MustCompile(ChinaBroadcastNetwork)
	if reg.MatchString(phone) {
		return "中国广电"
	}
	SatelliteCommunication := `^1349|^174`
	reg = regexp.MustCompile(SatelliteCommunication)
	if reg.MatchString(phone) {
		return "卫星通信"
	}

	return "未知运营商"
}

// type mapper
type StringFunctionTypeMapper struct{}

func (m StringFunctionTypeMapper) MapType(_ *influxql.Measurement, _ string) influxql.DataType {
	return influxql.Unknown
}

func (m StringFunctionTypeMapper) MapTypeBatch(_ *influxql.Measurement, _ map[string]*influxql.FieldNameSpace, _ *influxql.Schema) error {
	return nil
}

func (m StringFunctionTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if stringFunc := GetStringFunction(name); stringFunc != nil {
		return stringFunc.CallTypeFunc(name, args)
	}
	return influxql.Unknown, nil
}

// valuer
type StringValuer struct{}

var _ influxql.CallValuer = StringValuer{}

func (StringValuer) Value(_ string) (interface{}, bool) {
	return nil, false
}

func (StringValuer) SetValuer(_ influxql.Valuer, _ int) {
}

func (v StringValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if stringFunc := GetMaterializeFunction(name, STRING); stringFunc != nil {
		return stringFunc.CallFunc(name, args)
	}
	return nil, false
}
