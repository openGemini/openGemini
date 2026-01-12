// Copyright 2025 openGemini Authors
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

package prompt

import (
	"strings"

	"github.com/openGemini/go-prompt"
)

type Completer struct {
	suggestions    []prompt.Suggest
	aggregateFuncs []prompt.Suggest
	timeFuncs      []prompt.Suggest
	operators      []prompt.Suggest
	filters        []prompt.Suggest

	suggest bool
}

func NewCompleter() *Completer {
	c := &Completer{}

	// Initialize aggregate functions
	c.aggregateFuncs = []prompt.Suggest{
		{Text: "ABS()", Description: "Returns the absolute value of the field value."},
		{Text: "ACOS()", Description: "Returns the arccosine (in radians) of the field value. Field values must be between -1 and 1"},
		{Text: "ASIN()", Description: "Returns the arcsine (in radians) of the field value. Field values must be between -1 and 1"},
		{Text: "COS()", Description: "Returns the cosine of the field value"},
		{Text: "SIN()", Description: "Returns the sine of the field value"},
		{Text: "TAN()", Description: "Returns the tangent of the field value"},
		{Text: "ATAN()", Description: "Returns the arctangent (in radians) of the field value."},
		{Text: "ATAN2()", Description: "Returns the the arctangent of y/x in radians"},
		{Text: "CEIL()", Description: "Returns the subsequent value rounded up to the nearest integer"},
		{Text: "EXP()", Description: "Returns the exponential of the field value"},
		{Text: "FLOOR()", Description: "Returns the subsequent value rounded down to the nearest integer"},
		{Text: "LN()", Description: "Returns the natural logarithm of the field value"},
		{Text: "LOG()", Description: "Returns the logarithm of the field value with base b"},
		{Text: "LOG2()", Description: "Returns the logarithm of the field value to the base 2"},
		{Text: "LOG10()", Description: "Returns the logarithm of the field value to the base 10"},
		{Text: "POW()", Description: "Returns the field value to the power of x"},
		{Text: "ROUND()", Description: "Returns the subsequent value rounded to the nearest integer"},
		{Text: "SQRT()", Description: "Returns the square root of field value"},
		{Text: "FIRST()", Description: "Returns the field value with the oldest timestamp"},
		{Text: "LAST()", Description: "Returns the field value with the most recent timestamp"},
		{Text: "MIN()", Description: "Returns the lowest field value"},
		{Text: "MAX()", Description: "Returns the greatest field value"},
		{Text: "TOP()", Description: "Returns the greatest N field values"},
		{Text: "BOTTOM()", Description: "Returns the smallest N field values"},
		{Text: "PERCENTILE()", Description: "Returns the Nth percentile field value"},
		{Text: "SAMPLE()", Description: "Returns a random sample of N field values"},
		{Text: "PERCENTILE_OGSKETCH()", Description: "Returns the Nth percentile(approximate) field value"},
		{Text: "STR()", Description: "Returns whether the specified character is contained in the string"},
		{Text: "STRLEN()", Description: "Returns the length of the string"},
		{Text: "SUBSTR()", Description: "Returns a substring of a string"},
		{Text: "CASTOR()", Description: "Anomaly Detection and Prediction"},
		{Text: "COUNT()", Description: "Returns the number of non-null field values"},
		{Text: "COUNT(time)", Description: "the total amount of data by time column"},
		{Text: "MEAN()", Description: "Returns the arithmetic average of field values"},
		{Text: "SUM()", Description: "Returns the sum of field values."},
		{Text: "MODE()", Description: "Returns the most frequent value in a list of field values"},
		{Text: "STDDEV()", Description: "Returns the standard deviation of field values"},
		{Text: "MEDIAN()", Description: "Returns the middle value from a sorted list of field values"},
		{Text: "SPREAD()", Description: "Returns the difference between the minimum and maximum field values"},
		{Text: "DISTINCT()", Description: "Returns the list of unique field values"},
		{Text: "IRATE()", Description: "( |An - An-1| )/( |Tn-1 - Tn| )"},
		{Text: "RATE()", Description: "( |A1-A2| )/( |T1-T2|)"},
		{Text: "MOVING_AVERAGE()", Description: "Returns the rolling average across a window of subsequent field values"},
		{Text: "HOLT_WINTERS()", Description: "Returns N number of predicted field values"},
		{Text: "CUMULATIVE_SUM()", Description: "Returns the running total of subsequent field values"},
		{Text: "DERIVATIVE()", Description: "Returns the rate of change between subsequent field values"},
		{Text: "DIFFERENCE()", Description: "Returns the result of subtraction between subsequent field values"},
		{Text: "ELAPSED()", Description: "Returns the difference between subsequent field value’s timestamps"},
		{Text: "NON_NEGATIVE_DERIVATIVE()", Description: "Returns the non-negative rate of change between subsequent field values"},
		{Text: "NON_NEGATIVE_DIFFERENCE()", Description: "Returns the non-negative result of subtraction between subsequent field values"},
	}

	// Initialize time functions
	c.timeFuncs = []prompt.Suggest{
		{Text: "time()", Description: "Time function"},
		{Text: "now()", Description: "Current time"},
		{Text: "GROUP BY time()", Description: "Group by time intervals"},
		{Text: "NOW()", Description: "Return current timestamp"},
		{Text: "TIME", Description: "Return timestamp for each point"},
		{Text: "ELAPSED", Description: "Return time difference between points"},
		{Text: "HOUR", Description: "Extract hour from timestamp"},
		{Text: "MINUTE", Description: "Extract minute from timestamp"},
		{Text: "SECOND", Description: "Extract second from timestamp"},
	}

	// Initialize operators
	c.operators = []prompt.Suggest{
		{Text: "AND", Description: "Logical AND"},
		{Text: "OR", Description: "Logical OR"},
		{Text: "=", Description: "Equal to"},
		{Text: "!=", Description: "Not equal to"},
		{Text: ">", Description: "Greater than"},
		{Text: ">=", Description: "Greater than or equal to"},
		{Text: "<", Description: "Less than"},
		{Text: "<=", Description: "Less than or equal to"},
	}

	c.filters = []prompt.Suggest{
		{Text: "FROM", Description: "Specify data source"},
		{Text: "WHERE", Description: "Filter conditions"},
		{Text: "GROUP BY", Description: "Group results"},
		{Text: "ORDER BY", Description: "Sort results"},
		{Text: "LIMIT", Description: "Limit number of rows"},
		{Text: "OFFSET", Description: "Skip number of rows"},
		{Text: "SLIMIT", Description: "Limit number of series"},
		{Text: "SOFFSET", Description: "Skip number of series"},
		{Text: "WITH", Description: "Additional options"},
		{Text: "TIME", Description: "Time column"},
		{Text: "FILL", Description: "Fill null values"},
		{Text: "INTO", Description: "Write destination"},
	}

	// Initialize base suggestions
	c.suggestions = []prompt.Suggest{
		// Basic query commands
		{Text: "SHOW", Description: "Display database information"},
		{Text: "CREATE", Description: "Create database or retention policy"},
		{Text: "DROP", Description: "Delete database or measurement"},
		{Text: "SELECT", Description: "Query data"},
		{Text: "INSERT", Description: "Insert data"},
		{Text: "DELETE", Description: "Delete data"},
		{Text: "USE", Description: "Switch database and retention policy"},

		// Exit commands
		{Text: "exit", Description: "Exit CLI"},
		{Text: "quit", Description: "Exit CLI"},
	}

	return c
}

func (c *Completer) completer(d prompt.Document) []prompt.Suggest {
	var suggestions []prompt.Suggest
	if !c.suggest {
		return suggestions
	}

	line := d.TextBeforeCursor()
	words := strings.Fields(line)
	word := d.GetWordBeforeCursor()

	// 如果是空行，返回所有一级命令
	if len(words) == 0 {
		return c.suggestions
	}

	// 根据第一个命令词来判断
	switch strings.ToUpper(words[0]) {
	case "SHOW":
		// 只返回 SHOW 相关的二级命令
		suggestions = []prompt.Suggest{
			{Text: "DATABASES", Description: "List all databases"},
			{Text: "MEASUREMENTS", Description: "List all measurements"},
			{Text: "SERIES", Description: "List all series"},
			{Text: "SERIES CARDINALITY", Description: "List time series cardinality"},
			{Text: "SHARDS", Description: "List shard information"},
			{Text: "SHARD GROUPS", Description: "List shard group information"},
			{Text: "TAG KEYS", Description: "List all tag keys"},
			{Text: "TAG VALUES", Description: "List values for a tag key"},
			{Text: "FIELD KEYS", Description: "List all field keys"},
			{Text: "RETENTION POLICIES", Description: "List retention policies"},
			{Text: "CLUSTER", Description: "List the status of all nodes in the cluster"},
			{Text: "USERS", Description: "Show user list information"},
		}
		// 如果当前输入只有 SHOW，则返回所有二级命令
		if len(words) == 1 && !strings.HasSuffix(line, " ") {
			return prompt.FilterHasPrefix(suggestions, word, true)
		}
		// 如果正在输入第二个词，则根据前缀过滤二级命令
		if len(words) <= 2 {
			return prompt.FilterHasPrefix(suggestions, word, true)
		}
	case "CREATE":
		suggestions = []prompt.Suggest{
			{Text: "DATABASE", Description: "Create new database"},
			{Text: "RETENTION POLICY", Description: "Create retention policy"},
			{Text: "SUBSCRIPTION", Description: "Create subscription"},
		}
	case "DROP":
		suggestions = []prompt.Suggest{
			{Text: "DATABASE", Description: "Drop database"},
			{Text: "MEASUREMENT", Description: "Drop measurement"},
			{Text: "SERIES", Description: "Drop series"},
			{Text: "RETENTION POLICY", Description: "Drop retention policy"},
		}
	case "SELECT":
		if len(words) <= 2 {
			suggestions = append(suggestions, c.aggregateFuncs...)
			suggestions = append(suggestions, []prompt.Suggest{
				{Text: "*", Description: "Select all fields"},
				{Text: "FROM", Description: "Specify data source"},
			}...)
		} else if strings.HasSuffix(line, "FROM ") {
			suggestions = []prompt.Suggest{
				{Text: "<measurement_name>", Description: "Enter measurement name"},
			}
		} else if strings.Contains(line, "WHERE") {
			suggestions = append(suggestions, []prompt.Suggest{
				{Text: "<tag_name>", Description: "Enter tag name"},
				{Text: "<tag_value>", Description: "Enter tag value"},
			}...)
			suggestions = append(suggestions, c.operators...)
			suggestions = append(suggestions, c.timeFuncs...)
		} else if len(words) >= 4 {
			suggestions = append(suggestions, c.filters...)
			suggestions = append(suggestions, c.operators...)
			suggestions = append(suggestions, c.timeFuncs...)
		}
	}

	// 如果没有匹配到特定命令，则返回默认建议
	if len(suggestions) == 0 {
		suggestions = c.suggestions
	}

	return prompt.FilterHasPrefix(suggestions, word, true)
}

func (c *Completer) switchCompleter(s bool) {
	c.suggest = s
}
