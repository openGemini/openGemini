// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// the version of prometheus has been updated to v0.46.0

package tests

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

var (
	patSpace       = regexp.MustCompile("[\t ]+")
	patLoad        = regexp.MustCompile(`^load\s+(.+?)$`)
	patEvalInstant = regexp.MustCompile(`^eval(?:_(fail|ordered|skip))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$`)
)

var testStartTime = time.Unix(0, 0).UTC()

// PromTest is a sequence of read and write commands that are run
// against a test storage.
type PromTest struct {
	Test
}

func NewPromTestFromFile(t *testing.T, filename string, db string, rp string, s Server) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	test := &PromTest{}
	test.db = db
	test.rp = rp
	test.writes = make(Writes, 0)

	err = test.parse(string(content), t, s)
	test.clear()
	return err
}

func raise(line int, format string, v ...interface{}) error {
	return &parser.ParseErr{
		LineOffset: line,
		Err:        fmt.Errorf(format, v...),
	}
}

func parseLoad(lines []string, i int) (int, []string, error) {
	if !patLoad.MatchString(lines[i]) {
		return i, nil, raise(i, "invalid load command. (load <step:duration>)")
	}
	parts := patLoad.FindStringSubmatch(lines[i])

	gap, err := model.ParseDuration(parts[1])
	if err != nil {
		return i, nil, raise(i, "invalid step definition %q: %s", parts[1], err)
	}
	data := make([]string, 0)
	for i+1 < len(lines) {
		i++
		defLine := lines[i]
		if len(defLine) == 0 {
			i--
			break
		}
		metric, vals, err := parser.ParseSeriesDesc(defLine)
		ts := testStartTime
		samples := make([]promql.Point, 0, len(vals))
		for _, v := range vals {
			if !v.Omitted {
				samples = append(samples, promql.Point{
					T: ts.UnixNano() / int64(time.Millisecond/time.Nanosecond),
					V: v.Value,
				})
			}
			ts = ts.Add(time.Duration(gap))
		}
		AppendData(&data, metric, samples)
		if err != nil {
			if perr, ok := err.(*parser.ParseErr); ok {
				perr.LineOffset = i
			}
			return i, nil, err
		}
	}
	return i, data, nil
}

func AppendData(data *[]string, metric labels.Labels, samples []promql.Point) {
	if len(metric) == 0 {
		return
	}
	writes := metric[0].Value
	for _, label := range metric {
		writes += "," + label.Name + "=" + label.Value
	}

	for _, val := range samples {
		*data = append(*data, fmt.Sprintf(`%s value=%f %d`, writes, val.V, val.T))
	}
}

func (t *PromTest) parseEval(lines []string, i int) (int, []*Query, error) {
	if !patEvalInstant.MatchString(lines[i]) {
		return i, nil, raise(i, "invalid evaluation command. (eval[_fail|_ordered] instant [at <offset:duration>] <query>")
	}
	parts := patEvalInstant.FindStringSubmatch(lines[i])
	var (
		mod  = parts[1]
		at   = parts[2]
		expr = parts[3]
	)
	_, err := parser.ParseExpr(expr)
	if err != nil {
		if perr, ok := err.(*parser.ParseErr); ok {
			perr.LineOffset = i
			posOffset := parser.Pos(strings.Index(lines[i], expr))
			perr.PositionRange.Start += posOffset
			perr.PositionRange.End += posOffset
			perr.Query = lines[i]
		}
		return i, nil, err
	}

	offset, err := model.ParseDuration(at)
	if err != nil {
		return i, nil, raise(i, "invalid step definition %q: %s", parts[1], err)
	}
	ts := testStartTime.Add(time.Duration(offset))

	queries := make([]*Query, 0)
	for i+1 < len(lines) {
		i++
		defLine := lines[i]
		if len(defLine) == 0 {
			i--
			break
		}
		if f, err := parseNumber(defLine); err == nil {
			AppendQueries(&queries, i+1, expr, ts, f, mod)
			break
		}
		_, vals, err := parser.ParseSeriesDesc(defLine)
		if err != nil {
			if perr, ok := err.(*parser.ParseErr); ok {
				perr.LineOffset = i
			}
			return i, nil, err
		}

		// Currently, we are not expecting any matrices.
		if len(vals) > 1 {
			return i, nil, raise(i, "expecting multiple values in instant evaluation not allowed")
		}
		AppendQueries(&queries, i+1, expr, ts, vals[0].Value, mod)
	}
	return i, queries, nil
}

func AppendQueries(queries *[]*Query, line int, expr string, startTime time.Time, exp float64, mod string) error {
	var fail, ordered, skip bool
	switch mod {
	case "ordered":
		ordered = true
	case "fail":
		fail = true
	case "skip":
		skip = true
	}
	qs, err := atModifierTestCases(expr, startTime)
	if err != nil {
		return err
	}
	for _, q := range qs {
		*queries = append(*queries, &Query{
			name:    strconv.Itoa(line),
			command: q.expr,
			params:  url.Values{"db": []string{"db0"}, "time": []string{strconv.FormatInt(q.evalTime.Unix(), 10)}},
			exp:     strconv.FormatFloat(exp, 'f', 1, 64),
			path:    "/api/v1/query",
			pattern: true,
			ordered: ordered,
			fail:    fail,
			skip:    skip,
		})
		*queries = append(*queries, &Query{
			name:    strconv.Itoa(line),
			command: q.expr,
			params:  url.Values{"db": []string{"db0"}, "start": []string{strconv.FormatInt(q.evalTime.Add(-time.Minute).Unix(), 10)}, "end": []string{strconv.FormatInt(q.evalTime.Add(time.Minute).Unix(), 10)}, "step": []string{"60"}},
			exp:     strconv.FormatFloat(exp, 'f', 1, 64),
			path:    "/api/v1/query_range",
			pattern: true,
			ordered: ordered,
			fail:    fail,
			skip:    skip,
		})
	}
	return nil
}

// getLines returns trimmed lines after removing the comments.
func getLines(input string) []string {
	lines := strings.Split(input, "\n")
	for i, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "#") {
			l = ""
		}
		lines[i] = l
	}
	return lines
}

// parse the given command sequence and appends it to the test.
func (test *PromTest) parse(input string, t *testing.T, s Server) error {
	lines := getLines(input)
	var err error
	// Scan for steps line by line.
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		if len(l) == 0 {
			continue
		}

		switch c := strings.ToLower(patSpace.Split(l, 2)[0]); {
		case c == "clear":
			test.clear()
		case c == "load":
			var writes []string
			i, writes, err = parseLoad(lines, i)
			if err != nil {
				return err
			}
			test.writes = append(test.writes, &Write{data: strings.Join(writes, "\n")})
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		case strings.HasPrefix(c, "eval"):
			var queries []*Query
			i, queries, err = test.parseEval(lines, i)
			RunQueries(queries, t, s)
		default:
			return raise(i, "invalid command %q", l)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type atModifierTestCase struct {
	expr     string
	evalTime time.Time
}

func atModifierTestCases(exprStr string, evalTime time.Time) ([]atModifierTestCase, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}
	ts := timestamp.FromTime(evalTime)

	containsNonStepInvariant := false
	// Setting the @ timestamp for all selectors to be evalTime.
	// If there is a subquery, then the selectors inside it don't get the @ timestamp.
	// If any selector already has the @ timestamp set, then it is untouched.
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		_, _, subqTs := subqueryTimes(path)
		if subqTs != nil {
			// There is a subquery with timestamp in the path,
			// hence don't change any timestamps further.
			return nil
		}
		switch n := node.(type) {
		case *parser.VectorSelector:
			if n.Timestamp == nil {
				n.Timestamp = makeInt64Pointer(ts)
			}

		case *parser.MatrixSelector:
			if vs := n.VectorSelector.(*parser.VectorSelector); vs.Timestamp == nil {
				vs.Timestamp = makeInt64Pointer(ts)
			}

		case *parser.SubqueryExpr:
			if n.Timestamp == nil {
				n.Timestamp = makeInt64Pointer(ts)
			}

		case *parser.Call:
			_, ok := promql.AtModifierUnsafeFunctions[n.Func.Name]
			containsNonStepInvariant = containsNonStepInvariant || ok
		}
		return nil
	})

	if containsNonStepInvariant {
		// Since there is a step invariant function, we cannot automatically
		// generate step invariant test cases for it sanely.
		return nil, nil
	}

	newExpr := expr.String() // With all the @ evalTime set.
	additionalEvalTimes := []int64{-10 * ts, 0, ts / 5, ts, 10 * ts}
	if ts == 0 {
		additionalEvalTimes = []int64{-1000, -ts, 1000}
	}
	testCases := make([]atModifierTestCase, 0, len(additionalEvalTimes))
	for _, et := range additionalEvalTimes {
		testCases = append(testCases, atModifierTestCase{
			expr:     newExpr,
			evalTime: timestamp.Time(et),
		})
	}

	return testCases, nil
}

// clear the current test storage of all inserted samples.
func (t *PromTest) clear() {
	t.queries = nil
	t.writes = nil
}

func RunQueries(queries []*Query, t *testing.T, s Server) {
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.ExecuteProm(s); err != nil {
				if query.fail {
					t.Skipf("SKIP:: %s", query.name)
				}
				t.Error(query.Error(err))
			} else {
				if query.fail {
					t.Error(fmt.Errorf("expected error evaluating query %q (line %s) but got none", query.command, query.name))
				}
				if query.ordered && query.path == "/api/v1/query_range" {
					// Ordering isn't defined for range queries.
					t.Skipf("SKIP:: %s", query.name)
				}
				if !query.success() {
					t.Error(query.failureMessage())
				}
			}
		})
	}
}

func parseNumber(s string) (float64, error) {
	n, err := strconv.ParseInt(s, 0, 64)
	f := float64(n)
	if err != nil {
		f, err = strconv.ParseFloat(s, 64)
	}
	if err != nil {
		return 0, fmt.Errorf("%w, error parsing number", err)
	}
	return f, nil
}

func subqueryTimes(path []parser.Node) (time.Duration, time.Duration, *int64) {
	var (
		subqOffset, subqRange time.Duration
		ts                    int64 = math.MaxInt64
	)
	for _, node := range path {
		switch n := node.(type) {
		case *parser.SubqueryExpr:
			subqOffset += n.OriginalOffset
			subqRange += n.Range
			if n.Timestamp != nil {
				// The @ modifier on subquery invalidates all the offset and
				// range till now. Hence resetting it here.
				subqOffset = n.OriginalOffset
				subqRange = n.Range
				ts = *n.Timestamp
			}
		}
	}
	var tsp *int64
	if ts != math.MaxInt64 {
		tsp = &ts
	}
	return subqOffset, subqRange, tsp
}

func makeInt64Pointer(val int64) *int64 {
	valp := new(int64)
	*valp = val
	return valp
}
