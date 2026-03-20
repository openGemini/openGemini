package influxql_test

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExpr(t *testing.T) {
	cond := "a = 1"
	expr, err := influxql.ParseExpr(cond)
	assert.NoError(t, err)

	assert.Equal(t, cond, expr.String())
}

func TestParallelParseExpr(t *testing.T) {
	parallel := 100

	var conds []string
	for i := 0; i < 1000; i++ {
		conds = append(conds, fmt.Sprintf("c_%d = %d", i, i))
	}

	var doTest = func() {
		for _, cond := range conds {
			expr, err := influxql.ParseExpr(cond)
			assert.NoError(t, err)

			assert.Equal(t, cond, expr.String())
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			doTest()
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkParseExpr(b *testing.B) {
	cond := "a = 1 and b = 2 and c= 3"
	for i := 0; i < b.N; i++ {
		_, _ = influxql.ParseExpr(cond)
	}
}

func TestInfAndNan(t *testing.T) {
	s := "value::float * -Inf AS value"
	rhs := setupParse(s)
	require.Equal(t, rhs.Val, math.Inf(-1))

	s1 := "value::float * +Inf AS value"
	rhs1 := setupParse(s1)
	require.Equal(t, rhs1.Val, math.Inf(1))

	s2 := "value::float * NaN AS value"
	rhs2 := setupParse(s2)
	require.True(t, math.IsNaN(rhs2.Val))
}

func setupParse(s string) *influxql.NumberLiteral {
	p := influxql.NewParser(strings.NewReader("SELECT " + s + " FROM mock"))
	expr, _ := p.ParseStatement()
	statement := expr.(*influxql.SelectStatement)
	binaryExpr := statement.Fields[0].Expr.(*influxql.BinaryExpr)
	rhs := binaryExpr.RHS.(*influxql.NumberLiteral)
	return rhs
}

func isDateString(s string) bool     { return dateStringRegexp.MatchString(s) }
func isDateTimeString(s string) bool { return dateTimeStringRegexp.MatchString(s) }

var dateStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var dateTimeStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}.+`)

func TestIsDateString(t *testing.T) {
	sl := &influxql.StringLiteral{}

	tests := []string{"", "2024-10-10", "2024-10-10 20:00:00", "a202-10-10", "2024a10-10", "2024-1-100", "2024-100-0"}
	for _, s := range tests {
		sl.Val = s
		require.Equal(t, isDateString(s) || isDateTimeString(s), sl.IsTimeLiteral(),
			fmt.Sprintf("IsTimeLiteral(%s)", sl.Val))
	}
}

func BenchmarkDataTime(b *testing.B) {
	dataStr := "2024-01-02"
	dataTimeStr := "2024-01-10 20:00:00"
	sl := &influxql.StringLiteral{}

	b.Run("isDateString-v1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			isDateString(dataStr)
		}
	})
	b.Run("isDateString-v2", func(b *testing.B) {
		sl.Val = dataStr
		for i := 0; i < b.N; i++ {
			sl.IsTimeLiteral()
		}
	})
	b.Run("isDateTimeString-v1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			isDateTimeString(dataTimeStr)
		}
	})
	b.Run("isDateTimeString-v2", func(b *testing.B) {
		sl.Val = dataTimeStr
		for i := 0; i < b.N; i++ {
			sl.IsTimeLiteral()
		}
	})
}
