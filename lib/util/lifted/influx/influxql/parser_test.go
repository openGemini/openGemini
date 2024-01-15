package influxql_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
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
