package tests

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServer_PromQuery_Basic(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258327.955"}},
			command: `up + (1 != bool 2)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258327.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258327.955"}},
			command: `up + (1 == bool 2)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258327.955,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258327.955"}},
			command: `up`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[1709258327.955,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: label filter",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{job="container"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `sum(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"15"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: count",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `count(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: min",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `min(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: max",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `max(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"6"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: avg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `avg(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: sum group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `sum(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:8080"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090"},"value":[1709258357.955,"7"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: count group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `count(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:8080"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:9090"},"value":[1709258357.955,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: min group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `min(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:8080"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: max group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `max(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:8080"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090"},"value":[1709258357.955,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: avg group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `avg(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:8080"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090"},"value":[1709258357.955,"3.5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: topk(1, up*100) group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `topk(1, up*100)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258357.955,"600"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: top-sum group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `topk(3, sum(up{job="container"}) by (job))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"container"},"value":[1709258357.955,"15"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: top-sum group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357"}},
			command: `topk(3, sum by (job, instance) (increase(up[2m]))/2)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357,"1.9681666666666666"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: avg without() group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `avg without() (up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: avg without(instance) group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `avg without(instance) (up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"container"},"value":[1709258357.955,"5"]},{"metric":{"job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: avg without(instance, job) group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `avg without(instance, job) (up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"4.5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: avg without(nonexistent) group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `avg without(nonexistent) (up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with label filter",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{job="container"}[3m]`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"values":[[1709258357.955,"6"]]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"]]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with rate",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `rate(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"0.022222222222222223"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with sum(rate) group by ",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `sum(rate(up[3m])) by (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"prometheus"},"value":[1709258357.955,"0.022222222222222223"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with irate",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `irate(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"0.06666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with deriv",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `deriv(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"0.06666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with increase",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `increase(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with predict_linear",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `predict_linear(up[3m],2)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"4.133333333333334"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with avg_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `avg_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"2.5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with sum_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `sum_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with count_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `count_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"1"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"1"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"1"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with min_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `min_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: range vector selector with max_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `max_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `up`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"values":[[1709258390,"6"],[1709258450,"6"],[1709258510,"6"],[1709258570,"6"],[1709258630,"6"]]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258390,"5"],[1709258450,"5"],[1709258510,"5"],[1709258570,"5"],[1709258630,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258390,"4"],[1709258450,"4"],[1709258510,"4"],[1709258570,"4"],[1709258630,"4"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"values":[[1709258330,"2"],[1709258390,"3"],[1709258450,"3"],[1709258510,"3"],[1709258570,"3"],[1709258630,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: label filter",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `up{job="container"}`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"values":[[1709258390,"6"],[1709258450,"6"],[1709258510,"6"],[1709258570,"6"],[1709258630,"6"]]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258390,"5"],[1709258450,"5"],[1709258510,"5"],[1709258570,"5"],[1709258630,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258390,"4"],[1709258450,"4"],[1709258510,"4"],[1709258570,"4"],[1709258630,"4"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: sum",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `sum(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258390,"15"],[1709258450,"15"],[1709258510,"15"],[1709258570,"15"],[1709258630,"15"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: count",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `count(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258390,"3"],[1709258450,"3"],[1709258510,"3"],[1709258570,"3"],[1709258630,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: min",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `min(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258390,"4"],[1709258450,"4"],[1709258510,"4"],[1709258570,"4"],[1709258630,"4"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: max",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `max(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258390,"6"],[1709258450,"6"],[1709258510,"6"],[1709258570,"6"],[1709258630,"6"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: avg",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `avg(up{job="container"})`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258390,"5"],[1709258450,"5"],[1709258510,"5"],[1709258570,"5"],[1709258630,"5"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: sum group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `sum(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070"},"values":[[1709258390,"6"],[1709258450,"6"],[1709258510,"6"],[1709258570,"6"],[1709258630,"6"]]},{"metric":{"instance":"localhost:8080"},"values":[[1709258390,"5"],[1709258450,"5"],[1709258510,"5"],[1709258570,"5"],[1709258630,"5"]]},{"metric":{"instance":"localhost:9090"},"values":[[1709258330,"2"],[1709258390,"7"],[1709258450,"7"],[1709258510,"7"],[1709258570,"7"],[1709258630,"7"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: count group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `count(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070"},"values":[[1709258390,"1"],[1709258450,"1"],[1709258510,"1"],[1709258570,"1"],[1709258630,"1"]]},{"metric":{"instance":"localhost:8080"},"values":[[1709258390,"1"],[1709258450,"1"],[1709258510,"1"],[1709258570,"1"],[1709258630,"1"]]},{"metric":{"instance":"localhost:9090"},"values":[[1709258330,"1"],[1709258390,"2"],[1709258450,"2"],[1709258510,"2"],[1709258570,"2"],[1709258630,"2"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: min group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `min(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070"},"values":[[1709258390,"6"],[1709258450,"6"],[1709258510,"6"],[1709258570,"6"],[1709258630,"6"]]},{"metric":{"instance":"localhost:8080"},"values":[[1709258390,"5"],[1709258450,"5"],[1709258510,"5"],[1709258570,"5"],[1709258630,"5"]]},{"metric":{"instance":"localhost:9090"},"values":[[1709258330,"2"],[1709258390,"3"],[1709258450,"3"],[1709258510,"3"],[1709258570,"3"],[1709258630,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: max group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `max(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070"},"values":[[1709258390,"6"],[1709258450,"6"],[1709258510,"6"],[1709258570,"6"],[1709258630,"6"]]},{"metric":{"instance":"localhost:8080"},"values":[[1709258390,"5"],[1709258450,"5"],[1709258510,"5"],[1709258570,"5"],[1709258630,"5"]]},{"metric":{"instance":"localhost:9090"},"values":[[1709258330,"2"],[1709258390,"4"],[1709258450,"4"],[1709258510,"4"],[1709258570,"4"],[1709258630,"4"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: avg group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `avg(up) by (instance)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070"},"values":[[1709258390,"6"],[1709258450,"6"],[1709258510,"6"],[1709258570,"6"],[1709258630,"6"]]},{"metric":{"instance":"localhost:8080"},"values":[[1709258390,"5"],[1709258450,"5"],[1709258510,"5"],[1709258570,"5"],[1709258630,"5"]]},{"metric":{"instance":"localhost:9090"},"values":[[1709258330,"2"],[1709258390,"3.5"],[1709258450,"3.5"],[1709258510,"3.5"],[1709258570,"3.5"],[1709258630,"3.5"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: topk(1, up*100)",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `topk(1, up*100)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258390,"600"],[1709258450,"600"],[1709258510,"600"],[1709258570,"600"],[1709258630,"600"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258330,"200"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: top-sum group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `topk(3, sum(up{job="container"}) by (job))`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"container"},"values":[[1709258390,"15"],[1709258450,"15"],[1709258510,"15"],[1709258570,"15"],[1709258630,"15"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: top-sum group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `topk(3, sum by (job, instance) (increase(up[2m]))/2)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258330,"1.0681666666666667"],[1709258390,"1.75"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with rate",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `rate(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"0.016666666666666666"],[1709258372.955,"0.019444444444444445"],[1709258402.955,"0.019444444444444445"],[1709258432.955,"0.019444444444444445"],[1709258462.955,"0.019444444444444445"],[1709258492.955,"0.013888888888888888"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with sum(rate) group by ",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `sum(rate(up[3m])) by (job)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"prometheus"},"values":[[1709258342.955,"0.016666666666666666"],[1709258372.955,"0.019444444444444445"],[1709258402.955,"0.019444444444444445"],[1709258432.955,"0.019444444444444445"],[1709258462.955,"0.019444444444444445"],[1709258492.955,"0.013888888888888888"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with avg_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `avg_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"2"],[1709258372.955,"2"],[1709258402.955,"2"],[1709258432.955,"2"],[1709258462.955,"2"],[1709258492.955,"2"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with sum_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `sum_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"6"],[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with count_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `count_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with min_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `min_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with max_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `max_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with irate",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `irate(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"0.06666666666666667"],[1709258372.955,"0.06666666666666667"],[1709258402.955,"0.06666666666666667"],[1709258432.955,"0.06666666666666667"],[1709258462.955,"0.06666666666666667"],[1709258492.955,"0.06666666666666667"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with deriv",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `deriv(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"0.06666666666666667"],[1709258372.955,"0.06666666666666667"],[1709258402.955,"0.06666666666666667"],[1709258432.955,"0.06666666666666667"],[1709258462.955,"0.06666666666666667"],[1709258492.955,"0.06666666666666667"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with increase",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `increase(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"3"],[1709258372.955,"3.5"],[1709258402.955,"3.5"],[1709258432.955,"3.5"],[1709258462.955,"3.5"],[1709258492.955,"2.5"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: range vector selector with predict_linear",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `predict_linear(up[3m],2)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"3.1333333333333333"],[1709258372.955,"5.133333333333334"],[1709258402.955,"7.133333333333334"],[1709258432.955,"9.133333333333333"],[1709258462.955,"11.133333333333333"],[1709258492.955,"13.133333333333333"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  label_replace",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `label_replace(up,"host","$2","instance","(.*):(.*)")`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","host":"7070","instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"__name__":"up","host":"8080","instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"__name__":"up","host":"9090","instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"__name__":"up","host":"9090","instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: range vector selector with label_replace",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `label_replace(up,"host","$2","instance","(.*):(.*)")`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","host":"7070","instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"__name__":"up","host":"8080","instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"__name__":"up","host":"9090","instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"__name__":"up","host":"9090","instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  label_join",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `label_join(up,"joinSrc","-","instance","job")`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container","joinSrc":"localhost:7070-container"},"value":[1709258507.955,"6"]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container","joinSrc":"localhost:8080-container"},"value":[1709258507.955,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container","joinSrc":"localhost:9090-container"},"value":[1709258507.955,"4"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus","joinSrc":"localhost:9090-prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: range vector selector with label_join",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `label_join(up,"joinSrc","-","instance","job")`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container","joinSrc":"localhost:7070-container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container","joinSrc":"localhost:8080-container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container","joinSrc":"localhost:9090-container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus","joinSrc":"localhost:9090-prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  count_values",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `count_values("value", up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"value":"3"},"value":[1709258507.955,"1"]},{"metric":{"value":"4"},"value":[1709258507.955,"1"]},{"metric":{"value":"5"},"value":[1709258507.955,"1"]},{"metric":{"value":"6"},"value":[1709258507.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: count_values",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `count_values("value", up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"value":"1"},"values":[[1709258312.955,"1"]]},{"metric":{"value":"3"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"value":"4"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"value":"5"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"value":"6"},"values":[[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: count_values by job",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `count_values("value", up) by (job)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"container","value":"4"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"job":"container","value":"5"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"job":"container","value":"6"},"values":[[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"job":"prometheus","value":"1"},"values":[[1709258312.955,"1"]]},{"metric":{"job":"prometheus","value":"3"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: count_values by job",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `count_values("job", up) by (job)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"1"},"values":[[1709258312.955,"1"]]},{"metric":{"job":"3"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"job":"4"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"job":"5"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"job":"6"},"values":[[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  delta",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `delta(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"1.5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: delta",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `delta(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"2.5"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"2.5"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  idelta",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `idelta(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: idelta",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `idelta(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  stdvar",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `stdvar(up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258507.955,"1.25"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: stdvar",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `stdvar(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258312.955,"0"],[1709258342.955,"0.6666666666666666"],[1709258372.955,"1.25"],[1709258402.955,"1.25"],[1709258432.955,"1.25"],[1709258462.955,"1.25"],[1709258492.955,"1.25"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  stddev",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `stddev(up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258507.955,"1.118033988749895"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: stddev",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `stddev(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258312.955,"0"],[1709258342.955,"0.816496580927726"],[1709258372.955,"1.118033988749895"],[1709258402.955,"1.118033988749895"],[1709258432.955,"1.118033988749895"],[1709258462.955,"1.118033988749895"],[1709258492.955,"1.118033988749895"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  clamp",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `clamp(up,2,5)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: clamp",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `clamp(up,2,5)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"2"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  clamp_min",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `clamp_min(up,2)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: clamp_min",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `clamp_min(up,2)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"2"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  clamp_max",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `clamp_max(up,5)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  clamp_max",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `clamp_max(floor(sum(sum_over_time(up[20m])))+1,5)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258507.955,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: clamp_max",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `clamp_max(up,5)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  group",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `group(up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258507.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: group",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `group(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258312.955,"1"],[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  group without",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `group without(instance) (up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"container"},"value":[1709258507.955,"1"]},{"metric":{"job":"prometheus"},"value":[1709258507.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: group",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `group without(instance) (up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"container"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  scalar",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `scalar(up)`,
			exp:     `{"status":"success","data":{"resultType":"scalar","result":[1709258507.955,"NaN"]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: scalar",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `scalar(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258372.955,"NaN"],[1709258402.955,"NaN"],[1709258432.955,"NaN"],[1709258462.955,"NaN"],[1709258492.955,"NaN"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  round 1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `round(up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: round 1",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `round(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  round 2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `round(up,0.3)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258507.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258507.955,"5.1"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258507.955,"3.9"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258507.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: round 2",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `round(up,0.3)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5.1"],[1709258372.955,"5.1"],[1709258402.955,"5.1"],[1709258432.955,"5.1"],[1709258462.955,"5.1"],[1709258492.955,"5.1"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"3.9"],[1709258372.955,"3.9"],[1709258402.955,"3.9"],[1709258432.955,"3.9"],[1709258462.955,"3.9"],[1709258492.955,"3.9"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"0.8999999999999999"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  quantile_prom",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `quantile(0.3,up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258507.955,"3.9"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: quantile_prom",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `quantile(0.3,up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258312.955,"1"],[1709258342.955,"3.6"],[1709258372.955,"3.9"],[1709258402.955,"3.9"],[1709258432.955,"3.9"],[1709258462.955,"3.9"],[1709258492.955,"3.9"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  quantile_prom",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `quantile(2,up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258507.955,"+Inf"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: quantile_prom",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `quantile(2,up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258312.955,"+Inf"],[1709258342.955,"+Inf"],[1709258372.955,"+Inf"],[1709258402.955,"+Inf"],[1709258432.955,"+Inf"],[1709258462.955,"+Inf"],[1709258492.955,"+Inf"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "parenExpr parse",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258342.955"}},
			command: `sum((sum(increase(up[2m])) by (handler)/2))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258342.955,"1.5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: stdvar_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `stdvar_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"0.6666666666666666"],[1709258372.955,"0.6666666666666666"],[1709258402.955,"0.6666666666666666"],[1709258432.955,"0.6666666666666666"],[1709258462.955,"0.6666666666666666"],[1709258492.955,"0.6666666666666666"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: stddev_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `stddev_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"0.816496580927726"],[1709258372.955,"0.816496580927726"],[1709258402.955,"0.816496580927726"],[1709258432.955,"0.816496580927726"],[1709258462.955,"0.816496580927726"],[1709258492.955,"0.816496580927726"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: present_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `present_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: (1 - (up/down)) * 100",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `(1 - (up/down)) * 100`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"-500"],[1709258402.955,"-500"],[1709258432.955,"-500"],[1709258462.955,"-500"],[1709258492.955,"-500"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"-150"],[1709258372.955,"-150"],[1709258402.955,"-150"],[1709258432.955,"-150"],[1709258462.955,"-150"],[1709258492.955,"-150"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"-33.33333333333333"],[1709258372.955,"-33.33333333333333"],[1709258402.955,"-33.33333333333333"],[1709258432.955,"-33.33333333333333"],[1709258462.955,"-33.33333333333333"],[1709258492.955,"-33.33333333333333"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"83.33333333333334"],[1709258342.955,"25"],[1709258372.955,"25"],[1709258402.955,"25"],[1709258432.955,"25"],[1709258462.955,"25"],[1709258492.955,"25"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: holt_winters",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `holt_winters(up[3m],0.5,0.5)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: changes",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `changes(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"2"],[1709258372.955,"2"],[1709258402.955,"2"],[1709258432.955,"2"],[1709258462.955,"2"],[1709258492.955,"2"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: quantile_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `quantile_over_time(0.5,up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"2"],[1709258372.955,"2"],[1709258402.955,"2"],[1709258432.955,"2"],[1709258462.955,"2"],[1709258492.955,"2"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: resets",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `resets(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: resets",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `resets(down[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"2"],[1709258372.955,"2"],[1709258402.955,"2"],[1709258432.955,"2"],[1709258462.955,"2"],[1709258492.955,"2"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: regex measurement",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `count({__name__=~"down|up"} ) by (job)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"job":"prometheus"},"values":[[1709258312.955,"2"],[1709258342.955,"2"],[1709258372.955,"2"],[1709258402.955,"2"],[1709258432.955,"2"],[1709258462.955,"2"],[1709258492.955,"2"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: last_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `last_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: stddev by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `stddev by(job) (up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"container"},"values":[[1709258342.955,"0.5"],[1709258372.955,"0.816496580927726"],[1709258402.955,"0.816496580927726"],[1709258432.955,"0.816496580927726"],[1709258462.955,"0.816496580927726"],[1709258492.955,"0.816496580927726"]]},{"metric":{"job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: stdvar by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `stdvar by(job) (up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"container"},"values":[[1709258342.955,"0.25"],[1709258372.955,"0.6666666666666666"],[1709258402.955,"0.6666666666666666"],[1709258432.955,"0.6666666666666666"],[1709258462.955,"0.6666666666666666"],[1709258492.955,"0.6666666666666666"]]},{"metric":{"job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: topk by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `topk by(job) (2, up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: bottomk by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `bottomk by(job) (2, up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: vector + bool modifier",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `up + (1 == bool 2)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: modifier + vector",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `(1 * 2 + 4 / 6 - (10%7)^2) + up`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"-0.3333333333333339"],[1709258402.955,"-0.3333333333333339"],[1709258432.955,"-0.3333333333333339"],[1709258462.955,"-0.3333333333333339"],[1709258492.955,"-0.3333333333333339"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"-1.333333333333334"],[1709258372.955,"-1.333333333333334"],[1709258402.955,"-1.333333333333334"],[1709258432.955,"-1.333333333333334"],[1709258462.955,"-1.333333333333334"],[1709258492.955,"-1.333333333333334"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"-2.333333333333334"],[1709258372.955,"-2.333333333333334"],[1709258402.955,"-2.333333333333334"],[1709258432.955,"-2.333333333333334"],[1709258462.955,"-2.333333333333334"],[1709258492.955,"-2.333333333333334"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"-5.333333333333334"],[1709258342.955,"-3.333333333333334"],[1709258372.955,"-3.333333333333334"],[1709258402.955,"-3.333333333333334"],[1709258432.955,"-3.333333333333334"],[1709258462.955,"-3.333333333333334"],[1709258492.955,"-3.333333333333334"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: modifier - vector",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `(1 * 2 + 4 / 6 - (10%7)^2) - up`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"-12.333333333333334"],[1709258402.955,"-12.333333333333334"],[1709258432.955,"-12.333333333333334"],[1709258462.955,"-12.333333333333334"],[1709258492.955,"-12.333333333333334"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"-11.333333333333334"],[1709258372.955,"-11.333333333333334"],[1709258402.955,"-11.333333333333334"],[1709258432.955,"-11.333333333333334"],[1709258462.955,"-11.333333333333334"],[1709258492.955,"-11.333333333333334"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"-10.333333333333334"],[1709258372.955,"-10.333333333333334"],[1709258402.955,"-10.333333333333334"],[1709258432.955,"-10.333333333333334"],[1709258462.955,"-10.333333333333334"],[1709258492.955,"-10.333333333333334"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"-7.333333333333334"],[1709258342.955,"-9.333333333333334"],[1709258372.955,"-9.333333333333334"],[1709258402.955,"-9.333333333333334"],[1709258432.955,"-9.333333333333334"],[1709258462.955,"-9.333333333333334"],[1709258492.955,"-9.333333333333334"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: modifier * vector",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `(1 * 2 + 4 / 6 - (10%7)^2) * up`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"-38"],[1709258402.955,"-38"],[1709258432.955,"-38"],[1709258462.955,"-38"],[1709258492.955,"-38"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"-31.66666666666667"],[1709258372.955,"-31.66666666666667"],[1709258402.955,"-31.66666666666667"],[1709258432.955,"-31.66666666666667"],[1709258462.955,"-31.66666666666667"],[1709258492.955,"-31.66666666666667"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"-25.333333333333336"],[1709258372.955,"-25.333333333333336"],[1709258402.955,"-25.333333333333336"],[1709258432.955,"-25.333333333333336"],[1709258462.955,"-25.333333333333336"],[1709258492.955,"-25.333333333333336"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"-6.333333333333334"],[1709258342.955,"-19"],[1709258372.955,"-19"],[1709258402.955,"-19"],[1709258432.955,"-19"],[1709258462.955,"-19"],[1709258492.955,"-19"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: modifier / vector",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `(1 * 2 + 4 / 6 - (10%7)^2) / up`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"-1.0555555555555556"],[1709258402.955,"-1.0555555555555556"],[1709258432.955,"-1.0555555555555556"],[1709258462.955,"-1.0555555555555556"],[1709258492.955,"-1.0555555555555556"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"-1.2666666666666668"],[1709258372.955,"-1.2666666666666668"],[1709258402.955,"-1.2666666666666668"],[1709258432.955,"-1.2666666666666668"],[1709258462.955,"-1.2666666666666668"],[1709258492.955,"-1.2666666666666668"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"-1.5833333333333335"],[1709258372.955,"-1.5833333333333335"],[1709258402.955,"-1.5833333333333335"],[1709258432.955,"-1.5833333333333335"],[1709258462.955,"-1.5833333333333335"],[1709258492.955,"-1.5833333333333335"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"-6.333333333333334"],[1709258342.955,"-2.111111111111111"],[1709258372.955,"-2.111111111111111"],[1709258402.955,"-2.111111111111111"],[1709258432.955,"-2.111111111111111"],[1709258462.955,"-2.111111111111111"],[1709258492.955,"-2.111111111111111"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: modifier % vector",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `(1 * 2 + 4 / 6 - (10%7)^2) % up`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"-0.3333333333333339"],[1709258402.955,"-0.3333333333333339"],[1709258432.955,"-0.3333333333333339"],[1709258462.955,"-0.3333333333333339"],[1709258492.955,"-0.3333333333333339"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"-1.333333333333334"],[1709258372.955,"-1.333333333333334"],[1709258402.955,"-1.333333333333334"],[1709258432.955,"-1.333333333333334"],[1709258462.955,"-1.333333333333334"],[1709258492.955,"-1.333333333333334"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"-2.333333333333334"],[1709258372.955,"-2.333333333333334"],[1709258402.955,"-2.333333333333334"],[1709258432.955,"-2.333333333333334"],[1709258462.955,"-2.333333333333334"],[1709258492.955,"-2.333333333333334"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"-0.3333333333333339"],[1709258342.955,"-0.3333333333333339"],[1709258372.955,"-0.3333333333333339"],[1709258402.955,"-0.3333333333333339"],[1709258432.955,"-0.3333333333333339"],[1709258462.955,"-0.3333333333333339"],[1709258492.955,"-0.3333333333333339"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: modifier != vector",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `(1 * 2 + 4 / 6 - (10%7)^2) != up`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"5"],[1709258372.955,"5"],[1709258402.955,"5"],[1709258432.955,"5"],[1709258462.955,"5"],[1709258492.955,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"4"],[1709258372.955,"4"],[1709258402.955,"4"],[1709258432.955,"4"],[1709258462.955,"4"],[1709258492.955,"4"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"1"],[1709258342.955,"3"],[1709258372.955,"3"],[1709258402.955,"3"],[1709258432.955,"3"],[1709258462.955,"3"],[1709258492.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  absent",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258320.955"}, "lookback-delta": []string{"2s"}},
			command: `absent(up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258320.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  absent",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258328.955"}, "lookback-delta": []string{"2s"}},
			command: `absent(up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query:  absent",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"10s"}, "lookback-delta": []string{"1s"}},
			command: `absent(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258322.955,"1"],[1709258332.955,"1"],[1709258352.955,"1"],[1709258362.955,"1"],[1709258372.955,"1"],[1709258382.955,"1"],[1709258392.955,"1"],[1709258402.955,"1"],[1709258412.955,"1"],[1709258422.955,"1"],[1709258432.955,"1"],[1709258442.955,"1"],[1709258452.955,"1"],[1709258462.955,"1"],[1709258472.955,"1"],[1709258482.955,"1"],[1709258492.955,"1"],[1709258502.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258320.955"}},
			command: `absent_over_time(up[5s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258320.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258328.955"}},
			command: `absent_over_time(up[5s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"10s"}},
			command: `absent_over_time(up[5s])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258322.955,"1"],[1709258352.955,"1"],[1709258372.955,"1"],[1709258382.955,"1"],[1709258392.955,"1"],[1709258402.955,"1"],[1709258412.955,"1"],[1709258422.955,"1"],[1709258432.955,"1"],[1709258442.955,"1"],[1709258452.955,"1"],[1709258462.955,"1"],[1709258472.955,"1"],[1709258482.955,"1"],[1709258492.955,"1"],[1709258502.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"10s"}},
			command: `absent_over_time(up{instance="localhost:9090"}[5s])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090"},"values":[[1709258322.955,"1"],[1709258352.955,"1"],[1709258362.955,"1"],[1709258372.955,"1"],[1709258382.955,"1"],[1709258392.955,"1"],[1709258402.955,"1"],[1709258412.955,"1"],[1709258422.955,"1"],[1709258432.955,"1"],[1709258442.955,"1"],[1709258452.955,"1"],[1709258462.955,"1"],[1709258472.955,"1"],[1709258482.955,"1"],[1709258492.955,"1"],[1709258502.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"10s"}},
			command: `absent_over_time(up{instance="localhost:9090",instance="localhost:9090"}[5s])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258322.955,"1"],[1709258352.955,"1"],[1709258362.955,"1"],[1709258372.955,"1"],[1709258382.955,"1"],[1709258392.955,"1"],[1709258402.955,"1"],[1709258412.955,"1"],[1709258422.955,"1"],[1709258432.955,"1"],[1709258442.955,"1"],[1709258452.955,"1"],[1709258462.955,"1"],[1709258472.955,"1"],[1709258482.955,"1"],[1709258492.955,"1"],[1709258502.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query:  metric * +Inf",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258328.955"}},
			command: `up * +Inf`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258328.955,"+Inf"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  metric * -Inf",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258328.955"}},
			command: `up * -Inf`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258328.955,"-Inf"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  metric * NaN",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258328.955"}},
			command: `up * NaN`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258328.955,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  absent with no point",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258320.955"}, "lookback-delta": []string{"2s"}},
			command: `absent(sum(up) by (job))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258320.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query:  absent with no point",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258417.955"}, "end": []string{"1709258477.955"}, "step": []string{"10s"}, "lookback-delta": []string{"1s"}},
			command: `absent(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258417.955,"1"],[1709258427.955,"1"],[1709258437.955,"1"],[1709258447.955,"1"],[1709258457.955,"1"],[1709258467.955,"1"],[1709258477.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  absent with no shard",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1730104970.955"}, "end": []string{"1730104990.955"}, "step": []string{"10s"}, "lookback-delta": []string{"1s"}},
			command: `absent(up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  absent with no tag",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258417.955"}, "end": []string{"1709258477.955"}, "step": []string{"10s"}, "lookback-delta": []string{"1s"}},
			command: `absent(up{test="a"})`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"test":"a"},"values":[[1709258417.955,"1"],[1709258427.955,"1"],[1709258437.955,"1"],[1709258447.955,"1"],[1709258457.955,"1"],[1709258467.955,"1"],[1709258477.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  absent with no mst",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258417.955"}, "end": []string{"1709258477.955"}, "step": []string{"10s"}, "lookback-delta": []string{"1s"}},
			command: `absent(test)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258417.955,"1"],[1709258427.955,"1"],[1709258437.955,"1"],[1709258447.955,"1"],[1709258457.955,"1"],[1709258467.955,"1"],[1709258477.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: mad_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `mad_over_time(up[3m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:7070","job":"container"},"values":[[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:8080","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"container"},"values":[[1709258342.955,"0"],[1709258372.955,"0"],[1709258402.955,"0"],[1709258432.955,"0"],[1709258462.955,"0"],[1709258492.955,"0"]]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258312.955,"0"],[1709258342.955,"1"],[1709258372.955,"1"],[1709258402.955,"1"],[1709258432.955,"1"],[1709258462.955,"1"],[1709258492.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromWithError(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "range query: label_replace + topk",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `label_replace(topk(5,count(up>=0) by (job)),"name","kube_pod_status_phase","","")`,
			exp:     `unexpected status code: code=422, body={"status":"error","errorType":"execution","error":"populatePromSeries raise err: vector cannot contain metrics with the same labelset"}`,
			path:    "/api/v1/query_range",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			if query.exp != err.Error() {
				t.Error(query.Error(err))
			}
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Scalar(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`down,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=up,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=up,instance=localhost:9090,job=prometheus value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=up,instance=localhost:9090,job=prometheus value=6 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  scalar",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258507.955"}},
			command: `scalar(down)`,
			exp:     `{"status":"success","data":{"resultType":"scalar","result":[1709258507.955,"6"]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: scalar",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258507.955"}, "step": []string{"30s"}},
			command: `scalar(down)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258312.955,"1"],[1709258342.955,"5"],[1709258372.955,"6"],[1709258402.955,"6"],[1709258432.955,"6"],[1709258462.955,"6"],[1709258492.955,"6"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Bool_Modifier(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query: vector + (scalar == bool scalar)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up + ( 1 == bool 2 )`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: vector + (scalar != bool scalar)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up + ( 1 != bool 2 )`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258357.955,"7"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  vector bool scalar",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up >= bool 3`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: scalar bool vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `3 <= bool up`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258357.955,"1"]},{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  vector bool scalar",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `sum(up) > bool 3`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: scalar bool vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `3 < bool sum(up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Offset(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query: vector offset",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up offset 15s`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: sum vector offset",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `sum(up offset 15s)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258357.955,"12"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: rate vector offset",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `rate(up[1m] offset 15s)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"0.05"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: vector offset",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "step": []string{"15s"}},
			command: `up offset 15s`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"values":[[1709258357.955,"5"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"values":[[1709258357.955,"4"]]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"values":[[1709258327.955,"1"],[1709258342.955,"2"],[1709258357.955,"3"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: sum vector offset",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "step": []string{"15s"}},
			command: `sum(up offset 15s)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258327.955,"1"],[1709258342.955,"2"],[1709258357.955,"12"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  rate vector offset",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "step": []string{"15s"}},
			command: `rate(up[1m] offset 15s)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1709258342.955,"0.03333333333333333"],[1709258357.955,"0.05"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_At_Modifier(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query: vector @",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"0"}},
			command: `up @ 1709258357.955`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"value":[0,"6"]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[0,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[0,"4"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[0,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: vector @ offset",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"0"}},
			command: `up @ 1709258357.955 offset 15s`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[0,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[0,"4"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[0,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: sum vector @",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"0"}},
			command: `sum(up @ 1709258357.955)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"18"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: rate vector @",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"0"}},
			command: `rate(up[1m] @ 1709258357.955)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[0,"0.06666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query: rate vector @",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"-3000"}},
			command: `rate(up[1m] @ 1709258357.955)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[-3000,"0.06666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: sum vector @ start",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258357.955"}, "end": []string{"1709258357.955"}, "step": []string{"15s"}},
			command: `sum(up @ start())`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258357.955,"18"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: sum vector @ end",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258357.955"}, "end": []string{"1709258357.955"}, "step": []string{"15s"}},
			command: `sum(up @ end())`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258357.955,"18"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query: sum vector @ end",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"-30"}, "end": []string{"30"}, "step": []string{"15s"}},
			command: `sum(up @ 1709258357.955)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[-30,"18"],[-15,"18"],[0,"18"],[15,"18"],[30,"18"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

type FileInfo struct {
	enabled bool
	rate    float64
}

func TestServer_Prom_Evaluations(t *testing.T) {
	files, err := filepath.Glob("./testdata/*.test")
	require.NoError(t, err)

	fileMap := map[string]*FileInfo{
		"testdata/aggregators.test": {enabled: false},
		"testdata/at_modifier.test": {enabled: false},
		"testdata/collision.test":   {enabled: true},
		"testdata/functions.test":   {enabled: false},
		"testdata/histograms.test":  {enabled: true},
		"testdata/literals.test":    {enabled: true},
		"testdata/operators.test":   {enabled: true},
		"testdata/selectors.test":   {enabled: true},
		"testdata/staleness.test":   {enabled: true},
		"testdata/subquery.test":    {enabled: true},
	}

	var wg sync.WaitGroup
	for _, fn := range files {
		f, ok := fileMap[fn]
		if !ok || !f.enabled {
			continue
		}
		wg.Add(1)
		t.Run(fn, func(t *testing.T) {
			rate := runTestFile(t, fn)
			f.rate = rate
			wg.Done()
		})
	}

	wg.Wait()
	for n, f := range fileMap {
		if f.enabled {
			fmt.Printf("%s success rate: %f \n", n, f.rate)
		}
	}
}

func runTestFile(t *testing.T, fn string) float64 {
	s := OpenPromServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	rate, err := NewPromTestFromFile(t, fn, "prom", "autogen", s)
	if err != nil {
		t.Error(err)
	}
	return rate
}

func TestServer_PromQuery_timestamp(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	valueGap := 10
	startValue := 0
	valueNum := 10
	timeGap := 5 * 60 * 1000 * 1000 * 1000
	startTime := 0
	writes := make([]string, 0)
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 20
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 30
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 40
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 50
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 60
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 70
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 80
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=1,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query timestamp(http_requests{group='canary',instance='0',job='api-server'})",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:40:00Z"}},
			command: `timestamp(http_requests{group='canary',instance='0',job='api-server'})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[2400,"2400"]}]}}`,
			path:    "/api/v1/query",
		},

		&Query{
			name:    "instant query timestamp(3 + http_requests{group='canary',instance='0',job='api-server'} + 1)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:40:00Z"}},
			command: `3 + timestamp(http_requests{group='canary',instance='0',job='api-server'}) + 1`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[2400,"2404"]}]}}`,
			path:    "/api/v1/query",
		},

		&Query{
			name:    "instant query abs(http_requests{group='canary',instance='0',job='api-server'}) + timestamp(http_requests{group='canary',instance='0',job='api-server'})",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:40:00Z"}},
			command: `abs(http_requests{group='canary',instance='0',job='api-server'}) + timestamp(http_requests{group='canary',instance='0',job='api-server'})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[2400,"2640"]}]}}`,
			path:    "/api/v1/query",
		},

		&Query{
			name:    "(timestamp(http_requests{group='canary',instance='0',job='api-server'}) + 1) * 2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:40:00Z"}},
			command: `(timestamp(http_requests{group='canary',instance='0',job='api-server'}) + 1) * 2`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[2400,"4802"]}]}}`,
			path:    "/api/v1/query",
		},

		&Query{
			name:    "(1 + timestamp(vector(1))) * 2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:40:00Z"}},
			command: `(1 + timestamp(vector(1))) * 2`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[2400,"4802"]}]}}`,
			path:    "/api/v1/query",
		},

		&Query{
			name:    "(timestamp(http_requests{group='canary',instance='0',job='api-server'} + 3) + 1) * 2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:40:00Z"}},
			command: `(timestamp(http_requests{group='canary',instance='0',job='api-server'} + 3) + 1) * 2`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[2400,"4802"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// testData/*.test: load [timeGap] mst{tags} [startValue]+[valueGap]x[valueNum]...
func TestServer_PromQuery_Operators1(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	valueGap := 10
	startValue := 0
	valueNum := 10
	timeGap := 5 * 60 * 1000 * 1000 * 1000
	startTime := 0
	writes := make([]string, 0)
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 20
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 30
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 40
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 50
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 60
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 70
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 80
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=1,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	valueGap = 1
	valueNum = 100
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`vector_matching_a,__name__=vector_matching_a,l=x value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 2
	valueNum = 50
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`vector_matching_a,__name__=vector_matching_a,l=y value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 4
	valueNum = 25
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`vector_matching_b,__name__=vector_matching_b,l=x value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query sum - count",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) - COUNT(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"996"]},{"metric":{"job":"app-server"},"value":[3000,"2596"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query 2 - sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `2 - SUM(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"-998"]},{"metric":{"job":"app-server"},"value":[3000,"-2598"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query count ^ count",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `COUNT(http_requests) BY (job) ^ COUNT(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"256"]},{"metric":{"job":"app-server"},"value":[3000,"256"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum + sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) + SUM(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"2000"]},{"metric":{"job":"app-server"},"value":[3000,"5200"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector + rate*const",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{job="api-server", group="canary"} + rate(http_requests{job="api-server"}[5m]) * 5 * 60`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"330"]},{"metric":{"group":"canary","instance":"1","job":"api-server"},"value":[3000,"440"]}]}}`,
			path:    "/api/v1/query",
		},
		// start condition
		&Query{
			name:    "instant query vector AND vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} and http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[3000,"300"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[3000,"700"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND on(multi) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and on(instance, job) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND on(single) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and on(instance) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND ignore(single) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and ignoring(group) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND ignore(multi) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and ignoring(group, job) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector OR vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} or http_requests{group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[3000,"300"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[3000,"700"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[3000,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[3000,"800"]},{"metric":{"__name__":"http_requests","group":"production","instance":"0","job":"api-server"},"value":[3000,"100"]},{"metric":{"__name__":"http_requests","group":"production","instance":"0","job":"app-server"},"value":[3000,"500"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"api-server"},"value":[3000,"200"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"app-server"},"value":[3000,"600"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) OR vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) or http_requests{instance="1"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]},{"metric":{"group":"canary","instance":"1","job":"api-server"},"value":[3000,"401"]},{"metric":{"group":"canary","instance":"1","job":"app-server"},"value":[3000,"801"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"api-server"},"value":[3000,"200"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"app-server"},"value":[3000,"600"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:   "instant query (vector+1) OR on(single) (vector or vector or vector)",
			params: url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			// prom_src_cmd: command: `(http_requests{group="canary"} + 1) or on(instance) (http_requests or cpu_count or vector_matching_a)`,
			command: `(http_requests{group="canary"} + 1) or on(instance) (http_requests or vector_matching_a)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]},{"metric":{"group":"canary","instance":"1","job":"api-server"},"value":[3000,"401"]},{"metric":{"group":"canary","instance":"1","job":"app-server"},"value":[3000,"801"]},{"metric":{"__name__":"vector_matching_a","l":"x"},"value":[3000,"10"]},{"metric":{"__name__":"vector_matching_a","l":"y"},"value":[3000,"20"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:   "instant query (vector+1) OR ignore(multi) (vector or vector or vector)",
			params: url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			// prom_src_cmd: command: `(http_requests{group="canary"} + 1) or ignoring(l, group, job) (http_requests or cpu_count or vector_matching_a)`,
			command: `(http_requests{group="canary"} + 1) or ignoring(l, group, job) (http_requests or vector_matching_a)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"301"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"701"]},{"metric":{"group":"canary","instance":"1","job":"api-server"},"value":[3000,"401"]},{"metric":{"group":"canary","instance":"1","job":"app-server"},"value":[3000,"801"]},{"metric":{"__name__":"vector_matching_a","l":"x"},"value":[3000,"10"]},{"metric":{"__name__":"vector_matching_a","l":"y"},"value":[3000,"20"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector UNLESS vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} unless http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[3000,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[3000,"800"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector UNLESS on(single) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} unless on(job) http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector UNLESS on(multi) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} unless on(job, instance) http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[3000,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[3000,"800"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector UNLESS ignore(multi) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} unless ignoring(group, instance) http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector UNLESS ignore(single) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} unless ignoring(group) http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[3000,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[3000,"800"]}]}}`,
			path:    "/api/v1/query",
		},
		// end condition
		&Query{
			name:    "instant query vector / ignore vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} / ignoring(group) http_requests{group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0","job":"api-server"},"value":[3000,"3"]},{"metric":{"instance":"0","job":"app-server"},"value":[3000,"1.4"]},{"metric":{"instance":"1","job":"api-server"},"value":[3000,"2"]},{"metric":{"instance":"1","job":"app-server"},"value":[3000,"1.3333333333333333"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector / on(multi) ignore vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} / on(instance,job) http_requests{group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0","job":"api-server"},"value":[3000,"3"]},{"metric":{"instance":"0","job":"app-server"},"value":[3000,"1.4"]},{"metric":{"instance":"1","job":"api-server"},"value":[3000,"2"]},{"metric":{"instance":"1","job":"app-server"},"value":[3000,"1.3333333333333333"]}]}}`,
			path:    "/api/v1/query",
		},
		// start bool comparisons
		&Query{
			name:    "instant query sum == bool sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) == bool SUM(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"1"]},{"metric":{"job":"app-server"},"value":[3000,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum != bool sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) != bool SUM(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"0"]},{"metric":{"job":"app-server"},"value":[3000,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		// end bool comparisons
		&Query{
			name:    "instant query vector + time() > 3600",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `count by(group) (http_requests{} - time() < -2500)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary"},"value":[3000,"2"]},{"metric":{"group":"production"},"value":[3000,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query avg(rate(range vector)) / avg(instant vector)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg by (instance,job) (rate(http_requests[5m])) / avg by (instance,job) (http_requests)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0","job":"api-server"},"value":[3000,"0.0003333333333333333"]},{"metric":{"instance":"0","job":"app-server"},"value":[3000,"0.0003333333333333334"]},{"metric":{"instance":"1","job":"api-server"},"value":[3000,"0.0003333333333333334"]},{"metric":{"instance":"1","job":"app-server"},"value":[3000,"0.0003333333333333333"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query with self join: http_requests",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests + http_requests + http_requests + http_requests`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[3000,"1200"]},{"metric":{"group":"canary","instance":"0","job":"app-server"},"value":[3000,"2800"]},{"metric":{"group":"canary","instance":"1","job":"api-server"},"value":[3000,"1600"]},{"metric":{"group":"canary","instance":"1","job":"app-server"},"value":[3000,"3200"]},{"metric":{"group":"production","instance":"0","job":"api-server"},"value":[3000,"400"]},{"metric":{"group":"production","instance":"0","job":"app-server"},"value":[3000,"2000"]},{"metric":{"group":"production","instance":"1","job":"api-server"},"value":[3000,"800"]},{"metric":{"group":"production","instance":"1","job":"app-server"},"value":[3000,"2400"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query: http_requests",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1970-01-01T00:00:00Z"}, "end": []string{"1970-01-01T00:50:00Z"}, "step": []string{"15m"}},
			command: `http_requests`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"values":[[0,"0"],[900,"90"],[1800,"180"],[2700,"270"]]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"values":[[0,"0"],[900,"210"],[1800,"420"],[2700,"630"]]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"values":[[0,"0"],[900,"120"],[1800,"240"],[2700,"360"]]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"values":[[0,"0"],[900,"240"],[1800,"480"],[2700,"720"]]},{"metric":{"__name__":"http_requests","group":"production","instance":"0","job":"api-server"},"values":[[0,"0"],[900,"30"],[1800,"60"],[2700,"90"]]},{"metric":{"__name__":"http_requests","group":"production","instance":"0","job":"app-server"},"values":[[0,"0"],[900,"150"],[1800,"300"],[2700,"450"]]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"api-server"},"values":[[0,"0"],[900,"60"],[1800,"120"],[2700,"180"]]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"app-server"},"values":[[0,"0"],[900,"180"],[1800,"360"],[2700,"540"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "instant query with self join: http_requests",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) % 2 ^ 3 ^ 2 ^ 2`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"1000"]},{"metric":{"job":"app-server"},"value":[3000,"2600"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// group_left/group_right
func TestServer_PromQuery_Operators2(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	startTime := 0
	writes := make([]string, 0)

	writes = append(writes, fmt.Sprintf(`node_var,__name__=node_var,instance=abc,job=node value=%d %d`, 2, startTime))
	writes = append(writes, fmt.Sprintf(`node_role,__name__=node_role,instance=abc,job=node,role=prometheus value=%d %d`, 1, startTime))

	writes = append(writes, fmt.Sprintf(`node_cpu,__name__=node_cpu,instance=abc,job=node,mode=idle value=%d %d`, 3, startTime))
	writes = append(writes, fmt.Sprintf(`node_cpu,__name__=node_cpu,instance=abc,job=node,mode=user value=%d %d`, 1, startTime))
	writes = append(writes, fmt.Sprintf(`node_cpu,__name__=node_cpu,instance=def,job=node,mode=idle value=%d %d`, 8, startTime))
	writes = append(writes, fmt.Sprintf(`node_cpu,__name__=node_cpu,instance=def,job=node,mode=user value=%d %d`, 2, startTime))

	writes = append(writes, fmt.Sprintf(`random,__name__=random,foo=bar value=%d %d`, 1, startTime))

	writes = append(writes, fmt.Sprintf(`threshold,__name__=threshold,instance=abc,job=node,target=a@b.com value=%d %d`, 0, startTime))

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query vector on right vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_role * on (instance) group_right (role) node_var`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[300,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector on left vector 1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_var * on (instance) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[300,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector ignore left vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_var * ignoring (role) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[300,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector ignore right vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_role * ignoring (role) group_right (role) node_var`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[300,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector ignore(multi) right vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu * ignoring (role, mode) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","mode":"idle","role":"prometheus"},"value":[300,"3"]},{"metric":{"instance":"abc","job":"node","mode":"user","role":"prometheus"},"value":[300,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector on left vector 2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu * on (instance) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","mode":"idle","role":"prometheus"},"value":[300,"3"]},{"metric":{"instance":"abc","job":"node","mode":"user","role":"prometheus"},"value":[300,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector on left sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu / on (instance) group_left sum by (instance,job)(node_cpu)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","mode":"idle"},"value":[300,"0.75"]},{"metric":{"instance":"abc","job":"node","mode":"user"},"value":[300,"0.25"]},{"metric":{"instance":"def","job":"node","mode":"idle"},"value":[300,"0.8"]},{"metric":{"instance":"def","job":"node","mode":"user"},"value":[300,"0.2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum on left sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"node","mode":"idle"},"value":[300,"0.7857142857142857"]},{"metric":{"job":"node","mode":"user"},"value":[300,"0.21428571428571427"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum(sum on left sum)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `sum(sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[300,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector > on left",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu > on(job, instance) group_left(target) threshold`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"node_cpu","instance":"abc","job":"node","mode":"idle","target":"a@b.com"},"value":[300,"3"]},{"metric":{"__name__":"node_cpu","instance":"abc","job":"node","mode":"user","target":"a@b.com"},"value":[300,"1"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Operators3(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	startTime := 0
	writes := make([]string, 0)

	writes = append(writes, fmt.Sprintf(`random,__name__=random,foo=bar value=%d %d`, 2, startTime))
	writes = append(writes, fmt.Sprintf(`metricA,__name__=metricA,baz=meh value=%d %d`, 3, startTime))
	writes = append(writes, fmt.Sprintf(`metricB,__name__=metricB,baz=meh value=%d %d`, 4, startTime))

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query on nil",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `random + on() metricA`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[300,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query ignore nil",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `metricA + ignoring() metricB`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"baz":"meh"},"value":[300,"7"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query normal",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `metricA + metricB`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"baz":"meh"},"value":[300,"7"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Operators4(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	startTime := 0
	writes := make([]string, 0)

	writes = append(writes, fmt.Sprintf(`test_total,__name__=test_total,instance=localhost value=%d %d`, 50, startTime))
	writes = append(writes, fmt.Sprintf(`test_smaller,__name__=test_smaller,instance=localhost value=%d %d`, 10, startTime))

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query vector > bool vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `test_total > bool test_smaller`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost"},"value":[300,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector > vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `test_total > test_smaller`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"test_total","instance":"localhost"},"value":[300,"50"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector < bool vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `test_total < bool test_smaller`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost"},"value":[300,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector < vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `test_total < test_smaller`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Operators5(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := make([]string, 0)
	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query vector(1)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `vector(1)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector(time()) 0s",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `vector(time())`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector(time()) 5s",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:05Z"}},
			command: `vector(time())`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[5,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector(time()) 60m",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T01:00:00Z"}},
			command: `vector(time())`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[3600,"3600"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query year() 0s",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `year()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1970"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query time() 50m",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `time()`,
			exp:     `{"status":"success","data":{"resultType":"scalar","result":[3000,"3000"]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query year(vector(1136239445))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `year(vector(1136239445))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"2006"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query month()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `month()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query month(vector(1136239445))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `month(vector(1136239445))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query day_of_month()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `day_of_month()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query day_of_month(vector(1136239445))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `day_of_month(vector(1136239445))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query day_of_week()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `day_of_week()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query day_of_week(vector(1136239445))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `day_of_week(vector(1136239445))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query day_of_year()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `day_of_year()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query day_of_year(vector(1136239445))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `day_of_year(vector(1136239445))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query hour()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `hour()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query hour(vector(1136239445))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `hour(vector(1136239445))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"22"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query minute()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `minute()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query minute(vector(1136239445))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `minute(vector(1136239445))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query year(vector(1230767999))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `year(vector(1230767999))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"2008"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query year(vector(1230768000))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `year(vector(1230768000))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"2009"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query month(vector(1456790399)) + day_of_month(vector(1456790399)) / 100",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `month(vector(1456790399)) + day_of_month(vector(1456790399)) / 100`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"2.29"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query month(vector(1456790400)) + day_of_month(vector(1456790400)) / 100",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `month(vector(1456790400)) + day_of_month(vector(1456790400)) / 100`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"3.01"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query days_in_month(vector(1454284800))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `days_in_month(vector(1454284800))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"29"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query days_in_month(vector(1485907200))",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:00Z"}},
			command: `days_in_month(vector(1485907200))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"28"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Operators6(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	startTime := 1704067200000000000 // which mapped to 2024-01-01T00:00:10Z
	writes := make([]string, 0)

	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=localhost,version=canary,os=linux value=%d %d`, 1, startTime))
	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=localhost,version=canary,os=windows value=%d %d`, 2, startTime))
	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=localhost,version=beta,os=linux value=%d %d`, 3, startTime))
	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=localhost,version=beta,os=windows value=%d %d`, 4, startTime))
	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=10.0.0.1,version=canary,os=linux value=%d %d`, 5, startTime))
	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=10.0.0.1,version=canary,os=windows value=%d %d`, 6, startTime))
	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=10.0.0.1,version=beta,os=linux value=%d %d`, 7, startTime))
	writes = append(writes, fmt.Sprintf(`test,__name__=test,instance=10.0.0.1,version=beta,os=windows value=%d %d`, 8, startTime))

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "instant query: group",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}, "inner_chunk_size": []string{"1"}},
			command: `group by(version) (test + test)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"version":"beta"},"value":[1704067210,"1"]},{"metric":{"version":"canary"},"value":[1704067210,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: group",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `group without(version) (test + test)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"10.0.0.1","os":"linux"},"value":[1704067210,"1"]},{"metric":{"instance":"10.0.0.1","os":"windows"},"value":[1704067210,"1"]},{"metric":{"instance":"localhost","os":"linux"},"value":[1704067210,"1"]},{"metric":{"instance":"localhost","os":"windows"},"value":[1704067210,"1"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// histogram
func TestServer_PromQuery_Histogram0(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	initTime := 1713768282462000000
	buckets := []string{"+Inf", "0.1", "0.2", "0.4", "1", "120", "20", "3", "60", "8"}
	writes := make([]string, 0, 2*len(buckets))
	for i := 0; i < len(buckets); i++ {
		for j := 0; j < 5; j++ {
			time := int64(initTime) + 15*int64(time.Second)*int64(j)
			str := fmt.Sprintf(`up,__name__=up,instance=localhost:9090,le=%s,zzz=prometheus value=%d %d`, buckets[i], j+1, time)
			writes = append(writes, str)
			str = fmt.Sprintf(`up,__name__=up,instance=localhost:9090,le=%s,zzz=prometheus2 value=%d %d`, buckets[i], j*1000, time)
			writes = append(writes, str)
		}

	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  histogram_quantile",
			skip:    true,
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1713768282.462"}},
			command: `histogram_quantile(0.9,up)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","zzz":"prometheus"},"value":[1713768282.462,"0.09000000000000001"]},{"metric":{"instance":"localhost:9090","zzz":"prometheus2"},"value":[1713768282.462,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "range query:  histogram_quantile",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1713768282.462"}, "end": []string{"1713768432.462"}, "step": []string{"30s"}},
			command: `histogram_quantile(0.9,up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","zzz":"prometheus"},"values":[[1713768282.462,"0.09000000000000001"],[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"],[1713768402.462,"0.09000000000000001"],[1713768432.462,"0.09000000000000001"]]},{"metric":{"instance":"localhost:9090","zzz":"prometheus2"},"values":[[1713768282.462,"NaN"],[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"],[1713768402.462,"0.09000000000000001"],[1713768432.462,"0.09000000000000001"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  histogram_quantile(sum(increase))",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1713768282.462"}, "end": []string{"1713768432.462"}, "step": []string{"30s"}},
			command: `histogram_quantile(0.9,sum(increase(up[1m])) by (le,zzz))`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"zzz":"prometheus"},"values":[[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"]]},{"metric":{"zzz":"prometheus2"},"values":[[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "range query:  histogram_quantile(rate)",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1713768282.462"}, "end": []string{"1713768432.462"}, "step": []string{"30s"}},
			command: `histogram_quantile(0.9,rate(up[1m]))`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","zzz":"prometheus"},"values":[[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"]]},{"metric":{"instance":"localhost:9090","zzz":"prometheus2"},"values":[[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		&Query{
			name:    "query offset",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"-30060"}, "end": []string{"-29940"}, "step": []string{"60"}},
			command: `avg by (le) (rate(up[5m] @ 1713768342.462))`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"le":"+Inf"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"0.1"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"0.2"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"0.4"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"1"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"120"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"20"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"3"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"60"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]},{"metric":{"le":"8"},"values":[[-30060,"6.675000000000001"],[-30000,"6.675000000000001"],[-29940,"6.675000000000001"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Histogram1(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	initTime := 1713768282462000000
	buckets := []string{"+Inf", "0.1", "0.2", "0.4", "1", "120", "20", "3", "60", "8"}
	writes := make([]string, 0, 2*len(buckets))
	for i := 0; i < len(buckets); i++ {
		for j := 0; j < 5; j++ {
			time := int64(initTime) + 15*int64(time.Second)*int64(j)
			str := fmt.Sprintf(`up,__name__=up,instance=localhost:9090,le=%s,zzz=prometheus value=%d %d`, buckets[i], j+1, time)
			writes = append(writes, str)
			str = fmt.Sprintf(`up,__name__=up,instance=localhost:9090,le=%s,zzz=prometheus2 value=%d %d`, buckets[i], j*1000, time)
			writes = append(writes, str)

			str = fmt.Sprintf(`up,__name__=up,instance=localhost:9090,notle=abc,zzz=prometheus2 value=%d %d`, j*1000, time)
			writes = append(writes, str)
		}

	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		&Query{
			name:    "range query:  histogram_quantile",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1713768282.462"}, "end": []string{"1713768432.462"}, "step": []string{"30s"}, "inner_chunk_size": []string{"3"}},
			command: `histogram_quantile(0.9,up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","zzz":"prometheus"},"values":[[1713768282.462,"0.09000000000000001"],[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"],[1713768402.462,"0.09000000000000001"],[1713768432.462,"0.09000000000000001"]]},{"metric":{"instance":"localhost:9090","zzz":"prometheus2"},"values":[[1713768282.462,"NaN"],[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"],[1713768402.462,"0.09000000000000001"],[1713768432.462,"0.09000000000000001"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Histogram2(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	initTime := 1713768282462000000
	buckets := []string{"+Inf", "0.1", "0.2", "0.4", "1", "120", "20", "3", "60", "8"}
	writes := make([]string, 0, 2*len(buckets))
	for i := 0; i < len(buckets); i++ {
		for j := 0; j < 5; j++ {
			time := int64(initTime) + 15*int64(time.Second)*int64(j)
			str := fmt.Sprintf(`up,__name__=up,instance=localhost:9090,le=%s,zzz=prometheus value=%d %d`, buckets[i], j+1, time)
			writes = append(writes, str)
			//str = fmt.Sprintf(`up,__name__=up,instance=localhost:9090,le=%s,zzz=prometheus2 value=%d %d`, buckets[i], j*1000, time)
			//writes = append(writes, str)
		}

	}
	str := fmt.Sprintf(`up,__name__=up,instance=localhost:9090,le=8,zzz=prometheus value=%d %d`, 7, int64(initTime)+15*int64(time.Second)*7)
	writes = append(writes, str)

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		&Query{
			name:    "range query:  histogram_quantile",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1713768282.462"}, "end": []string{"1713768582.462"}, "step": []string{"15s"}, "lookback-delta": []string{"10s"}},
			command: `histogram_quantile(0.9,up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","zzz":"prometheus"},"values":[[1713768282.462,"0.09000000000000001"],[1713768297.462,"0.09000000000000001"],[1713768312.462,"0.09000000000000001"],[1713768327.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768387.462,"NaN"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Subquery1(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	startTime := 0
	writes := make([]string, 0)

	writes = append(writes, fmt.Sprintf(`metric,__name__=metric value=%d %d`, 1, startTime))
	writes = append(writes, fmt.Sprintf(`metric,__name__=metric value=%d %d`, 2, 10*1000*1000*1000))

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  rate(subquery)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:10Z"}},
			command: `rate(metric[20s:10s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[10,"0.1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  rate(subquery)2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:20Z"}},
			command: `rate(metric[20s:5s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[20,"0.05"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Subquery2(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	valueGap := 20
	startValue := 0
	valueNum := 1000
	timeGap := 10 * 1000 * 1000 * 1000
	startTime := 0
	writes := make([]string, 0)
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	startTime += (valueNum + 1) * timeGap
	startValue = 200
	valueGap = 30
	valueNum = 1000
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	startTime = 0
	startValue = 0
	valueGap = 10
	valueNum = 1000
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	startTime += (valueNum + 1) * timeGap
	startValue = 100
	valueGap = 30
	valueNum = 1000
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	startTime = 0
	startValue = 0
	valueGap = 30
	valueNum = 1000
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	startTime += (valueNum + 1) * timeGap
	startValue = 300
	valueGap = 80
	valueNum = 1000
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	startTime = 0
	startValue = 0
	valueGap = 40
	valueNum = 2000
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  rate(subquery)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"8000"}},
			command: `rate(http_requests{group=~"pro.*"}[1m:10s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"production","instance":"0","job":"api-server"},"value":[8000,"1"]},{"metric":{"group":"production","instance":"1","job":"api-server"},"value":[8000,"2"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Subquery3(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	values := []float64{1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229, 832040, 1346269, 2178309, 3524578, 5702887, 9227465, 14930352, 24157817, 39088169, 63245986, 102334155, 165580141, 267914296, 433494437, 701408733, 1134903170, 1836311903, 2971215073, 4807526976, 7778742049, 12586269025, 20365011074, 32951280099, 53316291173, 86267571272, 139583862445, 225851433717, 365435296162, 591286729879, 956722026041, 1548008755920, 2504730781961, 4052739537881, 6557470319842, 10610209857723, 17167680177565, 27777890035288, 44945570212853, 72723460248141, 117669030460994, 190392490709135, 308061521170129, 498454011879264, 806515533049393, 1304969544928657, 2111485077978050, 3416454622906707, 5527939700884757, 8944394323791464, 14472334024676221, 23416728348467685, 37889062373143906, 61305790721611591, 99194853094755497, 160500643816367088, 259695496911122585, 420196140727489673, 679891637638612258, 1100087778366101931, 1779979416004714189, 2880067194370816120, 4660046610375530309, 7540113804746346429, 12200160415121876738, 19740274219868223167, 31940434634990099905, 51680708854858323072, 83621143489848422977, 135301852344706746049, 218922995834555169026, 354224848179261915075, 573147844013817084101, 927372692193078999176, 1500520536206896083277, 2427893228399975082453, 3928413764606871165730, 6356306993006846248183, 10284720757613717413913, 16641027750620563662096, 26925748508234281076009, 43566776258854844738105, 70492524767089125814114, 114059301025943970552219, 184551825793033096366333, 298611126818977066918552, 483162952612010163284885, 781774079430987230203437, 1264937032042997393488322, 2046711111473984623691759, 3311648143516982017180081, 5358359254990966640871840, 8670007398507948658051921, 14028366653498915298923761, 22698374052006863956975682, 36726740705505779255899443, 59425114757512643212875125, 96151855463018422468774568, 155576970220531065681649693, 251728825683549488150424261, 407305795904080553832073954, 659034621587630041982498215, 1066340417491710595814572169, 1725375039079340637797070384, 2791715456571051233611642553, 4517090495650391871408712937, 7308805952221443105020355490, 11825896447871834976429068427, 19134702400093278081449423917, 30960598847965113057878492344, 50095301248058391139327916261, 81055900096023504197206408605, 131151201344081895336534324866, 212207101440105399533740733471, 343358302784187294870275058337, 555565404224292694404015791808, 898923707008479989274290850145, 1454489111232772683678306641953, 2353412818241252672952597492098, 3807901929474025356630904134051, 6161314747715278029583501626149, 9969216677189303386214405760200, 16130531424904581415797907386349, 26099748102093884802012313146549, 42230279526998466217810220532898, 68330027629092351019822533679447, 110560307156090817237632754212345, 178890334785183168257455287891792, 289450641941273985495088042104137, 468340976726457153752543329995929, 757791618667731139247631372100066, 1226132595394188293000174702095995, 1983924214061919432247806074196061, 3210056809456107725247980776292056, 5193981023518027157495786850488117, 8404037832974134882743767626780173, 13598018856492162040239554477268290, 22002056689466296922983322104048463, 35600075545958458963222876581316753, 57602132235424755886206198685365216, 93202207781383214849429075266681969, 150804340016807970735635273952047185, 244006547798191185585064349218729154, 394810887814999156320699623170776339, 638817435613190341905763972389505493, 1033628323428189498226463595560281832, 1672445759041379840132227567949787325, 2706074082469569338358691163510069157, 4378519841510949178490918731459856482, 7084593923980518516849609894969925639, 11463113765491467695340528626429782121, 18547707689471986212190138521399707760}
	timeGap := 7 * 1000 * 1000 * 1000
	startTime := 0
	writes := make([]string, 0)
	for i := 0; i < len(values); i++ {
		writes = append(writes, fmt.Sprintf(`metric,__name__=metric value=%f %d`, values[i], startTime+i*timeGap))
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  rate(range)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"80"}},
			command: `rate(metric[1m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[80,"2.517857142857143"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  rate(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"80"}},
			command: `rate(metric[1m:10s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[80,"2.3666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_MultiAgg_HashAgg(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	valueGap := 10
	startValue := 0
	valueNum := 10
	timeGap := 5 * 60 * 1000 * 1000 * 1000
	startTime := 0
	writes := make([]string, 0)
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 20
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 30
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 40
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 50
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=0,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 60
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=1,group=production value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 70
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=0,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}
	valueGap = 80
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=app-server,instance=1,group=canary value=%d %d`, startValue+i*valueGap, startTime+i*timeGap))
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// hashAgg start
		&Query{
			name:    "upper without(x) + lower groupByAll -> wihout(x) pushdown -> streamAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg without(instance) (rate(http_requests[5m]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","job":"api-server"},"value":[3000,"0.11666666666666667"]},{"metric":{"group":"canary","job":"app-server"},"value":[3000,"0.25"]},{"metric":{"group":"production","job":"api-server"},"value":[3000,"0.05"]},{"metric":{"group":"production","job":"app-server"},"value":[3000,"0.18333333333333335"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper without(x) + lower without(x, x) -> upper is subLower -> streamAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg without(job) (avg without(instance, job) (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary"},"value":[3000,"0.18333333333333335"]},{"metric":{"group":"production"},"value":[3000,"0.11666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper without(x) + lower without() -> upper not subLower -> hashAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg without(instance) (avg without() (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","job":"api-server"},"value":[3000,"0.11666666666666667"]},{"metric":{"group":"canary","job":"app-server"},"value":[3000,"0.25"]},{"metric":{"group":"production","job":"api-server"},"value":[3000,"0.05"]},{"metric":{"group":"production","job":"app-server"},"value":[3000,"0.18333333333333335"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper without(x) + lower by(x, x) -> upper is suffixLower -> streamAgg // aggExprTranpiler has perf to by(instance) by(instance, job) -> streamAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg without(job) (avg by(instance, job) (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0"},"value":[3000,"0.13333333333333333"]},{"metric":{"instance":"1"},"value":[3000,"0.16666666666666669"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper without(x) + lower by(x, x) -> upper not suffixLower -> hashAgg // aggExprTranpiler has perf to by(job) by(instance, job) -> hashAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg without(instance) (avg by(instance, job) (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"0.08333333333333334"]},{"metric":{"job":"app-server"},"value":[3000,"0.21666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper by(x) + lower without(x) -> upper is sameLower -> streamAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg by (job) (avg without (job) (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[3000,"0.15"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper by(x) + lower without(x) -> upper not sameLower -> hashAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg by (instance) (avg without (job) (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0"},"value":[3000,"0.13333333333333333"]},{"metric":{"instance":"1"},"value":[3000,"0.16666666666666669"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper by(x) + lower by(x) -> upper is prefixLower -> streamAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg by (instance) (avg by (instance, job) (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0"},"value":[3000,"0.13333333333333333"]},{"metric":{"instance":"1"},"value":[3000,"0.16666666666666669"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper by(x) + lower by(x) -> upper not prefixLower -> hashAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg by (job) (avg by (instance, job) (rate(http_requests[5m])))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"0.08333333333333334"]},{"metric":{"job":"app-server"},"value":[3000,"0.21666666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "upper by(x) + lower binop -> hashAgg",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `avg by (job) (http_requests + http_requests)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[3000,"500"]},{"metric":{"job":"app-server"},"value":[3000,"1300"]}]}}`,
			path:    "/api/v1/query",
		},
		// hashAgg end
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			if query.command == "avg without(job) (avg without(instance, job) (rate(http_requests[5m])))" {
				query.exp = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary"},"value":[3000,"0.18333333333333332"]},{"metric":{"group":"production"},"value":[3000,"0.11666666666666667"]}]}}`
				if !query.success() {
					t.Error(query.failureMessage())
				}
			} else {
				t.Error(query.failureMessage())
			}
		}
	}
}

func TestServer_PromQuery_Regular_Match(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{instance=~".*7070|.*8080"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{job=~".*tainer$"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{job=~"^prome.*"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{instance!~".*7070|.*8080"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{job!~".*tainer$"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `up{job!~"^prome.*"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_MetaData(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "label names without match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}},
			command: ``,
			exp:     `{"status":"success","data":["__name__","instance","job"]}`,
			path:    "/api/v1/labels",
		},
		&Query{
			name:    "labels names with match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}"}},
			command: ``,
			exp:     `{"status":"success","data":["__name__","instance","job"]}`,
			path:    "/api/v1/labels",
		},
		&Query{
			name:    "label values without match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}},
			command: ``,
			exp:     `{"status":"success","data":["localhost:7070","localhost:8080","localhost:9090"]}`,
			path:    "/api/v1/label/instance/values",
		},
		&Query{
			name:    "labels values with match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}"}},
			command: ``,
			exp:     `{"status":"success","data":["container"]}`,
			path:    "/api/v1/label/job/values",
		},
		&Query{
			name:    "series with match up",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up"}},
			command: ``,
			exp:     `{"status":"success","data":[{"__name__":"up","instance":"localhost:7070","job":"container"},{"__name__":"up","instance":"localhost:8080","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"prometheus"}]}`,
			path:    "/api/v1/series",
		},
		&Query{
			name:    "series with match up{job}",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}"}},
			command: ``,
			exp:     `{"status":"success","data":[{"__name__":"up","instance":"localhost:7070","job":"container"},{"__name__":"up","instance":"localhost:8080","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"container"}]}`,
			path:    "/api/v1/series",
		},
		&Query{
			name:    "metadata with match up{job}",
			params:  url.Values{"db": []string{"db0"}, "limit": []string{"10"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}", "up{job=\"prometheus\"}"}},
			command: ``,
			exp:     `{"status":"success"}`,
			path:    "/api/v1/metadata",
		},
		// with metric store
		&Query{
			name:    "label names without match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}},
			command: ``,
			exp:     `{"status":"success","data":["__name__","instance","job"]}`,
			path:    "/prometheus/up/api/v1/labels",
		},
		&Query{
			name:    "labels names with match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}"}},
			command: ``,
			exp:     `{"status":"success","data":["__name__","instance","job"]}`,
			path:    "/prometheus/up/api/v1/labels",
		},
		&Query{
			name:    "label values without match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}},
			command: ``,
			exp:     `{"status":"success","data":["localhost:7070","localhost:8080","localhost:9090"]}`,
			path:    "/prometheus/up/api/v1/label/instance/values",
		},
		&Query{
			name:    "labels values with match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}"}},
			command: ``,
			exp:     `{"status":"success","data":["container"]}`,
			path:    "/prometheus/up/api/v1/label/job/values",
		},
		&Query{
			name:    "series with match up",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up"}},
			command: ``,
			exp:     `{"status":"success","data":[{"__name__":"up","instance":"localhost:7070","job":"container"},{"__name__":"up","instance":"localhost:8080","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"prometheus"}]}`,
			path:    "/prometheus/up/api/v1/series",
		},
		&Query{
			name:    "series with match up{job}",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}"}},
			command: ``,
			exp:     `{"status":"success","data":[{"__name__":"up","instance":"localhost:7070","job":"container"},{"__name__":"up","instance":"localhost:8080","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"container"}]}`,
			path:    "/prometheus/up/api/v1/series",
		},
		&Query{
			name:    "labels names with match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}", "up{job=\"prometheus\"}"}},
			command: ``,
			exp:     `{"status":"success","data":["__name__","instance","job"]}`,
			path:    "/prometheus/up/api/v1/labels",
		},
		&Query{
			name:    "labels values with match",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}", "up{job=\"prometheus\"}"}},
			command: ``,
			exp:     `{"status":"success","data":["container","prometheus"]}`,
			path:    "/prometheus/up/api/v1/label/job/values",
		},
		&Query{
			name:    "series with match up{job}",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}", "up{job=\"prometheus\"}"}},
			command: ``,
			exp:     `{"status":"success","data":[{"__name__":"up","instance":"localhost:7070","job":"container"},{"__name__":"up","instance":"localhost:8080","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"prometheus"}]}`,
			path:    "/prometheus/up/api/v1/series",
		},
		&Query{
			name:    "metadata with match up{job}",
			params:  url.Values{"db": []string{"db0"}, "limit": []string{"10"}, "start": []string{"1709258312.955"}, "end": []string{"1709258357.955"}, "match[]": []string{"up{job=\"container\"}", "up{job=\"prometheus\"}"}},
			command: ``,
			exp:     `{"status":"success"}`,
			path:    "/prometheus/up/api/v1/metadata",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Compatibility(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=6 %d`, 1709258357955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=vm value=6 %d`, 1709258358955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=6 %d`, 1709258312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=5 %d`, 1709258327955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:9090,job=container value=3 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8080,job=container value=2 %d`, 1709258342955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=container value=1 %d`, 1709258357955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:7070,job=vm value=1 %d`, 1709258358955000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `{type="free", instance!="demo.promlabs.com:10000"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `last_over_time(up[1m])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:7070","job":"container"},"value":[1709258357.955,"6"]},{"metric":{"__name__":"up","instance":"localhost:8080","job":"container"},"value":[1709258357.955,"5"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258357.955,"4"]},{"metric":{"__name__":"up","instance":"localhost:9090","job":"prometheus"},"value":[1709258357.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "label names",
			params:  url.Values{"db": []string{"db0"}},
			command: ``,
			exp:     `{"status":"success","data":["__name__","instance","job"]}`,
			path:    "/api/v1/labels",
		},
		&Query{
			name:    "label values",
			params:  url.Values{"db": []string{"db0"}},
			command: ``,
			exp:     `{"status":"success","data":["container","prometheus","vm"]}`,
			path:    "/api/v1/label/job/values",
		},
		&Query{
			name:    "series",
			params:  url.Values{"db": []string{"db0"}, "match[]": []string{"up"}},
			command: ``,
			exp:     `{"status":"success","data":[{"__name__":"up","instance":"localhost:7070","job":"container"},{"__name__":"up","instance":"localhost:7070","job":"vm"},{"__name__":"up","instance":"localhost:8080","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"container"},{"__name__":"up","instance":"localhost:9090","job":"prometheus"}]}`,
			path:    "/api/v1/series",
		},
		&Query{
			name:    "label values exact",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258358.955"}, "end": []string{"1709258358.955"}, "exact": []string{"true"}},
			command: ``,
			exp:     `{"status":"success","data":["vm"]}`,
			path:    "/api/v1/label/job/values",
		},
		&Query{
			name:    "series exact",
			params:  url.Values{"db": []string{"db0"}, "match[]": []string{"up"}, "start": []string{"1709258358.955"}, "end": []string{"1709258358.955"}, "exact": []string{"true"}},
			command: ``,
			exp:     `{"status":"success","data":[{"__name__":"up","instance":"localhost:7070","job":"vm"}]}`,
			path:    "/api/v1/series",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Subquery_IrateAndIdelta(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric,__name__=metric value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric,__name__=metric value=2 %d`, 1704067201000000000),
		fmt.Sprintf(`metric,__name__=metric value=3 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,__name__=metric value=4 %d`, 1704067203000000000),
		fmt.Sprintf(`metric,__name__=metric value=7 %d`, 1704067204000000000),
		fmt.Sprintf(`metric,__name__=metric value=10 %d`, 1704067205000000000),
		fmt.Sprintf(`metric,__name__=metric value=14 %d`, 1704067206000000000),
		fmt.Sprintf(`metric,__name__=metric value=22 %d`, 1704067207000000000),
		fmt.Sprintf(`metric,__name__=metric value=25 %d`, 1704067208000000000),
		fmt.Sprintf(`metric,__name__=metric value=21 %d`, 1704067209000000000),
		fmt.Sprintf(`metric,__name__=metric value=21 %d`, 1704067210000000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  irate(subquery)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:00Z"}},
			command: `irate(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  irate(subquery)2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `irate(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  irate(subquery)3",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:09Z"}},
			command: `irate(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067209,"21"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  irate(subquery)4",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `irate(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  irate(subquery)5",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `irate(metric[10s:3s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"2.3333333333333335"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  irate(subquery)6",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `irate(metric[10s:5s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"2.2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  irate(subquery)7",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `irate(metric[5s:2s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"10.5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  idelta(subquery)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:00Z"}},
			command: `idelta(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  idelta(subquery)2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `idelta(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  idelta(subquery)3",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:09Z"}},
			command: `idelta(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067209,"-4"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  idelta(subquery)4",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `idelta(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  idelta(subquery)5",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `idelta(metric[10s:3s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"7"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  idelta(subquery)6",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `idelta(metric[10s:5s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"11"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  idelta(subquery)7",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `idelta(metric[5s:2s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"-4"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServerPromQuerySubQueryStdVarOverTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric,__name__=metric value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric,__name__=metric value=2 %d`, 1704067201000000000),
		fmt.Sprintf(`metric,__name__=metric value=3 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,__name__=metric value=4 %d`, 1704067203000000000),
		fmt.Sprintf(`metric,__name__=metric value=7 %d`, 1704067204000000000),
		fmt.Sprintf(`metric,__name__=metric value=10 %d`, 1704067205000000000),
		fmt.Sprintf(`metric,__name__=metric value=14 %d`, 1704067206000000000),
		fmt.Sprintf(`metric,__name__=metric value=22 %d`, 1704067207000000000),
		fmt.Sprintf(`metric,__name__=metric value=25 %d`, 1704067208000000000),
		fmt.Sprintf(`metric,__name__=metric value=26 %d`, 1704067209000000000),
		fmt.Sprintf(`metric,__name__=metric value=28 %d`, 1704067210000000000),
		fmt.Sprintf(`metric,__name__=metric value=38 %d`, 1704067211000000000),
		fmt.Sprintf(`metric,__name__=metric value=40 %d`, 1704067212000000000),
		fmt.Sprintf(`metric,__name__=metric value=1000000 %d`, 1704067213000000000),
		fmt.Sprintf(`metric,__name__=metric value=2 %d`, 1704067214000000000),
		fmt.Sprintf(`metric,__name__=metric value=200000000 %d`, 1704067215000000000),
		fmt.Sprintf(`metric,__name__=metric value=3 %d`, 1704067216000000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "Case: Beyond the left boundary",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:00Z"}},
			command: `stdvar_over_time(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "Case: Beyond the right boundary",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:40Z"}},
			command: `stdvar_over_time(metric[4s:2s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067240,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "Case: Overlap with the left border",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:01Z"}},
			command: `stdvar_over_time(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067201,"0.25"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "Case: Overlap with the right border",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:20Z"}},
			command: `stdvar_over_time(metric[5s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067220,"5555555388888890"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "Case: Normal situation",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `stdvar_over_time(metric[4s:2s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"54.88888888888889"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "Case: Huge data differences",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:16Z"}},
			command: `stdvar_over_time(metric[3s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067216,"7475187374375002"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "Case: IsDev = true",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `stddev_over_time(metric[4s:2s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"7.408703590297623"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_MultiAgg_HashAgg_StdvarAndStddev(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=2 %d`, 1704067201000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=3 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=4 %d`, 1704067203000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=7 %d`, 1704067204000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=10 %d`, 1704067205000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=14 %d`, 1704067206000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=22 %d`, 1704067207000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=25 %d`, 1704067208000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=21 %d`, 1704067209000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=1 value=21 %d`, 1704067210000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=2 value=4 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=2 value=7 %d`, 1704067203000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=2 value=10 %d`, 1704067204000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=2 value=14 %d`, 1704067205000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=2 value=22 %d`, 1704067206000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=3 value=25 %d`, 1704067207000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=3 value=28 %d`, 1704067208000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=3 value=21 %d`, 1704067209000000000),
		fmt.Sprintf(`metric,__name__=metric,instance=3 value=21 %d`, 1704067210000000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  stddev(query)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:00Z"}},
			command: `stddev(metric + metric + metric)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067200,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:00Z"}},
			command: `stddev(rate(metric[10s]) + rate(metric[10s]) + rate(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)3",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:00Z"}},
			command: `stddev(stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067200,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)4",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:04Z"}},
			command: `stddev(metric + metric + metric)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067204,"4.5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)5",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:04Z"}},
			command: `stddev(rate(metric[10s]) + rate(metric[10s]) + rate(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067204,"0.07500000000000018"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)6",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:04Z"}},
			command: `stddev(stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067204,"0.5855455718786668"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)7",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `stddev(metric + metric + metric)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"7.3484692283495345"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)8",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `stddev(rate(metric[10s]) + rate(metric[10s]) + rate(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"2.847586697538812"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stddev(query)9",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `stddev(stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"8.568310835456089"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stdvar(query)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `stdvar(metric + metric + metric)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stdvar(query)2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `stdvar(rate(metric[10s]) + rate(metric[10s]) + rate(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"7.336250000000001"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  stdvar(query)3",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `stdvar(stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]) + stddev_over_time(metric[10s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"49.745024058332234"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Subquery_FuncOverTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1 value=14 %d`, 1704067200000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=15 %d`, 1704067201000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=64 %d`, 1704067202000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=71 %d`, 1704067203000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=15 %d`, 1704067204000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=51 %d`, 1704067205000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=52 %d`, 1704067206000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=15 %d`, 1704067207000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=65 %d`, 1704067208000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=11 %d`, 1704067209000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=13 %d`, 1704067210000000000),

		fmt.Sprintf(`metric2,__name__=metric2 value=7 %d`, 1704067200000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=8 %d`, 1704067201000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=6 %d`, 1704067202000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=7 %d`, 1704067203000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=4 %d`, 1704067204000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=2 %d`, 1704067205000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=10 %d`, 1704067206000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=12 %d`, 1704067207000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=14 %d`, 1704067208000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=7 %d`, 1704067209000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=8 %d`, 1704067210000000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "instant query: min_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `min_over_time(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"11"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: max_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `max_over_time(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"71"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: sum_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `sum_over_time(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"386"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: avg_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `avg_over_time(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"35.09090909090909"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: count_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `count_over_time(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"11"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: rate(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `rate(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"18.7"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: delta(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `delta(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"-1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: increase(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `increase(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"187"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: mad_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `mad_over_time(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"4"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: min_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `min_over_time(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"-Inf"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: max_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `max_over_time(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"+Inf"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: sum_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `sum_over_time(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: avg_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `avg_over_time(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: count_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `count_over_time(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"11"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: rate(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `rate(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: delta(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `delta(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: increase(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `increase(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: mad_over_time(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `mad_over_time(((metric1 - 14) / (metric2 - 7))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"7.066666666666667"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "range query: increase(subquery)",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1704067200"}, "end": []string{"1704067210"}, "step": []string{"1s"}},
			command: `max_over_time((time() - max(metric1))[2s:1s] offset 2s)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1704067202,"1704067186"],[1704067203,"1704067186"],[1704067204,"1704067186"],[1704067205,"1704067186"],[1704067206,"1704067189"],[1704067207,"1704067189"],[1704067208,"1704067189"],[1704067209,"1704067192"],[1704067210,"1704067192"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Subquery_AvgOverTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{}
	for i := 1755149700000000000; i < 1755149760000000000; i += 1000000 {
		writes = append(writes, fmt.Sprintf(`metric1,__name__=metric1 value=14 %d`, i))
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "range query: increase(subquery)",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1755149760"}, "end": []string{"1755149760"}, "step": []string{"60s"}},
			command: `avg_over_time(metric1{}[59999ms])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1755149760,"14"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Test(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{}
	for i := 1754380800000000000; i < 1754380861000000000; i += 1000000 {
		writes = append(writes, fmt.Sprintf(`metric1,__name__=metric1 value=14 %d`, i))
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "range query: increase(subquery)",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1754380800"}, "end": []string{"1754380861"}, "step": []string{"3600"}},
			command: `metric1`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric1"},"values":[[1754380800,"14"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// fix: value [Comparison op] time_prom()
func TestServer_PromQuery_Comparison_Op_Fix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric,__name__=metric,tag=1 value=1 %d`, 0),
		fmt.Sprintf(`metric,__name__=metric,tag=2 value=4000 %d`, 1000000000),
		fmt.Sprintf(`metric,__name__=metric,tag=3 value=2 %d`, 2000000000),
		fmt.Sprintf(`metric,__name__=metric,tag=4 value=5000 %d`, 3000000000),
		fmt.Sprintf(`metric,__name__=metric,tag=5 value=3 %d`, 4000000000),
		fmt.Sprintf(`metric,__name__=metric,tag=6 value=6000 %d`, 5000000000),
		fmt.Sprintf(`metric,__name__=metric,tag=7 value=4 %d`, 6000000000),
		fmt.Sprintf(`metric,__name__=metric,tag=8 value=7000 %d`, 7000000000),
		fmt.Sprintf(`metric,__name__=metric,tag=9 value=5 %d`, 8000000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "instant query:  metric > time()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:10Z"}},
			command: `metric > time()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric","tag":"2"},"value":[10,"4000"]},{"metric":{"__name__":"metric","tag":"4"},"value":[10,"5000"]},{"metric":{"__name__":"metric","tag":"6"},"value":[10,"6000"]},{"metric":{"__name__":"metric","tag":"8"},"value":[10,"7000"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  metric > time() < time()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:10Z"}},
			command: `metric > time() < time()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  time() >= time() < metric",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:10Z"}},
			command: `time() >= (time() < metric)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query:  time() > metric - time()",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:00:10Z"}},
			command: `time() > metric - time()`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"tag":"1"},"value":[10,"1"]},{"metric":{"tag":"3"},"value":[10,"0"]},{"metric":{"tag":"5"},"value":[10,"-1"]},{"metric":{"tag":"7"},"value":[10,"-2"]},{"metric":{"tag":"9"},"value":[10,"-3"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServerPromQuerySubQueryPredictLinear(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric,__name__=metric value=14 %d`, 1704067200000000000),
		fmt.Sprintf(`metric,__name__=metric value=15 %d`, 1704067201000000000),
		fmt.Sprintf(`metric,__name__=metric value=64 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,__name__=metric value=71 %d`, 1704067203000000000),
		fmt.Sprintf(`metric,__name__=metric value=15 %d`, 1704067204000000000),
		fmt.Sprintf(`metric,__name__=metric value=51 %d`, 1704067205000000000),
		fmt.Sprintf(`metric,__name__=metric value=52 %d`, 1704067206000000000),
		fmt.Sprintf(`metric,__name__=metric value=15 %d`, 1704067207000000000),
		fmt.Sprintf(`metric,__name__=metric value=65 %d`, 1704067208000000000),
		fmt.Sprintf(`metric,__name__=metric value=11 %d`, 1704067209000000000),
		fmt.Sprintf(`metric,__name__=metric value=13 %d`, 1704067210000000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "instant query: predict_linear(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `predict_linear(metric[8s:1s], 10)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"77.55555555555554"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: deriv(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `deriv(metric[8s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"2.6666666666666665"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: predict_linear(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `predict_linear((metric / (metric - 14))[8s:1s], 10)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data: predict_linear(subquery)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `predict_linear(((metric - 14) / (metric - 14))[8s:1s], 10)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_ScalarExpression(t *testing.T) {
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	test := NewTest("db0", "autogen")

	test.addQueries([]*Query{
		{
			name:    "instant query: 1 + 1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"4"}},
			command: `1 + 1`,
			exp:     `{"status":"success","data":{"resultType":"scalar","result":[4,"2"]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServerPromQuerySubQueryAbsentOverTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	writes := []string{
		fmt.Sprintf(`metric,__name__=metric value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric,__name__=metric value=1 %d`, 1704067201000000000),
		fmt.Sprintf(`metric,__name__=metric value=1 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,__name__=metric value=1 %d`, 1704067211000000000),
	}
	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "range query:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1704067180"}, "end": []string{"1704067220"}, "step": []string{"10s"}},
			command: `absent_over_time(metric[10s:2s])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1704067180,"1"],[1704067190,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		{
			name:    "range query:  present_over_time",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1704067180"}, "end": []string{"1704067220"}, "step": []string{"10s"}},
			command: `present_over_time(metric[10s:2s])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1704067200,"1"],[1704067210,"1"],[1704067220,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		{
			name:    "instant query with odd data:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:02Z"}},
			command: `absent_over_time(((metric-1) / (metric-1))[2s:1s])[2s:1s]`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query with odd data:  absent_over_time",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:02Z"}},
			command: `absent_over_time(((metric) / (metric-1))[2s:1s])[2s:1s]`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}
func TestServer_PromQuery_Subquery_LastAndQuantile(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric,__name__=metric value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric,__name__=metric value=2 %d`, 1704067201000000000),
		fmt.Sprintf(`metric,__name__=metric value=3 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,__name__=metric value=4 %d`, 1704067203000000000),
		fmt.Sprintf(`metric,__name__=metric value=7 %d`, 1704067204000000000),
		fmt.Sprintf(`metric,__name__=metric value=10 %d`, 1704067205000000000),
		fmt.Sprintf(`metric,__name__=metric value=14 %d`, 1704067206000000000),
		fmt.Sprintf(`metric,__name__=metric value=22 %d`, 1704067207000000000),
		fmt.Sprintf(`metric,__name__=metric value=25 %d`, 1704067208000000000),
	}

	test := NewTest("db0", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instant query:  last_over_time(subquery)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:00Z"}},
			command: `last_over_time(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067200,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  last_over_time(subquery)2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:30Z"}},
			command: `last_over_time(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067230,"25"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  last_over_time(subquery)3",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `last_over_time(metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"25"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  last_over_time(subquery)4",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `last_over_time(metric[10s:3s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"14"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  last_over_time(subquery)5",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:06:30Z"}},
			command: `last_over_time(metric[10s:3s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  quantile_over_time(subquery)1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:08Z"}},
			command: `quantile_over_time(1,metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067208,"25"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  quantile_over_time(subquery)2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `quantile_over_time(0,metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  quantile_over_time(subquery)3",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `quantile_over_time(1.5,metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"+Inf"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  quantile_over_time(subquery)4",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `quantile_over_time(-1.5,metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"-Inf"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  quantile_over_time(subquery)5",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:00:30Z"}},
			command: `quantile_over_time(0.5,metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067230,"25"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  quantile_over_time(subquery)6",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"2024-01-01T00:05:30Z"}},
			command: `quantile_over_time(0.5,metric[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Subquery_ChangesAndResetsAndHoltWinters(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1 value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=1 %d`, 1704067201000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=3 %d`, 1704067202000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=4 %d`, 1704067203000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=4 %d`, 1704067204000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=8 %d`, 1704067205000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=14 %d`, 1704067206000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=22 %d`, 1704067207000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=25 %d`, 1704067208000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=21 %d`, 1704067209000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=21 %d`, 1704067210000000000),

		fmt.Sprintf(`metric2,__name__=metric2 value=7 %d`, 1704067200000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=8 %d`, 1704067201000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=6 %d`, 1704067202000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=4 %d`, 1704067203000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=4 %d`, 1704067204000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=2 %d`, 1704067205000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=10 %d`, 1704067206000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=12 %d`, 1704067207000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=14 %d`, 1704067208000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=7 %d`, 1704067209000000000),
		fmt.Sprintf(`metric2,__name__=metric2 value=8 %d`, 1704067210000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instant query:  changes(subquery)1",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:05Z"}},
			command: `changes(metric1[5s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067205,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  changes(subquery)2",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:35Z"}},
			command: `changes(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067235,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  changes(subquery)3",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:05:35Z"}},
			command: `changes(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  changes(subquery)4",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `changes((metric1/(metric1-4))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"7"]}]}}`,
			path:    "/api/v1/query",
		}, {
			name:    "instant query:  resets(subquery)1",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:05Z"}},
			command: `resets(metric1[5s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067205,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  resets(subquery)2",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:35Z"}},
			command: `resets(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067235,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  resets(subquery)3",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:05:35Z"}},
			command: `resets(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  resets(subquery)4",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `resets(metric1[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  resets(subquery)5",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `resets((metric1/(metric1-4))[10s:1s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"5"]}]}}`,
			path:    "/api/v1/query",
		}, {
			name:    "instant query:  holtWinters(subquery)1",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `holt_winters(metric1[10s:1s],0.5,0.5)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"24.841201782226562"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  holtWinters(subquery)2",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `holt_winters(((metric1-1)/(metric1-22))[10s:1s],0.9,0.2)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  holtWinters(subquery)3",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `holt_winters(((metric1-1)/(metric1-1))[10s:1s],0.9,0.2)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1704067210,"NaN"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  holtWinters(subquery)4",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `holt_winters(metric1[10s:1s],1.1,0.8)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_BinOp_OR_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1,node=a value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric1,__name__=metric1,node=a value=1 %d`, 1704067201000000000),

		fmt.Sprintf(`metric1,__name__=metric1,node=a value=7 %d`, 1704067208000000000),
		fmt.Sprintf(`metric1,__name__=metric1,node=a value=8 %d`, 1704067209000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "metric1{node=\"not_exist\"} < 3 or metric1 > 3",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1704067210"}},
			command: `metric1{node="not_exist"} < 3 or metric1 > 3`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric1","node":"a"},"value":[1704067210,"8"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "metric1 > 3 or metric1{node=\"not_exist\"} < 3",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1704067210"}},
			command: `metric1 > 3 or metric1{node="not_exist"} < 3`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric1","node":"a"},"value":[1704067210,"8"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_BinOp_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1,a=a value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric1,__name__=metric1,a=a value=1 %d`, 1704067201000000000),

		fmt.Sprintf(`metric2,__name__=metric2,b=b value=7 %d`, 1704067208000000000),
		fmt.Sprintf(`metric2,__name__=metric2,b=b value=8 %d`, 1704067209000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "range query:  metric1 offset 8s or metric2 offset 1s",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"1704067208"}, "end": []string{"1704067209"}, "step": []string{"1s"}, "lookback-delta": []string{"0s"}},
			command: `metric1 offset 8s or metric2 offset 1s`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric1","a":"a"},"values":[[1704067208,"1"],[1704067209,"1"]]},{"metric":{"__name__":"metric2","b":"b"},"values":[[1704067209,"7"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		{
			name:    "range query:  metric1 offset 8s or metric2 offset 1s",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1704067209"}, "lookback-delta": []string{"0s"}},
			command: `metric1 offset 8s or metric2 offset 1s`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric1","a":"a"},"value":[1704067209,"1"]},{"metric":{"__name__":"metric2","b":"b"},"value":[1704067209,"7"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Selector_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1,a=abb value=1 %d`, 1704067200000000000),
		fmt.Sprintf(`metric1,__name__=metric1,aa=bb value=2 %d`, 1704067200000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instant query:  metric1",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1704067200"}},
			command: `metric1`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric1","a":"abb"},"value":[1704067200,"1"]},{"metric":{"__name__":"metric1","aa":"bb"},"value":[1704067200,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  metric1{b=\"b\"}",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1704067200"}},
			command: `metric1{b!="b"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric1","a":"abb"},"value":[1704067200,"1"]},{"metric":{"__name__":"metric1","aa":"bb"},"value":[1704067200,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query:  metric1{b=\"b\", a=\"abb\"}",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1704067200"}},
			command: `metric1{foo!~"bar", a="abb", x!="y", z="", group!=""}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric1","a":"abb"},"value":[1704067200,"1"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_SortFunc(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric,rank=1 value=32 %d`, 1704067200000000000),
		fmt.Sprintf(`metric,rank=2 value=256 %d`, 1704067201000000000),
		fmt.Sprintf(`metric,rank=3 value=1024 %d`, 1704067202000000000),
		fmt.Sprintf(`metric,rank=4 value=1 %d`, 1704067203000000000),
		fmt.Sprintf(`metric,rank=5 value=64 %d`, 1704067204000000000),
		fmt.Sprintf(`metric,rank=6 value=512 %d`, 1704067205000000000),
		fmt.Sprintf(`metric,rank=7 value=128 %d`, 1704067206000000000),
		fmt.Sprintf(`metric,rank=8 value=8 %d`, 1704067207000000000),
		fmt.Sprintf(`metric,rank=9 value=2 %d`, 1704067208000000000),
		fmt.Sprintf(`metric,rank=10 value=4 %d`, 1704067209000000000),
		fmt.Sprintf(`metric,rank=11 value=16 %d`, 1704067210000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "instant query: sort(metric)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `sort(metric)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"rank":"4"},"value":[1704067210,"1"]},{"metric":{"rank":"9"},"value":[1704067210,"2"]},{"metric":{"rank":"10"},"value":[1704067210,"4"]},{"metric":{"rank":"8"},"value":[1704067210,"8"]},{"metric":{"rank":"11"},"value":[1704067210,"16"]},{"metric":{"rank":"1"},"value":[1704067210,"32"]},{"metric":{"rank":"5"},"value":[1704067210,"64"]},{"metric":{"rank":"7"},"value":[1704067210,"128"]},{"metric":{"rank":"2"},"value":[1704067210,"256"]},{"metric":{"rank":"6"},"value":[1704067210,"512"]},{"metric":{"rank":"3"},"value":[1704067210,"1024"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: sort_desc(metric)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `sort_desc(metric)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"rank":"3"},"value":[1704067210,"1024"]},{"metric":{"rank":"6"},"value":[1704067210,"512"]},{"metric":{"rank":"2"},"value":[1704067210,"256"]},{"metric":{"rank":"7"},"value":[1704067210,"128"]},{"metric":{"rank":"5"},"value":[1704067210,"64"]},{"metric":{"rank":"1"},"value":[1704067210,"32"]},{"metric":{"rank":"11"},"value":[1704067210,"16"]},{"metric":{"rank":"8"},"value":[1704067210,"8"]},{"metric":{"rank":"10"},"value":[1704067210,"4"]},{"metric":{"rank":"9"},"value":[1704067210,"2"]},{"metric":{"rank":"4"},"value":[1704067210,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    `instant query: sort_by_label(metric, "rank")`,
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `sort_by_label(metric, "rank")`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"rank":"1"},"value":[1704067210,"32"]},{"metric":{"rank":"2"},"value":[1704067210,"256"]},{"metric":{"rank":"3"},"value":[1704067210,"1024"]},{"metric":{"rank":"4"},"value":[1704067210,"1"]},{"metric":{"rank":"5"},"value":[1704067210,"64"]},{"metric":{"rank":"6"},"value":[1704067210,"512"]},{"metric":{"rank":"7"},"value":[1704067210,"128"]},{"metric":{"rank":"8"},"value":[1704067210,"8"]},{"metric":{"rank":"9"},"value":[1704067210,"2"]},{"metric":{"rank":"10"},"value":[1704067210,"4"]},{"metric":{"rank":"11"},"value":[1704067210,"16"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    `instant query: sort_by_label_desc(metric, "rank")`,
			params:  url.Values{"db": []string{"prom"}, "time": []string{"2024-01-01T00:00:10Z"}},
			command: `sort_by_label_desc(metric, "rank")`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"rank":"11"},"value":[1704067210,"16"]},{"metric":{"rank":"10"},"value":[1704067210,"4"]},{"metric":{"rank":"9"},"value":[1704067210,"2"]},{"metric":{"rank":"8"},"value":[1704067210,"8"]},{"metric":{"rank":"7"},"value":[1704067210,"128"]},{"metric":{"rank":"6"},"value":[1704067210,"512"]},{"metric":{"rank":"5"},"value":[1704067210,"64"]},{"metric":{"rank":"4"},"value":[1704067210,"1"]},{"metric":{"rank":"3"},"value":[1704067210,"1024"]},{"metric":{"rank":"2"},"value":[1704067210,"256"]},{"metric":{"rank":"1"},"value":[1704067210,"32"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_IntervalIndex_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-batch,deviceIp=127.0.0.1 value=0 %d`, 1718867655000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-batch,deviceIp=127.0.0.1 value=0 %d`, 1718867715000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-batch,deviceIp=127.0.0.1 value=0 %d`, 1718867775000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-batch,deviceIp=127.0.0.1 value=0 %d`, 1718867835000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-bpm,deviceIp=127.0.0.1 value=2 %d`, 1718868375000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-bpm,deviceIp=127.0.0.1 value=2 %d`, 1718868435000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-bpm,deviceIp=127.0.0.1 value=2 %d`, 1718868495000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-bpm,deviceIp=127.0.0.1 value=2 %d`, 1718868555000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-model,deviceIp=127.0.0.1 value=2 %d`, 1718868375000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-model,deviceIp=127.0.0.1 value=2 %d`, 1718868435000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-model,deviceIp=127.0.0.1 value=2 %d`, 1718868495000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-model,deviceIp=127.0.0.1 value=2 %d`, 1718868555000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-schedule-job,deviceIp=127.0.0.1 value=1 %d`, 1718867295000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-schedule-job,deviceIp=127.0.0.1 value=1 %d`, 1718867355000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-schedule-job,deviceIp=127.0.0.1 value=1 %d`, 1718867415000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-schedule-job,deviceIp=127.0.0.1 value=1 %d`, 1718867475000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-service,deviceIp=127.0.0.1 value=30 %d`, 1718868375000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-service,deviceIp=127.0.0.1 value=30 %d`, 1718868435000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-service,deviceIp=127.0.0.1 value=30 %d`, 1718868495000000000),
		fmt.Sprintf(`metric1,__name__=metric1,appName=adc-service,deviceIp=127.0.0.1 value=30 %d`, 1718868555000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "range query: metric1{deviceIp=xxx}",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"1718866800"}, "end": []string{"1718868600"}, "step": []string{"30m"}},
			command: `metric1{deviceIp="127.0.0.1"}`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric1","appName":"adc-bpm","deviceIp":"127.0.0.1"},"values":[[1718868600,"2"]]},{"metric":{"__name__":"metric1","appName":"adc-model","deviceIp":"127.0.0.1"},"values":[[1718868600,"2"]]},{"metric":{"__name__":"metric1","appName":"adc-service","deviceIp":"127.0.0.1"},"values":[[1718868600,"30"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		{
			name:    "instance query:  metric1{deviceIp=xxx}",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1718868600"}},
			command: `metric1{deviceIp="127.0.0.1"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric1","appName":"adc-bpm","deviceIp":"127.0.0.1"},"value":[1718868600,"2"]},{"metric":{"__name__":"metric1","appName":"adc-model","deviceIp":"127.0.0.1"},"value":[1718868600,"2"]},{"metric":{"__name__":"metric1","appName":"adc-service","deviceIp":"127.0.0.1"},"value":[1718868600,"30"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_PredictLinear_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1 value=0 %d`, 0),
		fmt.Sprintf(`metric1,__name__=metric1 value=10 %d`, 300000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=20 %d`, 600000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=30 %d`, 900000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=40 %d`, 1200000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=0 %d`, 1500000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=10 %d`, 1800000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=20 %d`, 2100000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=30 %d`, 2400000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=40 %d`, 2700000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=50 %d`, 3000000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query:  predict_linear(metric1[100m] @ 2000, 3600)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"0"}},
			command: `predict_linear(metric1[100m] @ 2000, 3600)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"25.357142857142854"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query:  predict_linear(metric1[100m] @ 2000, 3600)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1800"}},
			command: `predict_linear(metric1[100m] @ 2000, 3600)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1800,"31.785714285714285"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query:  predict_linear(metric1[100m] @ 3000, 3600)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"0"}},
			command: `predict_linear(metric1[100m] @ 3000, 3600)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"45.00000000000001"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query:  predict_linear(metric1[100m] @ 3000, 3600)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1800"}},
			command: `predict_linear(metric1[100m] @ 3000, 3600)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1800,"64.0909090909091"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_PredictLinearAndDeriv_ConstantYFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1 value=37 %d`, 0),
		fmt.Sprintf(`metric1,__name__=metric1 value=37 %d`, 100000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=37 %d`, 200000000000),
		fmt.Sprintf(`metric1,__name__=metric1 value=37 %d`, 300000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query: predict_linear(metric1[300s], 100)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"300"}},
			command: `predict_linear(metric1[300s], 100)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[300,"37"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: predict_linear(metric1[300s], 200)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"300"}},
			command: `predict_linear(metric1[300s], 200)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[300,"37"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: deriv(metric1[300s])",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"300"}},
			command: `deriv(metric1[300s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[300,"0"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "subquery: predict_linear(metric1[300s:100s])",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"300"}},
			command: `predict_linear(metric1[300s:100s], 233)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[300,"37"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "subquery: deriv(metric1[300s:100s])",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"300"}},
			command: `deriv(metric1[300s:100s])`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[300,"0"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_NameTag_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	startValue := 0
	valueNum := 10
	timeGap := 300 * 1000 * 1000 * 1000
	startTime := 0
	writes := make([]string, 0)
	for i := 0; i <= valueNum; i++ {
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=production value=%d %d`, startValue+i*10, startTime+i*timeGap))
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=production value=%d %d`, startValue+i*20, startTime+i*timeGap))
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=0,group=canary value=%d %d`, startValue+i*30, startTime+i*timeGap))
		writes = append(writes, fmt.Sprintf(`http_requests,__name__=http_requests,job=api-server,instance=1,group=canary value=%d %d`, startValue+i*40, startTime+i*timeGap))
		writes = append(writes, fmt.Sprintf(`foo,__name__=foo,job=api-server,instance=0,region=europe value=%d %d`, startValue+i*90, startTime+i*timeGap))
		writes = append(writes, fmt.Sprintf(`foo,__name__=foo,job=api-server value=%d %d`, startValue+i*100, startTime+i*timeGap))
	}

	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query: sum without (instance) - 1",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"3000"}},
			command: `sum without (instance) (http_requests{job="api-server"} or foo)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","job":"api-server"},"value":[3000,"700"]},{"metric":{"group":"production","job":"api-server"},"value":[3000,"300"]},{"metric":{"job":"api-server","region":"europe"},"value":[3000,"900"]},{"metric":{"job":"api-server"},"value":[3000,"1000"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: sum without (instance) - 2",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"3000"}},
			command: `sum without (instance) (http_requests{job="api-server"})`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","job":"api-server"},"value":[3000,"700"]},{"metric":{"group":"production","job":"api-server"},"value":[3000,"300"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "range query: sum without (instance) - 3",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"0"}, "end": []string{"3000"}, "step": []string{"3000"}},
			command: `sum without (instance) (http_requests{job="api-server"} or foo)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"group":"canary","job":"api-server"},"values":[[0,"0"],[3000,"700"]]},{"metric":{"group":"production","job":"api-server"},"values":[[0,"0"],[3000,"300"]]},{"metric":{"job":"api-server"},"values":[[0,"0"],[3000,"1000"]]},{"metric":{"job":"api-server","region":"europe"},"values":[[0,"0"],[3000,"900"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		{
			name:    "range query: sum without (instance) - 4",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"0"}, "end": []string{"3000"}, "step": []string{"3000"}},
			command: `sum without (instance) (http_requests{job="api-server"})`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"group":"canary","job":"api-server"},"values":[[0,"0"],[3000,"700"]]},{"metric":{"group":"production","job":"api-server"},"values":[[0,"0"],[3000,"300"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Filter_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=container value=5 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8080,job=container value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=5 %d`, 1709258341955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:7070,job=container value=4 %d`, 1709258342955000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instant query:  up >= 5",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258342.955"}},
			command: `up >= 5`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258342.955,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: up >= ((5*3+5*2)/5)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258342.955"}},
			command: `up >= ((5*3+5*2)/5)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:9090","job":"container"},"value":[1709258342.955,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: up >= bool 5",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258342.955"}},
			command: `up >= bool 5`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:7070","job":"container"},"value":[1709258342.955,"0"]},{"metric":{"instance":"localhost:8080","job":"container"},"value":[1709258342.955,"0"]},{"metric":{"instance":"localhost:9090","job":"container"},"value":[1709258342.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instant query: count(up >= 5)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258342.955"}},
			command: `count(up >= 5)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1709258342.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "range query: count(up >= 5)",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"1709258312.955"}, "end": []string{"1709258342.955"}, "step": []string{"15s"}},
			command: `count(up >= 5)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258342.955,"1"]]}]}}`,
			path:    "/api/v1/query_range",
		},
		{
			name:    "range query: count(up >= 5) / count(up) * 100",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"1709258312.955"}, "end": []string{"1709258342.955"}, "step": []string{"15s"}},
			command: `count(up >= 5) / count(up) * 100`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1709258342.955,"33.33333333333333"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_Idelta_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1,path=/foo value=0 %d`, 0),
		fmt.Sprintf(`metric1,__name__=metric1,path=/foo value=50 %d`, 300000000000),
		fmt.Sprintf(`metric1,__name__=metric1,path=/foo value=100 %d`, 600000000000),
		fmt.Sprintf(`metric1,__name__=metric1,path=/foo value=150 %d`, 900000000000),
		fmt.Sprintf(`metric1,__name__=metric1,path=/bar value=0 %d`, 0),
		fmt.Sprintf(`metric1,__name__=metric1,path=/bar value=50 %d`, 300000000000),
		fmt.Sprintf(`metric1,__name__=metric1,path=/bar value=100 %d`, 600000000000),
		fmt.Sprintf(`metric1,__name__=metric1,path=/bar value=50 %d`, 900000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "range query: idelta(metric1[20m]) - 1",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"1140"}, "end": []string{"1260"}, "step": []string{"60s"}},
			command: `idelta(metric1[20m])`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"path":"/bar"},"values":[[1140,"-50"],[1200,"-50"],[1260,"-50"]]},{"metric":{"path":"/foo"},"values":[[1140,"50"],[1200,"50"],[1260,"50"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_DuplicateLabels_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`testmetric1,__name__=testmetric1,src=a,dst=b value=0 %d`, 0),
		fmt.Sprintf(`testmetric1,__name__=testmetric1,src=a,dst=b value=10 %d`, 300000000000),
		fmt.Sprintf(`testmetric2,__name__=testmetric2,src=a,dst=b value=1 %d`, 0),
		fmt.Sprintf(`testmetric2,__name__=testmetric2,src=a,dst=b value=11 %d`, 300000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query:  {__name__=~\"testmetric1|testmetric2\"}[5m]",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"300"}},
			command: `{__name__=~"testmetric1|testmetric2"}[5m]`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"testmetric1","dst":"b","src":"a"},"values":[[0,"0"],[300,"10"]]},{"metric":{"__name__":"testmetric2","dst":"b","src":"a"},"values":[[0,"1"],[300,"11"]]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query:  changes({__name__=~\"testmetric1|testmetric2\"}[5m])",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"300"}},
			command: `changes({__name__=~"testmetric1|testmetric2"}[5m])`,
			exp:     `unexpected status code: code=422, body={"status":"error","errorType":"execution","error":"populatePromSeries raise err: vector cannot contain metrics with the same labelset"}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			if query.exp != err.Error() {
				t.Error(query.Error(err))
			}
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_NestedCount_Bugfix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`metric1,__name__=metric1,id=1 value=1 %d`, 1000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=1 value=2 %d`, 2000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=1 value=3 %d`, 3000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=1 value=4 %d`, 4000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=1 value=5 %d`, 5000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=2 value=6 %d`, 1000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=2 value=7 %d`, 2000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=2 value=8 %d`, 3000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=3 value=9 %d`, 4000000000),
		fmt.Sprintf(`metric1,__name__=metric1,id=3 value=0 %d`, 5000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query: count(rate(metric1[5s]))",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"5"}},
			command: `count(rate(metric1[5s]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[5,"3"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			if query.exp != err.Error() {
				t.Error(query.Error(err))
			}
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_MultiShard_MultiMetric_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicy("autogen", 1, 6*time.Hour, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job1=prometheus value=2 %d`, 1709348312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job1=prometheus value=2 %d`, 1709438312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job2=prometheus value=3 %d`, 1709348312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job3=prometheus value=5 %d`, 1709438312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8086,job1=prometheus value=2 %d`, 1709348312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8086,job1=prometheus value=2 %d`, 1709438312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8086,job2=prometheus value=3 %d`, 1709348312955000000),
		fmt.Sprintf(`down,__name__=down,instance=localhost:8086,job3=prometheus value=5 %d`, 1709438312955000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query:  (sum_over_time(up[1d])) or (sum_over_time(down[1d]))",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709438312.955"}},
			command: `(sum_over_time(up[1d])) or (sum_over_time(down[1d]))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:8086","job1":"prometheus"},"value":[1709438312.955,"2"]},{"metric":{"instance":"localhost:8086","job3":"prometheus"},"value":[1709438312.955,"5"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			if query.exp != err.Error() {
				t.Error(query.Error(err))
			}
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_BinaryExpr_RemoveMetric_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job=prometheus value=1 %d`, 1709258312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job=prometheus value=2 %d`, 1709258327955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job=prometheus value=4 %d`, 1709258342955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job=prometheus value=6 %d`, 1709258357955000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query:  (avg_over_time(up[60s]) > 3) or (up > 3)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258357.955"}},
			command: `(avg_over_time(up[60s]) > 3) or (up > 3)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:8086","job":"prometheus"},"value":[1709258357.955,"3.25"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query:  (avg_over_time(up[60s]) > 10) or (up > 3)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258357.955"}},
			command: `(avg_over_time(up[60s]) > 10) or (up > 3)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:8086","job":"prometheus"},"value":[1709258357.955,"6"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query:  (avg_over_time(up[60s]) > 3) and (up > 3)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258357.955"}},
			command: `(avg_over_time(up[60s]) > 3) and (up > 3)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:8086","job":"prometheus"},"value":[1709258357.955,"3.25"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query:  (avg_over_time(up[60s]) > 10) and (up > 3)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709258357.955"}},
			command: `(avg_over_time(up[60s]) > 10) and (up > 3)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			if query.exp != err.Error() {
				t.Error(query.Error(err))
			}
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_PromQuery_AggrExpr_ByMetricLabel_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job1=prometheus value=1 %d`, 1709348312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:8086,job1=prometheus value=2 %d`, 1709438312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job2=prometheus value=3 %d`, 1709348312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job3=prometheus value=5 %d`, 1709438312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9092,job4=prometheus value=4 %d`, 1709348312955000000),
		fmt.Sprintf(`up,__name__=up,instance=localhost:9092,job5=prometheus value=6 %d`, 1709438312955000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "instance query: count(up) by(__name__)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709438312.955"}},
			command: `count(up) by(__name__)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1709438312.955,"3"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: count(up) by(__name__, instance)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709438312.955"}},
			command: `count(up) by(__name__, instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"localhost:8086"},"value":[1709438312.955,"1"]},{"metric":{"__name__":"up","instance":"localhost:9090"},"value":[1709438312.955,"1"]},{"metric":{"__name__":"up","instance":"localhost:9092"},"value":[1709438312.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: count(up) by(instance)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709438312.955"}},
			command: `count(up) by(instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:8086"},"value":[1709438312.955,"1"]},{"metric":{"instance":"localhost:9090"},"value":[1709438312.955,"1"]},{"metric":{"instance":"localhost:9092"},"value":[1709438312.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: count(up) without(__name__)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709438312.955"}},
			command: `count(up) without(__name__)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:8086","job1":"prometheus"},"value":[1709438312.955,"1"]},{"metric":{"instance":"localhost:9090","job3":"prometheus"},"value":[1709438312.955,"1"]},{"metric":{"instance":"localhost:9092","job5":"prometheus"},"value":[1709438312.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: count(up) without(__name__, instance)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709438312.955"}},
			command: `count(up) without(__name__, instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job1":"prometheus"},"value":[1709438312.955,"1"]},{"metric":{"job3":"prometheus"},"value":[1709438312.955,"1"]},{"metric":{"job5":"prometheus"},"value":[1709438312.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		{
			name:    "instance query: count(up) without(instance)",
			params:  url.Values{"db": []string{"prom"}, "time": []string{"1709438312.955"}},
			command: `count(up) without(instance)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job1":"prometheus"},"value":[1709438312.955,"1"]},{"metric":{"job3":"prometheus"},"value":[1709438312.955,"1"]},{"metric":{"job5":"prometheus"},"value":[1709438312.955,"1"]}]}}`,
			path:    "/api/v1/query",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			if query.exp != err.Error() {
				t.Error(query.Error(err))
			}
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Prom_Or_MultiPointOfAGroup_BugFix(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("prom", NewRetentionPolicySpec("autogen", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`up1,__name__=up1,job=prometheus value=1 %d`, 1709258330000000000),
		fmt.Sprintf(`up1,__name__=up1,job=prometheus value=2 %d`, 1709258930000000000),
		fmt.Sprintf(`up1,__name__=up1,job=prometheus value=3 %d`, 1709259530000000000),
		fmt.Sprintf(`up1,__name__=up1,job=prometheus value=4 %d`, 1709260130000000000),
		fmt.Sprintf(`up1,__name__=up1,job=prometheus value=5 %d`, 1709260730000000000),
		fmt.Sprintf(`up1,__name__=up1,job=prometheus value=6 %d`, 1709261330000000000),
	}
	test := NewTest("prom", "autogen")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		{
			name:    "range query",
			params:  url.Values{"db": []string{"prom"}, "start": []string{"1709258330"}, "end": []string{"1709261330"}, "step": []string{"10m"}},
			command: `up1 <= 3 or up1 > 3`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up1","job":"prometheus"},"values":[[1709258330,"1"],[1709258930,"2"],[1709259530,"3"],[1709260130,"4"],[1709260730,"5"],[1709261330,"6"]]}]}}`,
			path:    "/api/v1/query_range",
		},
	}...)
	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.ExecuteProm(s); err != nil {
			if query.exp != err.Error() {
				t.Error(query.Error(err))
			}
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}
