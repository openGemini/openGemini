package tests

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServer_PromQuery(t *testing.T) {
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
			name:    "instant query: top-sum group by",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1709258357.955"}},
			command: `topk(3, sum(up{job="container"}) by (job))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"container"},"value":[1709258357.955,"15"]}]}}`,
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
			name:    "range query: top-sum group by",
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1709258330"}, "end": []string{"1709259360"}, "step": []string{"1m"}},
			command: `topk(3, sum(up{job="container"}) by (job))`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"container"},"values":[[1709258390,"15"],[1709258450,"15"],[1709258510,"15"],[1709258570,"15"],[1709258630,"15"]]}]}}`,
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

func TestServer_Prom_Evaluations(t *testing.T) {
	files, err := filepath.Glob("./testdata/*.test")
	require.NoError(t, err)

	for _, fn := range files {
		t.Run(fn, func(t *testing.T) {
			runTestFile(t, fn)
		})
	}
}

func runTestFile(t *testing.T, fn string) {
	t.Skip()
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	err := NewPromTestFromFile(t, fn, "db0", "rp0", s)
	t.Error(err)
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[9223372036.854,"996"]},{"metric":{"job":"app-server"},"value":[9223372036.854,"2596"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[9223372036.854,"256"]},{"metric":{"job":"app-server"},"value":[9223372036.854,"256"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum + sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) + SUM(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[9223372036.854,"2000"]},{"metric":{"job":"app-server"},"value":[9223372036.854,"5200"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector + rate*const",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{job="api-server", group="canary"} + rate(http_requests{job="api-server"}[5m]) * 5 * 60`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"330"]},{"metric":{"group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"440"]}]}}`,
			path:    "/api/v1/query",
		},
		// start condition
		&Query{
			name:    "instant query vector AND vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} and http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"300"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"700"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND on(multi) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and on(instance, job) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND on(single) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and on(instance) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND ignore(single) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and ignoring(group) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) AND ignore(multi) vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) and ignoring(group, job) http_requests{instance="0", group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector OR vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} or http_requests{group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"300"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"700"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[9223372036.854,"800"]},{"metric":{"__name__":"http_requests","group":"production","instance":"0","job":"api-server"},"value":[9223372036.854,"100"]},{"metric":{"__name__":"http_requests","group":"production","instance":"0","job":"app-server"},"value":[9223372036.854,"500"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"api-server"},"value":[9223372036.854,"200"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"app-server"},"value":[9223372036.854,"600"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query (vector+1) OR vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `(http_requests{group="canary"} + 1) or http_requests{instance="1"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"401"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[9223372036.854,"801"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"api-server"},"value":[9223372036.854,"200"]},{"metric":{"__name__":"http_requests","group":"production","instance":"1","job":"app-server"},"value":[9223372036.854,"600"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:   "instant query (vector+1) OR on(single) (vector or vector or vector)",
			params: url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			// prom_src_cmd: command: `(http_requests{group="canary"} + 1) or on(instance) (http_requests or cpu_count or vector_matching_a)`,
			command: `(http_requests{group="canary"} + 1) or on(instance) (http_requests or vector_matching_a)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"401"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[9223372036.854,"801"]},{"metric":{"__name__":"vector_matching_a","l":"x"},"value":[9223372036.854,"10"]},{"metric":{"__name__":"vector_matching_a","l":"y"},"value":[9223372036.854,"20"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:   "instant query (vector+1) OR ignore(multi) (vector or vector or vector)",
			params: url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			// prom_src_cmd: command: `(http_requests{group="canary"} + 1) or ignoring(l, group, job) (http_requests or cpu_count or vector_matching_a)`,
			command: `(http_requests{group="canary"} + 1) or ignoring(l, group, job) (http_requests or vector_matching_a)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"api-server"},"value":[9223372036.854,"301"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"0","job":"app-server"},"value":[9223372036.854,"701"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"401"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[9223372036.854,"801"]},{"metric":{"__name__":"vector_matching_a","l":"x"},"value":[9223372036.854,"10"]},{"metric":{"__name__":"vector_matching_a","l":"y"},"value":[9223372036.854,"20"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector UNLESS vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} unless http_requests{instance="0"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[9223372036.854,"800"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[9223372036.854,"800"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"api-server"},"value":[9223372036.854,"400"]},{"metric":{"__name__":"http_requests","group":"canary","instance":"1","job":"app-server"},"value":[9223372036.854,"800"]}]}}`,
			path:    "/api/v1/query",
		},
		// end condition
		&Query{
			name:    "instant query vector / ignore vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} / ignoring(group) http_requests{group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0","job":"api-server"},"value":[9223372036.854,"3"]},{"metric":{"instance":"0","job":"app-server"},"value":[9223372036.854,"1.4"]},{"metric":{"instance":"1","job":"api-server"},"value":[9223372036.854,"2"]},{"metric":{"instance":"1","job":"app-server"},"value":[9223372036.854,"1.3333333333333333"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector / on(multi) ignore vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `http_requests{group="canary"} / on(instance,job) http_requests{group="production"}`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0","job":"api-server"},"value":[9223372036.854,"3"]},{"metric":{"instance":"0","job":"app-server"},"value":[9223372036.854,"1.4"]},{"metric":{"instance":"1","job":"api-server"},"value":[9223372036.854,"2"]},{"metric":{"instance":"1","job":"app-server"},"value":[9223372036.854,"1.3333333333333333"]}]}}`,
			path:    "/api/v1/query",
		},
		// start bool comparisons
		&Query{
			name:    "instant query sum == bool sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) == bool SUM(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[9223372036.854,"1"]},{"metric":{"job":"app-server"},"value":[9223372036.854,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum != bool sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:50:00Z"}},
			command: `SUM(http_requests) BY (job) != bool SUM(http_requests) BY (job)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api-server"},"value":[9223372036.854,"0"]},{"metric":{"job":"app-server"},"value":[9223372036.854,"0"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"0","job":"api-server"},"value":[9223372036.854,"0.0003333333333333333"]},{"metric":{"instance":"0","job":"app-server"},"value":[9223372036.854,"0.0003333333333333334"]},{"metric":{"instance":"1","job":"api-server"},"value":[9223372036.854,"0.0003333333333333334"]},{"metric":{"instance":"1","job":"app-server"},"value":[9223372036.854,"0.0003333333333333333"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[9223372036.854,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector on left vector 1",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_var * on (instance) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[9223372036.854,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector ignore left vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_var * ignoring (role) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[9223372036.854,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector ignore right vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_role * ignoring (role) group_right (role) node_var`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","role":"prometheus"},"value":[9223372036.854,"2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector ignore(multi) right vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu * ignoring (role, mode) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","mode":"idle","role":"prometheus"},"value":[9223372036.854,"3"]},{"metric":{"instance":"abc","job":"node","mode":"user","role":"prometheus"},"value":[9223372036.854,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector on left vector 2",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu * on (instance) group_left (role) node_role`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","mode":"idle","role":"prometheus"},"value":[9223372036.854,"3"]},{"metric":{"instance":"abc","job":"node","mode":"user","role":"prometheus"},"value":[9223372036.854,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector on left sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu / on (instance) group_left sum by (instance,job)(node_cpu)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"abc","job":"node","mode":"idle"},"value":[9223372036.854,"0.75"]},{"metric":{"instance":"abc","job":"node","mode":"user"},"value":[9223372036.854,"0.25"]},{"metric":{"instance":"def","job":"node","mode":"idle"},"value":[9223372036.854,"0.8"]},{"metric":{"instance":"def","job":"node","mode":"user"},"value":[9223372036.854,"0.2"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum on left sum",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu)`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"node","mode":"idle"},"value":[9223372036.854,"0.7857142857142857"]},{"metric":{"job":"node","mode":"user"},"value":[9223372036.854,"0.21428571428571427"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query sum(sum on left sum)",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `sum(sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu))`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[9223372036.854,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector > on left",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `node_cpu > on(job, instance) group_left(target) threshold`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"node_cpu","instance":"abc","job":"node","mode":"idle","target":"a@b.com"},"value":[9223372036.854,"3"]},{"metric":{"__name__":"node_cpu","instance":"abc","job":"node","mode":"user","target":"a@b.com"},"value":[9223372036.854,"1"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[9223372036.854,"5"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query ignore nil",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `metricA + ignoring() metricB`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"baz":"meh"},"value":[9223372036.854,"7"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query normal",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `metricA + metricB`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"baz":"meh"},"value":[9223372036.854,"7"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost"},"value":[9223372036.854,"1"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector > vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `test_total > test_smaller`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"test_total","instance":"localhost"},"value":[9223372036.854,"50"]}]}}`,
			path:    "/api/v1/query",
		},
		&Query{
			name:    "instant query vector < bool vector",
			params:  url.Values{"db": []string{"db0"}, "time": []string{"1970-01-01T00:05:00Z"}},
			command: `test_total < bool test_smaller`,
			exp:     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost"},"value":[9223372036.854,"0"]}]}}`,
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
			exp:     `{"status":"success","data":{"resultType":"scalar","result":[{"metric":{},"value":[3000,"3000"]}]}}`,
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

// histogram
func TestServer_PromQuery_Histogram(t *testing.T) {
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
			str := fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus,le=%s value=3 %d`, buckets[i], time)
			writes = append(writes, str)
			str = fmt.Sprintf(`up,__name__=up,instance=localhost:9090,job=prometheus2,le=%s value=1000 %d`, buckets[i], time)
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
			params:  url.Values{"db": []string{"db0"}, "start": []string{"1713768282.462"}, "end": []string{"1713768432.462"}, "step": []string{"30s"}},
			command: `histogram_quantile(0.9,up)`,
			exp:     `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"values":[[1713768282.462,"0.09000000000000001"],[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"],[1713768402.462,"0.09000000000000001"],[1713768432.462,"0.09000000000000001"]]},{"metric":{"instance":"localhost:9090","job":"prometheus2"},"values":[[1713768282.462,"0.09000000000000001"],[1713768312.462,"0.09000000000000001"],[1713768342.462,"0.09000000000000001"],[1713768372.462,"0.09000000000000001"],[1713768402.462,"0.09000000000000001"],[1713768432.462,"0.09000000000000001"]]}]}}`,
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
