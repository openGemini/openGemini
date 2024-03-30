/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package opentelemetry

import (
	"io"
	"sync"

	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var otelContextPool sync.Pool
var otelContextPoolCh = make(chan *otelConext, cpu.GetCpuNum())

func GetOtelContext(r io.Reader) *otelConext {
	select {
	case octx := <-otelContextPoolCh:
		octx.reset()
		octx.br.Reset(r)
		return octx
	default:
		if v := otelContextPool.Get(); v != nil {
			octx := v.(*otelConext)
			octx.reset()
			octx.br.Reset(r)
			return octx
		}

		octx := &otelConext{
			ReqBuf: make([]byte, 0),

			rowsBuff: make(influx.Rows, 0, 20),
			logger:   logger.NewLogger(errno.ModuleUnknown),
		}
		octx.br.Reset(r)

		log := &otelLogger{
			logger: logger.NewLogger(errno.ModuleUnknown),
		}
		var err error
		octx.PtraceWriter = otel2influx.NewOtelTracesToLineProtocol(log)
		if err != nil {
			panic(err)
		}
		// TODO: support MetricsSchemaTelegrafPrometheusV1, MetricsSchemaTelegrafPrometheusV2 and more
		octx.PmetricWriter, err = otel2influx.NewOtelMetricsToLineProtocol(log, 1)
		if err != nil {
			panic(err)
		}
		octx.PlogWriter = otel2influx.NewOtelLogsToLineProtocol(log)
		return octx
	}
}

func PutOtelContext(octx *otelConext) {
	octx.reset()
	select {
	case otelContextPoolCh <- octx:
	default:
		otelContextPool.Put(octx)
	}
}
