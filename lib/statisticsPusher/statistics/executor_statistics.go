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

package statistics

type Statistics interface {
	BuildAccumulator()
	AssignName()
}

type BaseStatistics struct {
	derived Statistics
}

func (bs *BaseStatistics) Init(derived Statistics) {
	bs.derived = derived
	bs.derived.AssignName()
	bs.derived.BuildAccumulator()
}

type ExecutorStatistics struct {
	BaseStatistics
	Memory           Accumulator
	Goroutine        Accumulator
	ExecWaitTime     Accumulator
	ExecRunTime      Accumulator
	TransWaitTime    Accumulator
	TransRunTime     Accumulator
	DagEdge          Accumulator
	DagVertex        Accumulator
	ExecScheduled    Accumulator
	ExecTimeout      Accumulator
	ExecAbort        Accumulator
	ExecFailed       Accumulator
	TransFailed      Accumulator
	TransAbort       Accumulator
	TransFailedAbort Accumulator
	ColumnLength     Accumulator
	ColumnWidth      Accumulator
	SourceLength     Accumulator
	SourceWidth      Accumulator
	SinkLength       Accumulator
	SinkWidth        Accumulator
	SourceRows       Accumulator
	FilterRows       Accumulator
	AggRows          Accumulator
	MergeRows        Accumulator
	LimitRows        Accumulator
	FillRows         Accumulator
	MaterRows        Accumulator
	SinkRows         Accumulator
	accumulators     []NamedAccumulator
	tags             map[string]string
	name             string
}

var ExecutorStat = NewExecutorStatistics()

func NewExecutorStatistics() *ExecutorStatistics {
	stats := &ExecutorStatistics{
		accumulators: make([]NamedAccumulator, 0, 64),
	}
	stats.Init(stats)
	return stats
}

func (es *ExecutorStatistics) AssignName() {
	es.name = "executor"
}

func (es *ExecutorStatistics) BuildAccumulator() {
	es.Memory = NewNamedInt64Accumulator("memory")
	es.accumulators = append(es.accumulators, es.Memory.(NamedAccumulator))
	es.Goroutine = NewNamedInt64Accumulator("goroutine")
	es.accumulators = append(es.accumulators, es.Goroutine.(NamedAccumulator))
	es.ExecWaitTime = NewNamedInt64Accumulator("exec_wait_time")
	es.accumulators = append(es.accumulators, es.ExecWaitTime.(NamedAccumulator))
	es.ExecRunTime = NewNamedInt64Accumulator("exec_run_time")
	es.accumulators = append(es.accumulators, es.ExecRunTime.(NamedAccumulator))
	es.TransWaitTime = NewNamedInt64Accumulator("trans_wait_time")
	es.accumulators = append(es.accumulators, es.TransWaitTime.(NamedAccumulator))
	es.TransRunTime = NewNamedInt64Accumulator("trans_run_time")
	es.accumulators = append(es.accumulators, es.TransRunTime.(NamedAccumulator))
	es.DagEdge = NewNamedInt64Accumulator("dag_edge")
	es.accumulators = append(es.accumulators, es.DagEdge.(NamedAccumulator))
	es.DagVertex = NewNamedInt64Accumulator("dag_vertex")
	es.accumulators = append(es.accumulators, es.DagVertex.(NamedAccumulator))
	es.ExecScheduled = NewNamedInt64Accumulator("exec_scheduled")
	es.accumulators = append(es.accumulators, es.ExecScheduled.(NamedAccumulator))
	es.ExecTimeout = NewNamedInt64Accumulator("exec_timeout")
	es.accumulators = append(es.accumulators, es.ExecTimeout.(NamedAccumulator))
	es.ExecAbort = NewNamedInt64Accumulator("exec_abort")
	es.accumulators = append(es.accumulators, es.ExecAbort.(NamedAccumulator))
	es.ExecFailed = NewNamedInt64Accumulator("exec_failed")
	es.accumulators = append(es.accumulators, es.ExecFailed.(NamedAccumulator))
	es.TransFailed = NewNamedInt64Accumulator("trans_failed")
	es.accumulators = append(es.accumulators, es.TransFailed.(NamedAccumulator))
	es.TransAbort = NewNamedInt64Accumulator("trans_abort")
	es.accumulators = append(es.accumulators, es.TransAbort.(NamedAccumulator))
	es.TransFailedAbort = NewNamedInt64Accumulator("trans_failed_abort")
	es.accumulators = append(es.accumulators, es.TransFailedAbort.(NamedAccumulator))
	es.ColumnLength = NewNamedInt64Accumulator("column_length")
	es.accumulators = append(es.accumulators, es.ColumnLength.(NamedAccumulator))
	es.ColumnWidth = NewNamedInt64Accumulator("column_width")
	es.accumulators = append(es.accumulators, es.ColumnWidth.(NamedAccumulator))
	es.SourceLength = NewNamedInt64Accumulator("source_length")
	es.accumulators = append(es.accumulators, es.SourceLength.(NamedAccumulator))
	es.SourceWidth = NewNamedInt64Accumulator("source_width")
	es.accumulators = append(es.accumulators, es.SourceWidth.(NamedAccumulator))
	es.SinkLength = NewNamedInt64Accumulator("sink_length")
	es.accumulators = append(es.accumulators, es.SinkLength.(NamedAccumulator))
	es.SinkWidth = NewNamedInt64Accumulator("sink_width")
	es.accumulators = append(es.accumulators, es.SinkWidth.(NamedAccumulator))
	es.SourceRows = NewNamedInt64Accumulator("source_rows")
	es.accumulators = append(es.accumulators, es.SourceRows.(NamedAccumulator))
	es.FilterRows = NewNamedInt64Accumulator("filter_rows")
	es.accumulators = append(es.accumulators, es.FilterRows.(NamedAccumulator))
	es.AggRows = NewNamedInt64Accumulator("agg_rows")
	es.accumulators = append(es.accumulators, es.AggRows.(NamedAccumulator))
	es.MergeRows = NewNamedInt64Accumulator("merge_rows")
	es.accumulators = append(es.accumulators, es.MergeRows.(NamedAccumulator))
	es.LimitRows = NewNamedInt64Accumulator("limit_rows")
	es.accumulators = append(es.accumulators, es.LimitRows.(NamedAccumulator))
	es.FillRows = NewNamedInt64Accumulator("fill_rows")
	es.accumulators = append(es.accumulators, es.FillRows.(NamedAccumulator))
	es.MaterRows = NewNamedInt64Accumulator("materialized_rows")
	es.accumulators = append(es.accumulators, es.MaterRows.(NamedAccumulator))
	es.SinkRows = NewNamedInt64Accumulator("sink_rows")
	es.accumulators = append(es.accumulators, es.SinkRows.(NamedAccumulator))
}

func (es *ExecutorStatistics) Report() map[string]interface{} {
	var name string
	var value interface{}
	valueMap := make(map[string]interface{})
	for _, accumulator := range es.accumulators {
		name, value = accumulator.NamedSum()
		valueMap[name] = value
		name, value = accumulator.NamedCount()
		valueMap[name] = value
		name, value = accumulator.NamedLast()
		valueMap[name] = value
	}
	return valueMap
}

func InitExecutorStatistics(tags map[string]string) {
	ExecutorStat = NewExecutorStatistics()
	ExecutorStat.tags = tags
}

func CollectExecutorStatistics(buffer []byte) ([]byte, error) {
	valueMap := ExecutorStat.Report()
	buffer = AddPointToBuffer(ExecutorStat.name, ExecutorStat.tags, valueMap, buffer)
	return buffer, nil
}

func CreateExecutorWithShardKey(buffer []byte) ([]byte, error) {
	return nil, nil
}
