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

package executor

import (
	"container/list"
	"fmt"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

type VertexId int

type Edge struct {
	To           VertexId
	Backward     bool
	InputNumber  int
	OutputNumber int
}

func NewEdge(to VertexId, backward bool, inputNumber int, outputNumber int) *Edge {
	return &Edge{
		To:           to,
		Backward:     backward,
		InputNumber:  inputNumber,
		OutputNumber: outputNumber,
	}
}

type Edges []*Edge

type Vertex struct {
	Processor   Processor
	Id          VertexId
	DirectEdges Edges
	BackEdges   Edges
}

type VertexWriter interface {
	Explain(*Vertex)
	Item(string, interface{})
	String() string
}

func NewVertex(id VertexId, processor Processor) *Vertex {
	return &Vertex{
		Processor: processor,
		Id:        id,
	}
}

func (vertex *Vertex) Degree() int {
	return len(vertex.DirectEdges) + len(vertex.BackEdges)
}

func (vertex *Vertex) InDegree() int {
	return len(vertex.BackEdges)
}

func (vertex *Vertex) OutDegree() int {
	return len(vertex.DirectEdges)
}

func (vertex *Vertex) Explain(w VertexWriter) {
	for _, pair := range vertex.Processor.Explain() {
		w.Item(pair.First, pair.Second)
	}
	w.Explain(vertex)
}

type Vertexs []*Vertex

type WalkFn func(vertex *Vertex, m map[VertexId]int) error

type DAG struct {
	ProcessorMap map[Processor]VertexId
	Vertexs      Vertexs
}

func NewDAG(processors Processors) *DAG {
	dag := &DAG{}

	dag.ProcessorMap = make(map[Processor]VertexId)
	dag.Vertexs = make(Vertexs, 0, len(processors))

	inputmap := make(map[uintptr]VertexId)
	outputmap := make(map[uintptr]VertexId)

	for i, p := range processors {
		dag.ProcessorMap[p] = VertexId(i)
		dag.Vertexs = append(dag.Vertexs, NewVertex(VertexId(i), p))

		for _, in := range p.GetInputs() {
			inputmap[in.ConnectionId()] = VertexId(i)
		}

		for _, out := range p.GetOutputs() {
			outputmap[out.ConnectionId()] = VertexId(i)
		}
	}

	var err error
	for i := range processors {
		err = dag.AddEdges(VertexId(i), inputmap, outputmap)
		if err != nil {
			logger.NewLogger(errno.ModuleQueryEngine).Error("NewDAG warn", zap.Error(err))
		}
	}

	return dag
}

func (dag *DAG) Processors() Processors {
	procs := make(Processors, 0, len(dag.ProcessorMap))
	for p := range dag.ProcessorMap {
		procs = append(procs, p)
	}

	return procs
}

func (dag *DAG) AddEdge(edges Edges, edge *Edge, from Processor, to Processor) (Edges, error) {
	toId, ok := dag.ProcessorMap[to]

	if !ok {
		return nil, errno.NewError(errno.ProcessorNotFound)
	}

	edge.To = toId
	edges = append(edges, edge)

	return edges, nil
}

func (dag *DAG) AddEdges(id VertexId, inputmap map[uintptr]VertexId, outputmap map[uintptr]VertexId) error {
	from := dag.Vertexs[id].Processor

	inputs := from.GetInputs()

	for i, v := range inputs {
		var to Processor
		toId, ok := outputmap[v.ConnectionId()]

		if ok {
			to = dag.Vertexs[toId].Processor
		} else {
			return errno.NewError(errno.MissOutputProcessor, i, from.Name())
		}

		o := to.GetOutputNumber(v)

		edge := NewEdge(toId, true, i, o)
		edges, err := dag.AddEdge(dag.Vertexs[id].BackEdges, edge, from, to)
		if err != nil {
			return err
		}

		dag.Vertexs[id].BackEdges = edges
	}

	outputs := from.GetOutputs()

	for o, v := range outputs {
		var to Processor
		toId, ok := inputmap[v.ConnectionId()]

		if ok {
			to = dag.Vertexs[toId].Processor
		} else {
			return errno.NewError(errno.MissInputProcessor, o, from.Name())
		}

		i := to.GetInputNumber(v)

		edge := NewEdge(toId, false, i, o)
		edges, err := dag.AddEdge(dag.Vertexs[id].DirectEdges, edge, from, to)
		if err != nil {
			return err
		}

		dag.Vertexs[id].DirectEdges = edges
	}

	return nil
}

func (dag *DAG) Size() int {
	return len(dag.Vertexs)
}

func (dag *DAG) Span() int {
	return 0
}

func (dag *DAG) Path() int {
	numEdge := 0
	for _, vertex := range dag.Vertexs {
		numEdge = numEdge + len(vertex.DirectEdges)
	}
	return numEdge
}

func (dag *DAG) SourceVertexs() []VertexId {
	vertexIds := make([]VertexId, 0, len(dag.Vertexs))

	for _, v := range dag.Vertexs {
		if len(v.BackEdges) == 0 && len(v.DirectEdges) != 0 {
			vertexIds = append(vertexIds, v.Id)
		}
	}

	return vertexIds
}

func (dag *DAG) SinkVertexs() []VertexId {
	vertexIds := make([]VertexId, 0, len(dag.Vertexs))

	for _, v := range dag.Vertexs {
		if len(v.DirectEdges) == 0 && len(v.BackEdges) != 0 {
			vertexIds = append(vertexIds, v.Id)
		}
	}

	return vertexIds
}

func (dag *DAG) OrphanVertexs() []VertexId {
	vertexIds := make([]VertexId, 0, len(dag.Vertexs))

	for _, v := range dag.Vertexs {
		if len(v.DirectEdges) == 0 && len(v.BackEdges) == 0 {
			vertexIds = append(vertexIds, v.Id)
		}
	}

	return vertexIds
}

func (dag *DAG) CyclicGraph() bool {
	vertexIds := dag.SourceVertexs()

	detectFn := func(vertex *Vertex, m map[VertexId]int) error {
		id, ok := m[vertex.Id]

		if ok {
			return errno.NewError(errno.CyclicVertex, id)
		}

		if !ok {
			m[vertex.Id] = 0
		}

		return nil
	}

	for _, id := range vertexIds {
		detectMap := make(map[VertexId]int)
		if err := dag.Walk(id, false, detectFn, detectMap); err != nil {
			return true
		}
	}

	return false
}

func (dag *DAG) Walk(id VertexId, backward bool, fn WalkFn, m map[VertexId]int) error {

	if fn == nil {
		fn = func(vertex *Vertex, m map[VertexId]int) error {
			return nil
		}
	}

	if err := dag.recursiveWalk(id, backward, fn, m); err != nil {
		return err
	}

	return nil
}

func (dag *DAG) recursiveWalk(id VertexId, backward bool, fn WalkFn, m map[VertexId]int) error {
	vertex := dag.Vertexs[id]
	if err := fn(vertex, m); err != nil {
		return err
	}

	var edges Edges

	if backward {
		edges = vertex.BackEdges
	} else {
		edges = vertex.DirectEdges
	}

	for _, edge := range edges {
		if err := dag.recursiveWalk(edge.To, backward, fn, m); err != nil {
			return err
		}
	}

	return nil
}

func (dag *DAG) Explain(fn func(*DAG, *strings.Builder) VertexWriter) VertexWriter {
	writer := fn(dag, &strings.Builder{})

	for _, id := range dag.SinkVertexs() {
		dag.Vertexs[id].Explain(writer)
	}

	return writer
}

type VertexWriterImpl struct {
	DAG     *DAG
	Builder *strings.Builder
	Values  *list.List
	Spacer  *Spacer
}

func NewVertexWriterImpl(dag *DAG, builder *strings.Builder) *VertexWriterImpl {
	return &VertexWriterImpl{
		DAG:     dag,
		Builder: builder,
		Values:  list.New(),
		Spacer:  NewSpacer(),
	}
}

func (w *VertexWriterImpl) String() string {
	return w.Builder.String()
}

func (w *VertexWriterImpl) Explain(vertex *Vertex) {
	w.Builder.WriteString(w.Spacer.String())
	w.Builder.WriteString(vertex.Processor.Name())

	j := 0

	e := w.Values.Front()
	for {
		if e == nil {
			break
		}

		if j == 0 {
			w.Builder.WriteString("(")
		} else {
			w.Builder.WriteString(",")
		}

		j++

		vp, ok := e.Value.(*ValuePair)
		if !ok {
			r := e
			e = e.Next()
			w.Values.Remove(r)
			logger.GetLogger().Warn("VertexWriterImpl Explain: element value isn't *ValuePair")
			continue
		}
		w.Builder.WriteString(vp.First)
		w.Builder.WriteString("=[")
		w.Builder.WriteString(fmt.Sprintf("%v", vp.Second))
		w.Builder.WriteString("]")

		r := e
		e = e.Next()
		w.Values.Remove(r)
	}

	if j > 0 {
		w.Builder.WriteString(")")
	}

	w.Builder.WriteString("\n")

	w.Spacer.Add(2)
	w.ExplainEdges(vertex.BackEdges)
	w.Spacer.Sub(2)
}

func (w *VertexWriterImpl) ExplainEdges(edges Edges) {
	for _, edge := range edges {
		w.DAG.Vertexs[edge.To].Explain(w)
	}
}

func (w *VertexWriterImpl) Item(term string, value interface{}) {
	vp := NewValuePair(term, value)
	w.Values.PushBack(vp)
}

type PlanFrame struct {
	plan   hybridqp.QueryNode
	indice int
}

func NewPlanFrame(plan hybridqp.QueryNode) *PlanFrame {
	return &PlanFrame{
		plan:   plan,
		indice: 0,
	}
}

type TransformCreator interface {
	Create(LogicalPlan, query.ProcessorOptions) (Processor, error)
}

func RegistryTransformCreator(plan LogicalPlan, creator TransformCreator) bool {
	factory := GetTransformFactoryInstance()
	name := plan.String()

	_, ok := factory.Find(name)

	factory.Add(name, creator)

	return ok
}

type TransformCreatorFactory struct {
	creators map[string]TransformCreator
}

func NewTransformCreatorFactory() *TransformCreatorFactory {
	return &TransformCreatorFactory{
		creators: make(map[string]TransformCreator),
	}
}

func (r *TransformCreatorFactory) Add(name string, creator TransformCreator) {
	r.creators[name] = creator
}

func (r *TransformCreatorFactory) Find(name string) (TransformCreator, bool) {
	creator, ok := r.creators[name]
	return creator, ok
}

var instance *TransformCreatorFactory
var once sync.Once

func GetTransformFactoryInstance() *TransformCreatorFactory {
	once.Do(func() {
		instance = NewTransformCreatorFactory()
	})

	return instance
}

type ReaderCreator interface {
	CreateReader(outSchema hybridqp.RowDataType, ops []hybridqp.ExprOptions, schema hybridqp.Catalog, frags ShardsFragments, database string, ptID uint32) (Processor, error)
}

func RegistryReaderCreator(plan LogicalPlan, creator ReaderCreator) bool {
	factory := GetReaderFactoryInstance()
	name := plan.String()

	_, ok := factory.Find(name)

	if ok {
		return ok
	}

	factory.Add(name, creator)
	return ok
}

type ReaderCreatorFactory struct {
	creators map[string]ReaderCreator
}

func NewReaderCreatorFactory() *ReaderCreatorFactory {
	return &ReaderCreatorFactory{
		creators: make(map[string]ReaderCreator),
	}
}

func (r *ReaderCreatorFactory) Add(name string, creator ReaderCreator) {
	r.creators[name] = creator
}

func (r *ReaderCreatorFactory) Find(name string) (ReaderCreator, bool) {
	creator, ok := r.creators[name]
	return creator, ok
}

var readerInstance *ReaderCreatorFactory
var readerOnce sync.Once

func GetReaderFactoryInstance() *ReaderCreatorFactory {
	readerOnce.Do(func() {
		readerInstance = NewReaderCreatorFactory()
	})

	return readerInstance
}

type DAGBuilder struct {
	opt        query.ProcessorOptions
	stack      Processors
	processors map[Processor]*PlanFrame
	err        error
}

func NewDAGBuilder(opt query.ProcessorOptions) *DAGBuilder {
	builder := &DAGBuilder{
		opt:        opt,
		stack:      nil,
		processors: make(map[Processor]*PlanFrame),
		err:        nil,
	}

	return builder
}

func (b *DAGBuilder) Build(plan hybridqp.QueryNode) (*DAG, error) {
	b.stack = nil
	b.processors = make(map[Processor]*PlanFrame)
	b.err = nil

	Walk(b, plan)

	if b.err != nil {
		return nil, b.err
	}

	return b.buildDAG()
}

func (b *DAGBuilder) buildDAG() (*DAG, error) {
	procs := make(Processors, 0, len(b.processors))

	for k := range b.processors {
		procs = append(procs, k)
	}

	dag := NewDAG(procs)

	if dag.CyclicGraph() {
		return nil, errno.NewError(errno.CyclicGraph)
	}

	return dag, nil
}

func (b *DAGBuilder) Visit(plan hybridqp.QueryNode) LogicalPlanVisitor {
	from, err := b.buildProcessor(plan)

	if err != nil {
		b.err = err
		return nil
	}

	b.processors[from] = NewPlanFrame(plan)

	if !b.stack.Empty() {
		to := b.stack.Peek()
		frame, ok := b.processors[to]

		if !ok {
			b.err = errno.NewError(errno.MissInputTransform, plan)
			return nil
		}

		_ = Connect(from.GetOutputs()[0], to.GetInputs()[frame.indice])
		// increase indice and check if it is the last child of to processor.
		// if true, pop the processor from stack.
		frame.indice++
		if frame.indice >= len(frame.plan.Children()) {
			b.stack.Pop()
		}
	}

	if len(plan.Children()) != 0 && !plan.Children()[0].Dummy() {
		b.stack.Push(from)
		return b
	}

	return nil
}

func (b *DAGBuilder) buildProcessor(plan hybridqp.QueryNode) (Processor, error) {
	if plan.Dummy() {
		return nil, nil
	}

	if creator, ok := GetTransformFactoryInstance().Find(plan.String()); ok {
		return creator.Create(plan.(LogicalPlan), b.opt)
	}
	return nil, errno.NewError(errno.UnsupportedLogicalPlan, plan.String())
}
