/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"container/heap"
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

const (
	BALANCED_DIFF = 5
)

type SparseIndexScanTransform struct {
	BaseProcessor

	span         *tracing.Span
	indexSpan    *tracing.Span
	allocateSpan *tracing.Span
	buildSpan    *tracing.Span

	input            *ChunkPort
	output           *ChunkPort
	chunkBuilder     *ChunkBuilder
	executorBuilder  *ExecutorBuilder
	pipelineExecutor *PipelineExecutor
	info             *IndexScanExtraInfo
	frags            ShardsFragments
	schema           hybridqp.Catalog
	node             hybridqp.QueryNode
	wg               sync.WaitGroup
	opt              query.ProcessorOptions
	ops              []hybridqp.ExprOptions
}

func NewSparseIndexScanTransform(inRowDataType hybridqp.RowDataType, node hybridqp.QueryNode, ops []hybridqp.ExprOptions, info *IndexScanExtraInfo, schema hybridqp.Catalog) *SparseIndexScanTransform {
	trans := &SparseIndexScanTransform{
		input:        NewChunkPort(inRowDataType),
		output:       NewChunkPort(inRowDataType),
		chunkBuilder: NewChunkBuilder(inRowDataType),
		ops:          ops,
		opt:          *schema.Options().(*query.ProcessorOptions),
		info:         info,
		schema:       schema,
		node:         node,
	}
	return trans
}

type SparseIndexScanTransformCreator struct {
}

func (c *SparseIndexScanTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p := NewSparseIndexScanTransform(plan.Children()[0].RowDataType(), plan, plan.RowExprOptions(), nil, nil)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalSparseIndexScan{}, &SparseIndexScanTransformCreator{})

func (trans *SparseIndexScanTransform) Name() string {
	return GetTypeName(trans)
}

func (trans *SparseIndexScanTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *SparseIndexScanTransform) Close() {
	trans.Once(func() {
		trans.output.Close()
		if trans.pipelineExecutor != nil {
			// When the indexScanTransform is closed, the pipelineExecutor must be closed at the same time.
			// Otherwise, which increases the memory usage.
			trans.pipelineExecutor.Crash()
		}
	})
}

func (trans *SparseIndexScanTransform) Release() error {
	if trans.frags == nil {
		return nil
	}
	trans.Once(func() {
		for _, shardFrags := range trans.frags {
			for _, fileFrags := range shardFrags.FileMarks {
				file := fileFrags.GetFile()
				file.Unref()
				file.UnrefFileReader()
			}
		}
	})
	return nil
}

func (trans *SparseIndexScanTransform) initSpan() {
	trans.span = trans.StartSpan("[Index] SparseIndexScanTransform", true)
	trans.indexSpan = trans.StartSpan("index_cost", false)
	trans.allocateSpan = trans.StartSpan("allocate_cost", false)
	trans.buildSpan = trans.StartSpan("build_executor_cost", false)
}

func (trans *SparseIndexScanTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.span, trans.indexSpan, trans.allocateSpan, trans.buildSpan)
		trans.output.Close()
	}()
	if err := trans.scanWithSparseIndex(ctx); err != nil {
		return err
	}
	return trans.WorkHelper(ctx)
}

func (trans *SparseIndexScanTransform) scanWithSparseIndex(ctx context.Context) error {
	if trans.info == nil {
		return errors.New("SparseIndexScanTransform get the info failed")
	}
	schema, ok := trans.schema.(*QuerySchema)
	if !ok {
		return errors.New("the schema is invalid for SparseIndexScanTransform")
	}

	tracing.StartPP(trans.indexSpan)
	fragments, err := trans.info.Store.ScanWithSparseIndex(ctx, trans.info.Req.Database, trans.info.Req.PtID, trans.info.Req.ShardIDs, schema)
	if err != nil {
		return err
	}
	frs, ok := fragments.(ShardsFragments)
	if !ok {
		return errors.New("the fragments is invalid for SparseIndexScanTransform")
	}
	tracing.EndPP(trans.indexSpan)

	trans.frags = frs
	if err = trans.buildExecutor(trans.node, frs); err != nil {
		return err
	}
	output := trans.pipelineExecutor.root.transform.GetOutputs()
	if len(output) > 1 {
		return errors.New("the output of SparseIndexScanTransform should be 1")
	}
	return nil
}

func (trans *SparseIndexScanTransform) WorkHelper(ctx context.Context) error {
	var pipError error
	output := trans.pipelineExecutor.root.transform.GetOutputs()
	if len(output) != 1 {
		return errors.New("the output should be 1")
	}
	trans.input.ConnectNoneCache(output[0])

	go func() {
		if pipError = trans.pipelineExecutor.ExecuteExecutor(ctx); pipError != nil {
			if trans.pipelineExecutor.Aborted() {
				return
			}
			return
		}
	}()
	trans.Running(ctx)
	trans.wg.Wait()
	return nil
}

func (trans *SparseIndexScanTransform) Running(ctx context.Context) {
	trans.wg.Add(1)
	defer trans.wg.Done()
	for {
		select {
		case c, ok := <-trans.input.State:
			if !ok {
				return
			}
			trans.output.State <- c
		case <-ctx.Done():
			return
		}
	}
}

func (trans *SparseIndexScanTransform) buildExecutor(input hybridqp.QueryNode, fragments ShardsFragments) error {
	parallelism := trans.schema.Options().GetMaxParallel()
	if parallelism <= 0 {
		parallelism = cpu.GetCpuNum()
	}

	tracing.StartPP(trans.allocateSpan)
	fragmentsGroups, err := DistributeFragments(fragments, parallelism)
	if err != nil {
		return err
	}
	tracing.EndPP(trans.allocateSpan)

	tracing.StartPP(trans.buildSpan)
	trans.executorBuilder = NewSparseIndexScanExecutorBuilder(fragmentsGroups, trans.info)
	trans.executorBuilder.Analyze(trans.span)
	p, pipeError := trans.executorBuilder.Build(input)
	if pipeError != nil {
		return pipeError
	}
	var ok bool
	trans.pipelineExecutor, ok = p.(*PipelineExecutor)
	if !ok {
		return errors.New("the PipelineExecutor is invalid for SparseIndexScanTransform")
	}
	tracing.EndPP(trans.buildSpan)

	return nil
}

func (trans *SparseIndexScanTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *SparseIndexScanTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *SparseIndexScanTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *SparseIndexScanTransform) GetInputNumber(_ Port) int {
	return 0
}

type ShardsFragments map[uint64]*FileFragments

func NewShardsFragments() ShardsFragments {
	return make(map[uint64]*FileFragments)
}

type FileFragments struct {
	FragmentCount int64
	FileMarks     map[string]FileFragment
}

func NewFileFragments() *FileFragments {
	return &FileFragments{
		FragmentCount: 0,
		FileMarks:     make(map[string]FileFragment),
	}
}

func (fms *FileFragments) AddFileFragment(filePath string, fm FileFragment, fc int64) {
	fms.FileMarks[filePath] = fm
	fms.FragmentCount += fc
}

type FileFragment interface {
	GetFile() immutable.TSSPFile
	GetFragmentRanges() fragment.FragmentRanges
	GetFragmentRange(int) *fragment.FragmentRange
	AppendFragmentRange(fragment.FragmentRanges)
	FragmentCount() int64
	CutTo(num int64) FileFragment
}

type FileFragmentImpl struct {
	dataFile       immutable.TSSPFile
	fragmentRanges fragment.FragmentRanges
	fragmentCount  int64
}

func NewFileFragment(f immutable.TSSPFile, fr fragment.FragmentRanges, fc int64) *FileFragmentImpl {
	return &FileFragmentImpl{
		dataFile:       f,
		fragmentRanges: fr,
		fragmentCount:  fc,
	}
}

func (f *FileFragmentImpl) GetFile() immutable.TSSPFile {
	return f.dataFile
}

func (f *FileFragmentImpl) GetFragmentRanges() fragment.FragmentRanges {
	return f.fragmentRanges
}

func (f *FileFragmentImpl) GetFragmentRange(i int) *fragment.FragmentRange {
	return f.fragmentRanges[i]
}

func (f *FileFragmentImpl) AppendFragmentRange(frs fragment.FragmentRanges) {
	f.fragmentRanges = append(f.fragmentRanges, frs...)
	for _, e := range frs {
		f.fragmentCount += int64(e.End - e.Start)
	}
}

func (f *FileFragmentImpl) FragmentCount() int64 {
	return f.fragmentCount
}

func (f *FileFragmentImpl) CutTo(num int64) FileFragment {
	if f.FragmentCount() < num {
		return f
	}
	m := &FileFragmentImpl{dataFile: f.dataFile, fragmentRanges: make([]*fragment.FragmentRange, 0, len(f.GetFragmentRanges()))}
	for i := 0; m.FragmentCount() < num && i < len(f.fragmentRanges); i++ {
		ra := f.GetFragmentRange(i)
		if int64(ra.End-ra.Start) > num {
			m.fragmentRanges = append(m.fragmentRanges, fragment.NewFragmentRange(ra.Start, ra.Start+uint32(num)))
			m.fragmentCount += num
			ra.Start = ra.Start + uint32(num)
			break
		} else {
			m.fragmentRanges = append(m.fragmentRanges, ra)
			m.fragmentCount += int64(ra.End - ra.Start)
			f.fragmentRanges = f.fragmentRanges[1:]
		}
	}
	f.fragmentCount -= m.fragmentCount
	return m
}

// DistributeFragments used for balanced fragment allocation of data files.
// Get a complete view and assign it directly. Considering the total number of fragments,
// only about the same number of fragments are placed in a group during initialization.
func DistributeFragments(frags ShardsFragments, parallel int) (*ShardsFragmentsGroups, error) {
	groups := NewShardsFragmentsGroups(parallel)
	for i := range groups.Items {
		groups.Items[i] = NewShardsFragmentsGroup(make(map[uint64]*FileFragments), 0)
	}
	heap.Init(groups)
	for shardId, infos := range frags {
		for _, info := range infos.FileMarks {
			group, ok := heap.Pop(groups).(*ShardsFragmentsGroup)
			if !ok {
				return nil, errors.New("invalid the ShardsFragmentsGroup type")
			}
			_, ok = group.frags[shardId]
			if !ok {
				group.frags[shardId] = &FileFragments{FileMarks: make(map[string]FileFragment)}
			}
			group.frags[shardId].FileMarks[info.GetFile().Path()] = info
			group.frags[shardId].FragmentCount += info.FragmentCount()
			group.fragmentCount += info.FragmentCount()
			heap.Push(groups, group)
		}
	}
	groups.Balance()
	return groups, nil
}

type ShardsFragmentsGroups struct {
	Items []*ShardsFragmentsGroup
}

func NewShardsFragmentsGroups(parallel int) *ShardsFragmentsGroups {
	return &ShardsFragmentsGroups{
		Items: make([]*ShardsFragmentsGroup, parallel),
	}
}

func (fgs *ShardsFragmentsGroups) MoveFrags(from, to int) {
	x, y := fgs.Items[from], fgs.Items[to]
	x.SearchAndMoveExistFile(y)
	x.MoveToFileNotExist(y)
}

func (fgs *ShardsFragmentsGroups) Len() int {
	return len(fgs.Items)
}

func (fgs *ShardsFragmentsGroups) Less(i, j int) bool {
	x, y := fgs.Items[i], fgs.Items[j]
	return x.fragmentCount < y.fragmentCount
}

func (fgs *ShardsFragmentsGroups) Swap(i, j int) {
	fgs.Items[i], fgs.Items[j] = fgs.Items[j], fgs.Items[i]
}

func (fgs *ShardsFragmentsGroups) Push(x interface{}) {
	fgs.Items = append(fgs.Items, x.(*ShardsFragmentsGroup))
}

func (fgs *ShardsFragmentsGroups) Pop() interface{} {
	old := fgs.Items
	n := len(old)
	item := old[n-1]
	fgs.Items = old[0 : n-1]
	return item
}

func (fgs *ShardsFragmentsGroups) Balance() {
	sort.Sort(fgs)
	if fgs.Items[fgs.Len()-1].fragmentCount-fgs.Items[0].fragmentCount < BALANCED_DIFF {
		return
	}
	from, to := fgs.Len()-1, 0
	for from > to {
		if fgs.Items[from].fragmentCount-fgs.Items[to].fragmentCount < BALANCED_DIFF {
			break
		}
		fgs.MoveFrags(from, to)
		from--
		to++
	}
	fgs.Balance()
}

type ShardsFragmentsGroup struct {
	frags         ShardsFragments
	fragmentCount int64
}

func NewShardsFragmentsGroup(frags ShardsFragments, fragmentCount int64) *ShardsFragmentsGroup {
	return &ShardsFragmentsGroup{
		frags:         frags,
		fragmentCount: fragmentCount,
	}
}

func (sfg *ShardsFragmentsGroup) GetFrags() ShardsFragments {
	return sfg.frags
}

func (sfg *ShardsFragmentsGroup) GetFragmentCount() int64 {
	return sfg.fragmentCount
}

func (sfg *ShardsFragmentsGroup) SearchAndMoveExistFile(des *ShardsFragmentsGroup) {
	for id := range des.frags {
		p, ok := sfg.frags[id]
		if !ok {
			continue
		}
		for file := range des.frags[id].FileMarks {
			_, ok := p.FileMarks[file]
			if !ok {
				continue
			}
			sfg.MoveTo(des, id, file)
			if sfg.fragmentCount-des.fragmentCount < BALANCED_DIFF {
				return
			}
		}
	}
}

func (sfg *ShardsFragmentsGroup) MoveToFileNotExist(des *ShardsFragmentsGroup) {
	if sfg.fragmentCount-des.fragmentCount < BALANCED_DIFF {
		return
	}

	for id, v := range sfg.frags {
		for file := range v.FileMarks {
			sfg.MoveTo(des, id, file)
			if (sfg.fragmentCount - des.fragmentCount) <= BALANCED_DIFF {
				return
			}
		}
	}
}

func (sfg *ShardsFragmentsGroup) MoveTo(des *ShardsFragmentsGroup, id uint64, filePath string) {
	var newPage FileFragment
	v := sfg.frags[id]
	gap := (sfg.fragmentCount - des.fragmentCount) / 2
	if v.FileMarks[filePath].FragmentCount() < gap {
		newPage = v.FileMarks[filePath]
		delete(v.FileMarks, filePath)
	} else {
		newPage = v.FileMarks[filePath].CutTo(gap)
	}

	v.FragmentCount -= newPage.FragmentCount()
	_, ok := des.frags[id]
	if !ok {
		des.frags[id] = &FileFragments{FileMarks: make(map[string]FileFragment)}
	}
	newPagePath := newPage.GetFile().Path()
	if _, ok := des.frags[id].FileMarks[newPagePath]; ok {
		des.frags[id].FileMarks[newPagePath].AppendFragmentRange(newPage.GetFragmentRanges())
	} else {
		des.frags[id].FileMarks[newPagePath] = newPage
	}
	des.frags[id].FragmentCount += newPage.FragmentCount()
	des.fragmentCount += newPage.FragmentCount()
	sfg.fragmentCount -= newPage.FragmentCount()
}
