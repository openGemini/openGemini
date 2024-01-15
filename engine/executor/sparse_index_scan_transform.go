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
	"fmt"
	"sort"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	BALANCED_DIFF = 5
	Aborted       = "aborted"
)

type SparseIndexScanTransform struct {
	BaseProcessor

	aborted      bool
	hasRowCount  bool
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
	indexLogger      *logger.Logger
	outRowDataType   hybridqp.RowDataType
	frags            ShardsFragments
	rowCount         int64
	schema           hybridqp.Catalog
	node             hybridqp.QueryNode
	mutex            sync.RWMutex
	wg               sync.WaitGroup
	opt              query.ProcessorOptions
	ops              []hybridqp.ExprOptions
}

func NewSparseIndexScanTransform(inRowDataType hybridqp.RowDataType, node hybridqp.QueryNode, ops []hybridqp.ExprOptions, info *IndexScanExtraInfo, schema hybridqp.Catalog) *SparseIndexScanTransform {
	trans := &SparseIndexScanTransform{
		input:          NewChunkPort(inRowDataType),
		output:         NewChunkPort(inRowDataType),
		chunkBuilder:   NewChunkBuilder(inRowDataType),
		hasRowCount:    schema.HasRowCount(),
		outRowDataType: inRowDataType,
		ops:            ops,
		opt:            *schema.Options().(*query.ProcessorOptions),
		info:           info,
		schema:         schema,
		node:           node,
		indexLogger:    logger.NewLogger(errno.ModuleIndex),
	}
	return trans
}

type SparseIndexScanTransformCreator struct {
}

func (c *SparseIndexScanTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
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
		trans.mutex.Lock()
		trans.aborted = true
		if trans.pipelineExecutor != nil {
			// When the SparseIndexScanTransform is closed, the pipelineExecutor must be closed at the same time.
			// Otherwise, which increases the memory usage.
			trans.pipelineExecutor.Crash()
		}
		trans.mutex.Unlock()
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
				file.UnrefFileReader()
				file.Unref()
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
		if err.Error() == Aborted {
			return nil
		}
		return err
	}
	return trans.WorkHelper(ctx)
}

func (trans *SparseIndexScanTransform) scanWithSparseIndex(ctx context.Context) error {
	if trans.info == nil {
		return errors.New("SparseIndexScanTransform get the info failed")
	}
	if trans.aborted {
		return errors.New(Aborted)
	}
	schema, ok := trans.schema.(*QuerySchema)
	if !ok {
		return errors.New("the schema is invalid for SparseIndexScanTransform")
	}

	trans.mutex.RLock()
	defer trans.mutex.RUnlock()
	tracing.StartPP(trans.indexSpan)
	if trans.hasRowCount {
		rowCount, err := trans.info.Store.RowCount(trans.info.Req.Database, trans.info.Req.PtID, trans.info.Req.ShardIDs, schema)
		if err != nil {
			return err
		}
		trans.rowCount = rowCount
	} else {
		fragments, err := trans.info.Store.ScanWithSparseIndex(ctx, trans.info.Req.Database, trans.info.Req.PtID, trans.info.Req.ShardIDs, schema)
		if err != nil {
			return err
		}
		frs, ok := fragments.(ShardsFragments)
		if !ok {
			return errors.New("the fragments is invalid for SparseIndexScanTransform")
		}
		trans.frags = frs
	}
	tracing.EndPP(trans.indexSpan)

	if err := trans.buildExecutor(trans.node, trans.frags); err != nil {
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
	if trans.hasRowCount {
		trans.Counting(ctx)
	} else {
		trans.Running(ctx)
	}
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

func (trans *SparseIndexScanTransform) Counting(ctx context.Context) {
	trans.wg.Add(1)
	defer trans.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			c := NewChunkBuilder(trans.outRowDataType).NewChunk(influx.GetOriginMstName(trans.schema.Options().GetSourcesNames()[0]))
			c.AppendTagsAndIndex(ChunkTags{}, 0)
			c.AppendIntervalIndex(0)
			c.AppendTime(0)
			c.Column(0).AppendIntegerValue(trans.rowCount)
			c.Column(0).AppendNilsV2(true)
			trans.output.State <- c
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
	if trans.indexLogger.IsDebugLevel() {
		trans.indexLogger.Debug("SparseIndexScan meta infos", zap.String("db", trans.info.Req.Database),
			zap.Uint32("pt", trans.info.Req.PtID), zap.Uint64s("shardIds", trans.info.Req.ShardIDs))
		trans.indexLogger.Debug("SparseIndexScan index results", zap.String("shards fragments", fragments.String()))
	}

	var err error
	var fragmentsGroups *ShardsFragmentsGroups
	if trans.hasRowCount {
		fragmentsGroups = NewShardsFragmentsGroups(1)
		fragmentsGroups.Items[0] = NewShardsFragmentsGroup(NewShardsFragments(), 0)
	} else {
		fragmentsGroups, err = DistributeFragmentsV2(fragments, parallelism)
		if err != nil {
			trans.indexLogger.Error("SparseIndexScan allocates error", zap.Error(err))
			return err
		}
	}

	if trans.indexLogger.IsDebugLevel() {
		trans.indexLogger.Debug("SparseIndexScan allocates result", zap.String("groups fragments", fragmentsGroups.String()))
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

func (trans *SparseIndexScanTransform) IsSink() bool {
	// SparseIndexScanTransform will create new pipelineExecutor, it is sink node.
	return true
}

func (trans *SparseIndexScanTransform) Abort() {
	trans.mutex.Lock()
	defer trans.mutex.Unlock()
	if trans.aborted {
		return
	}
	trans.aborted = true
	if trans.pipelineExecutor != nil {
		trans.pipelineExecutor.Abort()
	}
}

type ShardsFragments map[uint64]*FileFragments

func NewShardsFragments() ShardsFragments {
	return make(map[uint64]*FileFragments)
}

func (sfs *ShardsFragments) String() string {
	var res string
	for shardId, shardFrags := range *sfs {
		res += fmt.Sprintf("shardId: %d\n", shardId)
		for file, frs := range shardFrags.FileMarks {
			res += fmt.Sprintf("file: %s\n", file)
			res += fmt.Sprintf("fragCount: %d\n", frs.FragmentCount())
			res += fmt.Sprintf("fragRanges: %s\n", frs.GetFragmentRanges().String())
		}
	}
	return res
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
	for m.FragmentCount() < num && len(f.fragmentRanges) > 0 {
		ra := f.GetFragmentRange(0)
		if need := num - m.FragmentCount(); int64(ra.End-ra.Start) > need {
			m.fragmentRanges = append(m.fragmentRanges, fragment.NewFragmentRange(ra.End-uint32(need), ra.End))
			m.fragmentCount += need
			ra.End = ra.End - uint32(need)
			break
		}
		m.fragmentRanges = append(m.fragmentRanges, ra)
		m.fragmentCount += int64(ra.End - ra.Start)
		f.fragmentRanges = f.fragmentRanges[1:]
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

// DistributeFragmentsV2 allocates fragments based on the parallel number so that
// the difference number of fragments between two groups is less than or equal to 1.
func DistributeFragmentsV2(frags ShardsFragments, parallel int) (*ShardsFragmentsGroups, error) {
	var fragTotalCount int
	var numFragForGroup int
	var files []*ShardFileFragment

	for shardId, shardFrag := range frags {
		fragTotalCount += int(shardFrag.FragmentCount)
		for _, file := range shardFrag.FileMarks {
			files = append(files, NewShardFileFragment(shardId, file))
		}
	}

	if fragTotalCount == 0 {
		fragmentsGroups := NewShardsFragmentsGroups(1)
		fragmentsGroups.Items[0] = NewShardsFragmentsGroup(NewShardsFragments(), 0)
		return fragmentsGroups, nil
	}

	if fragTotalCount < parallel {
		parallel = fragTotalCount
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].item.FragmentCount() < files[j].item.FragmentCount()
	})

	groups := NewShardsFragmentsGroups(parallel)
	numFragPerGroup, remainFragForGroup := fragTotalCount/parallel, fragTotalCount%parallel

	for i := 0; i < len(groups.Items); i++ {
		if remainFragForGroup > 0 && i < remainFragForGroup {
			numFragForGroup = numFragPerGroup + 1
		} else {
			numFragForGroup = numFragPerGroup
		}
		groups.Items[i] = NewShardsFragmentsGroup(NewShardsFragments(), 0)
		for groups.Items[i].GetFragmentCount() < int64(numFragForGroup) {
			if len(files) == 0 {
				return nil, fmt.Errorf("the number of files must be greater than 0 for DistributeFragmentsV2")
			}
			file := files[len(files)-1]
			groupFragCount, fileFragCount := groups.Items[i].GetFragmentCount(), file.item.FragmentCount()
			needFragCount := int64(numFragForGroup) - groupFragCount
			if groups.Items[i].frags[file.shardId] == nil {
				groups.Items[i].frags[file.shardId] = NewFileFragments()
			}
			if needFragCount >= fileFragCount {
				groups.Items[i].frags[file.shardId].FileMarks[file.item.GetFile().Path()] = file.item
				groups.Items[i].frags[file.shardId].FragmentCount += fileFragCount
				groups.Items[i].fragmentCount += fileFragCount
				files = files[:len(files)-1]
			} else {
				newFile := file.item.CutTo(needFragCount)
				groups.Items[i].frags[file.shardId].FileMarks[newFile.GetFile().Path()] = newFile
				groups.Items[i].frags[file.shardId].FragmentCount += needFragCount
				groups.Items[i].fragmentCount += needFragCount
			}
		}
	}
	return groups, nil
}

type ShardFileFragment struct {
	shardId uint64
	item    FileFragment
}

func NewShardFileFragment(shardId uint64, item FileFragment) *ShardFileFragment {
	return &ShardFileFragment{shardId: shardId, item: item}
}

type ShardsFragmentsGroups struct {
	Items       []*ShardsFragmentsGroup
	readerIndex int
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

func (fgs *ShardsFragmentsGroups) HasGroup() bool {
	return fgs.readerIndex < len(fgs.Items)
}

func (fgs *ShardsFragmentsGroups) PeekGroup() *ShardsFragmentsGroup {
	return fgs.Items[fgs.readerIndex]
}

func (fgs *ShardsFragmentsGroups) NextGroup() *ShardsFragmentsGroup {
	group := fgs.Items[fgs.readerIndex]
	fgs.readerIndex++
	return group
}

func (fgs *ShardsFragmentsGroups) String() string {
	var res string
	for i := range fgs.Items {
		res += fmt.Sprintf("group: %d; fragCount: %d\n", i, fgs.Items[i].GetFragmentCount())
		for k, v := range fgs.Items[i].frags {
			res += fmt.Sprintf("shardId: %d\n", k)
			for f, v1 := range v.FileMarks {
				res += fmt.Sprintf("file: %s\n", f)
				res += fmt.Sprintf("fragCount: %d\n", v1.FragmentCount())
				res += fmt.Sprintf("fragRanges: %s\n", v1.GetFragmentRanges().String())
			}
		}
	}
	return res
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
