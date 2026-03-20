// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consume

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/immutable/tsreader"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
	"github.com/openGemini/openGemini/services/pubsub"
	"go.uber.org/zap"
)

const (
	initProcessorTimeout = 5 * time.Second
	defaultRowLimit      = 1000
)

type ProcessorInterface interface {
	Init(topic *Topic) error
	Process(params FetchParams, onMsg func(msg protocol.Marshaler) bool) error
	Compile(sql string) (query.PreparedStatement, error)
	IteratorSize() int
	Release()
}

type Processor struct {
	engine    engine.Engine
	mc        metaclient.MetaClient
	Iterators []record.Iterator

	cancelFunc context.CancelFunc
	release    func()
}

func NewProcessor(mc metaclient.MetaClient, engine engine.Engine) *Processor {
	return &Processor{
		engine: engine,
		mc:     mc,
	}
}

func (p *Processor) Init(topic *Topic) error {
	stmt, err := p.Compile(topic.Query)
	if err != nil {
		return err
	}

	sources := stmt.Statement().Sources
	if len(sources) < 1 {
		return fmt.Errorf("no sources provided in the query")
	}

	mst, ok := sources[0].(*influxql.Measurement)
	if !ok {
		return fmt.Errorf("the first source is not a valid measurement")
	}
	mstOrigin, err := p.mc.Measurement(mst.Database, mst.RetentionPolicy, mst.Name)
	if err != nil {
		return fmt.Errorf("failed to query mst origin info for database=%s,"+
			" retention_policy=%s, name=%s: %w", mst.Database, mst.RetentionPolicy, mst.Name, err)
	}

	opts := stmt.ProcessorOptions()
	if opts == nil {
		return fmt.Errorf("failed to compile query: %s", topic.Query)
	}

	fields := stmt.Statement().Fields
	for _, field := range fields {
		var ref, ok = field.Expr.(*influxql.VarRef)
		if !ok {
			continue
		}
		if ref.Type == influxql.Tag {
			opts.TagAux = append(opts.TagAux, *ref)
		} else {
			opts.FieldAux = append(opts.FieldAux, *ref)
		}
	}

	influxql.WalkFunc(stmt.Statement().Condition, func(node influxql.Node) {
		ref, ok := node.(*influxql.VarRef)
		if ok {
			opts.Aux = append(opts.Aux, *ref)
		}
	})

	switch topic.Mode {
	case HistoryMode:
		ident := util.MeasurementIdent{
			DB:   mst.Database,
			RP:   mst.RetentionPolicy,
			Name: mstOrigin.Name,
		}
		p.Iterators, p.release = p.engine.CreateConsumeIterator(ident, p.engine.GetDBPtIds(mst.Database), opts)
		if len(p.Iterators) == 0 {
			return fmt.Errorf("consume failed to create consume iterator with query: %s", topic.Query)
		}
	case RealTimeMode:
		err = p.InitWalConsumeIterator(mst.Database, mst.RetentionPolicy, mstOrigin.Name, opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) Process(fetchParams FetchParams, onMsg func(msg protocol.Marshaler) bool) error {
	var (
		accumulated uint32
		timeoutHit  int32
	)

	if fetchParams.TimeoutMs > 0 {
		time.AfterFunc(time.Duration(fetchParams.TimeoutMs)*time.Millisecond, func() {
			atomic.StoreInt32(&timeoutHit, 1)
		})
	}

	for accumulated < fetchParams.MinRows {
		if atomic.LoadInt32(&timeoutHit) == 1 {
			break
		}

		for _, itr := range p.Iterators {
			rec, err := itr.Next()
			if err != nil {
				if err == io.EOF {
					continue
				}
				return err
			}
			if !onMsg(rec) {
				return nil
			}

			accumulated += uint32(rec.Rec.RowNums())
			if accumulated >= fetchParams.MinRows {
				return nil
			}
		}
	}
	return nil
}

func (p *Processor) Compile(sql string) (query.PreparedStatement, error) {
	q, err := influxql.ParseQuery(sql)
	if err != nil {
		return nil, err
	}

	mapper := &coordinator.ClusterShardMapper{
		MetaClient: p.mc,
		Logger:     logger.NewLogger(errno.ModuleMetaClient),
	}

	stmt, ok := q.Statements[0].(*influxql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("invalid select query")
	}

	return query.Prepare(stmt, mapper, query.SelectOptions{})
}

func (p *Processor) IteratorSize() int {
	return len(p.Iterators)
}

func (p *Processor) Release() {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
	for i := range p.Iterators {
		p.Iterators[i].Release()
	}
	p.Iterators = nil
	if p.release != nil {
		p.release()
	}
}

type FetchParams struct {
	MinRows   uint32
	MaxRows   uint32
	TimeoutMs uint32
}

type RecordIterCreator interface {
	CreateIterator(mst string) record.RecIterator
}

func (p *Processor) InitWalConsumeIterator(db, rp, mst string, opt *query.ProcessorOptions) error {
	opts, err := tsreader.NewConsumeOptions(opt, nil)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel

	walIterators := shelf.NewWalIterators(nil, mst, opts.Schema(), opts.TagSelected(), opts.FieldSelected(), defaultRowLimit, nil)
	p.Iterators = append(p.Iterators, walIterators)

	key := BuildPubSubMessageKey(db, rp)
	go func() {
		err = pubsub.DefaultCachedPubSub.Subscribe(key, ctx, func(msg pubsub.Message) {
			creator, ok := msg.(RecordIterCreator)
			if !ok {
				logger.GetLogger().Error("consume subscribe failed", zap.String("topic", key))
				return
			}
			iterator := creator.CreateIterator(mst)
			walIterators.AddIterator(iterator)
		})
		if err != nil {
			logger.GetLogger().Error("consume subscribe failed", zap.String("topic", key))
		}
	}()
	return nil
}

func BuildPubSubMessageKey(db, rp string) string {
	var b strings.Builder
	b.WriteString(db)
	b.WriteByte(':')
	b.WriteString(rp)
	return b.String()
}
