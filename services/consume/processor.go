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
	"fmt"
	"io"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
)

const noContentSId = 0

type ProcessorInterface interface {
	Init(topic *Topic) error
	Process(onMsg func(msg protocol.Marshaler) bool) error
	Compile(sql string) (query.PreparedStatement, error)
	IteratorSize() int
	IteratorReset()
}

type Processor struct {
	engine engine.Engine
	mc     metaclient.MetaClient
	itrs   []record.Iterator
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

	p.itrs = p.engine.CreateConsumeIterator(mst.Database, mstOrigin.Name, opts)
	if len(p.itrs) == 0 {
		return fmt.Errorf("failed to create consume iterator with query: %s", topic.Query)
	}
	return nil
}

func (p *Processor) Process(onMsg func(msg protocol.Marshaler) bool) error {
	for _, itr := range p.itrs {
		isDataReady, err := p.process(itr, onMsg)
		if err != nil {
			return err
		}
		if isDataReady {
			return nil
		}
	}
	// If a connection sends multiple requests for the same topic, reset to ensure that data can be obtained.
	p.IteratorReset()
	return nil
}

func (p *Processor) process(itr record.Iterator, onMsg func(msg protocol.Marshaler) bool) (bool, error) {
	for {
		sid, rec, err := itr.Next()
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if sid == noContentSId {
			return false, nil
		}
		if onMsg(rec) {
			return true, nil
		}
	}
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
	return len(p.itrs)
}

func (p *Processor) IteratorReset() {
	for i := range p.itrs {
		p.itrs[i].Release()
	}
	p.itrs = nil
}
