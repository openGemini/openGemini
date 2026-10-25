// Copyright 2026 openGemini Authors.
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

package executor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

type failedPointWriter struct {
	err error
}

func (w *failedPointWriter) RetryWritePointRows(_, _ string, _ []influx.Row) error {
	return w.err
}

func TestTargetTransform_WriteFailed(t *testing.T) {
	inRowDataType := buildSourceRowDataType()
	outRowDataType := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "written", Type: influxql.Integer})
	mst := &influxql.Measurement{Database: "db0", RetentionPolicy: "rp_not_exist", Name: "mst"}

	trans, err := executor.NewTargetTransform(inRowDataType, outRowDataType, nil,
		&query.ProcessorOptions{ChunkSize: 10}, &executor.QuerySchema{}, mst)
	require.NoError(t, err)

	source := executor.NewChunkPort(inRowDataType)
	require.NoError(t, executor.Connect(source, trans.GetInputs()[0]))

	writeErr := errors.New("retention policy not found: rp_not_exist")
	ctx := context.WithValue(context.Background(), executor.WRITER_CONTEXT, &failedPointWriter{err: writeErr})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// keep producing chunks so that the input still has data after every write fails
	go func() {
		chunk := buildSourceChunk1()
		for {
			select {
			case source.State <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- trans.Work(ctx)
	}()

	select {
	case err = <-done:
		require.ErrorIs(t, err, writeErr)
	case <-time.After(10 * time.Second):
		t.Fatal("TargetTransform does not return after the write failed")
	}
}
