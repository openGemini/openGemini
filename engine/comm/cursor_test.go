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

package comm

import (
	"errors"
	"io"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/require"
)

type MockIterator struct {
	init bool
	record.Iterator
	err error
}

func (m *MockIterator) Next() (*record.ConsumeRecord, error) {
	if !m.init {
		m.init = true
		return &record.ConsumeRecord{Rec: record.NewRecord(record.Schemas{{}}, false)}, nil
	}
	if m.err != nil {
		return nil, m.err
	}
	return nil, nil
}

func (m *MockIterator) Close() error {
	return nil
}

func (m *MockIterator) Release() {
}

type MockKeyCursor struct {
	init bool
	KeyCursor
	err error
}

func (m *MockKeyCursor) Next() (*record.Record, SeriesInfoIntf, error) {
	if !m.init {
		m.init = true
		return record.NewRecord(record.Schemas{{}}, false), nil, nil
	}
	if m.err != nil {
		return nil, nil, m.err
	}
	return nil, nil, nil
}

func (m *MockKeyCursor) Close() error {
	if m.err != nil {
		return m.err
	}
	return nil
}

func TestSourceIterator(t *testing.T) {
	opt := &query.ProcessorOptions{}
	itr := NewSourceIterator([]record.Iterator{&MockIterator{}, &MockIterator{err: io.EOF}}, opt)
	var res []*record.Record
	for {
		rec, _, err := itr.NextAggData()
		if err != nil {
			t.Fatal(err)
		}
		if rec == nil {
			break
		}
		res = append(res, rec)
	}
	require.Equal(t, len(res), 2)
	require.NoError(t, itr.Close())

	// construct error cases
	itr = NewSourceIterator([]record.Iterator{&MockIterator{err: errors.New("invalid"), init: true}}, opt)
	_, _, err := itr.NextAggData()
	require.EqualError(t, err, "invalid")
	require.NoError(t, itr.Close())
}

func TestWrapCursor(t *testing.T) {
	itr := NewWrapCursor(&MockIterator{})
	var res []*record.Record
	for {
		rec, _, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if rec == nil {
			break
		}
		res = append(res, rec)
	}
	require.Equal(t, len(res), 1)
	require.NoError(t, itr.Close())

	// construct error cases
	itr = NewWrapCursor(&MockIterator{err: io.EOF, init: true})
	_, _, err := itr.Next()
	require.NoError(t, err)
	itr = NewWrapCursor(&MockIterator{err: errors.New("invalid"), init: true})
	_, _, err = itr.Next()
	require.EqualError(t, err, "invalid")
	require.NoError(t, itr.Close())
}

func TestWrapIterator(t *testing.T) {
	itr := NewWrapIterator(&MockKeyCursor{}, 1)
	defer itr.Release()
	var res []*record.ConsumeRecord
	for {
		rec, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if rec == nil {
			break
		}
		res = append(res, rec)
	}
	require.Equal(t, len(res), 1)

	// construct error cases
	itr = NewWrapIterator(&MockKeyCursor{err: errors.New("invalid"), init: true}, 1)
	require.Equal(t, itr.SidCnt(), 1)
	_, err := itr.Next()
	require.EqualError(t, err, "invalid")
	itr.Release()
}
