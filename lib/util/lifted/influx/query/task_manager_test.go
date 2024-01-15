package query

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
)

func TestTaskManager_AssignQueryID(t1 *testing.T) {
	type fields struct {
		QueryIDOffset     uint64
		QueryIDUpperLimit uint64
		queries           map[uint64]*Task
		nextID            uint64
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint64
		wantErr bool
	}{
		{
			name: "NormalAssign",
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 100,
				queries:           nil,
				nextID:            9,
			},
			want:    10,
			wantErr: false,
		},
		{
			name: "AssignUpperLimit",
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 100,
				queries:           map[uint64]*Task{98: {}, 99: {}},
				nextID:            99,
			},
			want:    100,
			wantErr: false,
		},
		{
			name: "AssignUpperLimit2",
			fields: fields{
				QueryIDOffset:     100,
				QueryIDUpperLimit: 200,
				queries:           map[uint64]*Task{198: {}, 199: {}},
				nextID:            199,
			},
			want:    200,
			wantErr: false,
		},
		{
			name: "ReuseFromBeginning_Success",
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 100,
				queries:           map[uint64]*Task{98: {}, 99: {}},
				nextID:            100,
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaskManager{
				queryIDOffset:     tt.fields.QueryIDOffset,
				queryIDUpperLimit: tt.fields.QueryIDUpperLimit,
				queries:           tt.fields.queries,
				nextID:            tt.fields.nextID,
				registerOnce:      1,
			}
			got := t.AssignQueryID()
			if got != tt.want {
				t1.Errorf("AssignQueryID() got = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockRegister1 struct{}

func (r *mockRegister1) RetryRegisterQueryIDOffset(host string) (uint64, error) {
	time.Sleep(200 * time.Millisecond)
	return 100000, nil
}

func TestTaskManager_tryRegisterQueryIDOffset(t1 *testing.T) {
	t := &TaskManager{
		registerOnce: 0,
		Register:     &mockRegister1{},
		Logger:       logger.NewLogger(errno.ModuleUnknown),
	}

	// Concurrent registration
	// only 1 can call RetryRegisterQueryIDOffset()
	// other 9 will sync wait and not call RetryRegisterQueryIDOffset
	var count1 int32
	wg1 := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			_ = t.tryRegisterQueryIDOffset()
			_ = t.AssignQueryID()
			atomic.AddInt32(&count1, 1)
		}()
	}
	wg1.Wait()
	assert.Equal(t1, count1, int32(10))
	assert.Equal(t1, atomic.LoadUint32(&t.registerOnce), uint32(1))
	assert.Equal(t1, t.nextID, uint64(100010))
	assert.Equal(t1, t.queryIDOffset, uint64(100000))

	// Now, simulate concurrent assign id
	// all 10 goroutines can not call RetryRegisterQueryIDOffset(),
	// but they can normally assign id
	var count2 int32
	wg2 := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			_ = t.tryRegisterQueryIDOffset()
			_ = t.AssignQueryID()
			atomic.AddInt32(&count2, 1)
		}()
	}
	wg2.Wait()
	assert.Equal(t1, count2, int32(10))
	assert.Equal(t1, atomic.LoadUint32(&t.registerOnce), uint32(1))
	assert.Equal(t1, t.nextID, t.queryIDOffset+20)
	assert.Equal(t1, t.queryIDOffset, uint64(100000))
}
