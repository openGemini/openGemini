package query

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
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
				registered:        true,
			}
			got, _ := t.AssignQueryID()
			if got != tt.want {
				t1.Errorf("AssignQueryID() got = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockRegister struct {
}

func (r *mockRegister) RegisterQueryIDOffset(host string) (uint64, error) {
	time.Sleep(2 * time.Second)
	return 100000, nil
}

func TestTaskManager_tryRegisterQueryIDOffset(t1 *testing.T) {
	t := &TaskManager{
		registerOnce: atomic.Bool{},
		Register:     &mockRegister{},
	}

	// Concurrent registration
	var count1 int32
	a := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		a.Add(1)
		go func() {
			defer a.Done()
			_ = t.tryRegisterQueryIDOffset()
			_, err := t.AssignQueryID()
			if err != nil {
				atomic.AddInt32(&count1, 1)
			}
		}()
	}
	a.Wait()
	assert.Equal(t1, int32(99), count1)
	assert.Equal(t1, t.registerOnce.Load(), true)
	assert.Equal(t1, t.registered, true)
	assert.Equal(t1, t.nextID, uint64(100001))
	assert.Equal(t1, t.queryIDOffset, uint64(100000))

	// Registration completed, simulate concurrent assign id
	var errCount2 int32
	b := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		b.Add(1)
		go func() {
			defer b.Done()
			_ = t.tryRegisterQueryIDOffset()
			_, err := t.AssignQueryID()
			if err != nil {
				atomic.AddInt32(&errCount2, 1)
			}
		}()
	}
	b.Wait()
	assert.Equal(t1, int32(0), errCount2)
	assert.Equal(t1, t.registerOnce.Load(), true)
	assert.Equal(t1, t.registered, true)
	assert.Equal(t1, t.nextID, t.queryIDOffset+1+100)
	assert.Equal(t1, t.queryIDOffset, uint64(100000))

	c := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		c.Add(1)
		go func() {
			defer c.Done()
			_, err := t.AssignQueryID()
			assert.NoError(t1, err)
		}()
	}
	c.Wait()
	assert.Equal(t1, t.nextID, t.queryIDOffset+1+100+100)
}
