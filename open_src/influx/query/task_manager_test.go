package query

import (
	"testing"
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
				nextID:            10,
			},
			want:    10,
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
		{
			name: "ReuseFromBeginning_Fail",
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 5,
				queries:           map[uint64]*Task{1: {}, 2: {}, 3: {}, 4: {}},
				nextID:            5,
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "AssignAfterReuse_Success",
			// before reuse: { 2:{}, 4:{} } next=5
			// after reuse:  { 1:{}, 2:{}, 4:{} } next=2
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 5,
				queries:           map[uint64]*Task{1: {}, 2: {}, 4: {}},
				nextID:            2,
			},
			want:    3,
			wantErr: false,
		},
		{
			name: "AssignAfterReuse_Fail",
			// before reuse: { 2:{}, 3:{}, 4:{} } next=5
			// after reuse:  { 1:{}, 2:{}, 3:{}, 4:{} } next=2
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 5,
				queries:           map[uint64]*Task{1: {}, 2: {}, 3: {}, 4: {}},
				nextID:            2,
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaskManager{
				queryIDOffset:     tt.fields.QueryIDOffset,
				queryIDUpperLimit: tt.fields.QueryIDUpperLimit,
				queries:           tt.fields.queries,
				nextID:            tt.fields.nextID,
			}
			got, err := t.AssignQueryID()
			if (err != nil) != tt.wantErr {
				t1.Errorf("AssignQueryID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t1.Errorf("AssignQueryID() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTaskManager_reuseQueryIDFromBeginning(t1 *testing.T) {
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
			name: "SuccessAtFirst",
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 100,
				queries:           map[uint64]*Task{50: {}, 70: {}, 99: {}},
				nextID:            100,
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "SuccessAtSecond",
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 100,
				queries:           map[uint64]*Task{1: {}, 98: {}, 99: {}},
				nextID:            100,
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Fail",
			fields: fields{
				QueryIDOffset:     0,
				QueryIDUpperLimit: 5,
				queries:           map[uint64]*Task{1: {}, 2: {}, 3: {}, 4: {}},
				nextID:            5,
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaskManager{
				queryIDOffset:     tt.fields.QueryIDOffset,
				queryIDUpperLimit: tt.fields.QueryIDUpperLimit,
				queries:           tt.fields.queries,
				nextID:            tt.fields.nextID,
			}
			got, err := t.reuseQueryIDFromBeginning()
			if (err != nil) != tt.wantErr {
				t1.Errorf("reuseQueryIDFromBeginning() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t1.Errorf("reuseQueryIDFromBeginning() got = %v, want %v", got, tt.want)
			}
		})
	}
}
