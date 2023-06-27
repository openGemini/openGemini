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
				nextID:            9,
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
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaskManager{
				queryIDOffset:     tt.fields.QueryIDOffset,
				queryIDUpperLimit: tt.fields.QueryIDUpperLimit,
				queries:           tt.fields.queries,
				nextID:            tt.fields.nextID,
			}
			got := t.AssignQueryID()
			if got != tt.want {
				t1.Errorf("AssignQueryID() got = %v, want %v", got, tt.want)
			}
		})
	}
}
