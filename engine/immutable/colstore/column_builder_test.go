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

package colstore

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/encoding"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestColumnBuilder_EncodeColumn(t *testing.T) {
	type fields struct {
		data  []byte
		log   *Log.Logger
		coder *encoding.CoderContext
	}
	type args struct {
		ref *record.Field
		col *record.ColVal
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:   "test1",
			fields: fields{data: nil, log: nil, coder: nil},
			args: args{
				ref: &record.Field{
					Type: influx.Field_Type_Last,
					Name: "test"},
				col: nil},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered in f", r)
				}
			}()
			b := &ColumnBuilder{
				data:  tt.fields.data,
				log:   tt.fields.log,
				coder: tt.fields.coder,
			}
			got, err := b.EncodeColumn(tt.args.ref, tt.args.col)
			if (err != nil) != tt.wantErr {
				t.Errorf("ColumnBuilder.EncodeColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ColumnBuilder.EncodeColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}
