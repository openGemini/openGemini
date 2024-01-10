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

package analyzer_test

import (
	"testing"

	"github.com/openGemini/openGemini/app/ts-cli/analyzer"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestAnalyze(t *testing.T) {
	file := buildFile(t, t.TempDir())

	az := analyzer.NewAnalyzer()
	az.Analyze(file)
}

func buildFile(t *testing.T, saveDir string) string {
	lockPath := ""
	fileName := immutable.NewTSSPFileName(1, 0, 0, 0, true, &lockPath)
	builder := immutable.NewMsBuilder(saveDir, "mst", &lockPath, immutable.GetTsStoreConfig(), 0, fileName, 1, nil, 2, config.TSSTORE)

	schema := getDefaultSchemas()
	rec := record.NewRecordBuilder(schema)
	rec.Column(0).AppendIntegers(1, 2, 3, 4, 5, 6, 7, 8, 9)
	rec.Column(1).AppendFloats(1, 2, 3, 4, 5, 6, 7, 8, 9)
	rec.Column(2).AppendBooleans(true, true, false, false, false, true, false, false, false)
	rec.Column(3).AppendStrings("a", "b", "c", "d", "e", "b", "c", "d", "e")
	rec.AppendTime(1, 2, 3, 4, 5, 6, 7, 8, 9)
	require.NoError(t, builder.WriteData(1, rec))

	rec.ResetForReuse()
	rec.Column(0).AppendIntegers(11, 299999999, 35, 422222222222, 5111111, 1888768, 1, 2)
	rec.Column(1).AppendFloats(1.11, 2.22, 3.22, 4.33, 5.44, 6.66, 7.77, 8.88)
	rec.Column(2).AppendBooleanNulls(rec.Column(0).Len - 2)
	rec.Column(2).AppendBooleans(true, true)
	rec.Column(3).AppendStringNulls(rec.Column(0).Len)
	rec.AppendTime(1, 2, 33, 444, 5555, 66666, 777777, 88888888)
	require.NoError(t, builder.WriteData(2, rec))

	rec.ResetForReuse()
	rec.Column(0).AppendIntegers(1)
	rec.Column(1).AppendFloats(1)
	rec.Column(2).AppendBooleanNulls(1)
	rec.Column(3).AppendStringNulls(1)
	rec.AppendTime(1)
	require.NoError(t, builder.WriteData(3, rec))

	file, err := builder.NewTSSPFile(false)
	require.NoError(t, err)
	defer file.Close()

	return file.Path()
}

func getDefaultSchemas() record.Schemas {
	return record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
	}
}
