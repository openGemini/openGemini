// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package filterMst

import (
	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/openGemini/openGemini/app/ts-cli/tests/export"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"path/filepath"
	"testing"
)

var (
	FilterMstTxtFilePath = path.Join(export.GetCurrentPath(), "filterMst.txt")
	FilterMstCsvFilePath = path.Join(export.GetCurrentPath(), "filterMst.csv")
	DBFilterName         = "db0"
	FilterMstName        = "average_temperature"
)

func TestFilterMstExport(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export txt data using mstfilter:"+FilterMstName, func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.txt")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Out:               exportPath,
			Compress:          false,
			Format:            export.TxtFormatExporter,
			DBFilter:          DBFilterName,
			MeasurementFilter: FilterMstName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterMstTxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test export csv data using mstfilter:"+FilterMstName, func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.csv")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Out:               exportPath,
			Compress:          false,
			Format:            export.CsvFormatExporter,
			DBFilter:          DBFilterName,
			MeasurementFilter: FilterMstName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterMstCsvFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}
