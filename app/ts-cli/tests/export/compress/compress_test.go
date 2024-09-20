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

package compress

import (
	"compress/gzip"
	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/openGemini/openGemini/app/ts-cli/tests/export"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"path/filepath"
	"testing"
)

var (
	CompressTxtFilePath = path.Join(export.GetCurrentPath(), "compress.txt")
	CompressCsvFilePath = path.Join(export.GetCurrentPath(), "compress.csv")
	DBFilterName        = "db0"
	RPName              = "rp0"
	FilterMstName       = "average_temperature"
	FilterTimeName      = "2019-08-25T09:18:00Z~2019-08-26T07:48:00Z"
)

func TestCompressExport(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export txt data using compress", func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.gz")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Out:               exportPath,
			Compress:          true,
			Format:            export.TxtFormatExporter,
			DBFilter:          DBFilterName,
			RetentionFilter:   RPName,
			MeasurementFilter: FilterMstName,
			TimeFilter:        FilterTimeName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(CompressTxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		gzipFile, err := os.Open(exportPath)
		exportFile, err := gzip.NewReader(gzipFile)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test export csv data using compress", func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.csv")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Out:               exportPath,
			Compress:          true,
			Format:            export.CsvFormatExporter,
			DBFilter:          DBFilterName,
			RetentionFilter:   RPName,
			MeasurementFilter: FilterMstName,
			TimeFilter:        FilterTimeName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(CompressCsvFilePath)
		if err != nil {
			t.Fatal(err)
		}
		gzipFile, err := os.Open(exportPath)
		exportFile, err := gzip.NewReader(gzipFile)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}
