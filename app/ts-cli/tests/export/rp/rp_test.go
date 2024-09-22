// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package rp

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/openGemini/openGemini/app/ts-cli/tests/export"
	"github.com/stretchr/testify/assert"
	"github.com/vbauerster/mpb/v7"
)

var (
	RPTxtFilePath  = path.Join(export.GetCurrentPath(), "rp.txt")
	RPCsvFilePath  = path.Join(export.GetCurrentPath(), "rp.csv")
	DBFilterName   = "db0"
	RPName         = "rp0"
	FilterMstName  = "average_temperature"
	FilterTimeName = "2019-08-25T09:18:00Z~2019-08-26T07:48:00Z"
)

func TestRPExportTxt(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export txt data using retention:"+RPName, func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.txt")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		geminicli.MpbProgress = mpb.New(mpb.WithWidth(100))
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Out:               exportPath,
			Compress:          false,
			Format:            export.TxtFormatExporter,
			DBFilter:          DBFilterName,
			RetentionFilter:   RPName,
			MeasurementFilter: FilterMstName,
			TimeFilter:        FilterTimeName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(RPTxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}

func TestRPExportCsv(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export csv data using retention:"+RPName, func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.csv")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		geminicli.MpbProgress = mpb.New(mpb.WithWidth(100))
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Out:               exportPath,
			Compress:          false,
			Format:            export.CsvFormatExporter,
			DBFilter:          DBFilterName,
			RetentionFilter:   RPName,
			MeasurementFilter: FilterMstName,
			TimeFilter:        FilterTimeName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(RPCsvFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}
