package rp

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
	RPTxtFilePath  = path.Join(export.GetCurrentPath(), "rp.txt")
	RPCsvFilePath  = path.Join(export.GetCurrentPath(), "rp.csv")
	DBFilterName   = "db0"
	RPName         = "rp0"
	FilterMstName  = "average_temperature"
	FilterTimeName = "2019-08-25T09:18:00Z~2019-08-26T07:48:00Z"
)

func TestRPExport(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export txt data using retention:"+RPName, func(t *testing.T) {
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
	t.Run("test export csv data using retention:"+RPName, func(t *testing.T) {
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
