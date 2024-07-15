package filterTime

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
	FilterTime1TxtFilePath = path.Join(export.GetCurrentPath(), "filterTime1.txt")
	FilterTime2TxtFilePath = path.Join(export.GetCurrentPath(), "filterTime2.txt")
	FilterTime3TxtFilePath = path.Join(export.GetCurrentPath(), "filterTime3.txt")
	FilterTime4TxtFilePath = path.Join(export.GetCurrentPath(), "filterTime4.txt")
	FilterTime1CsvFilePath = path.Join(export.GetCurrentPath(), "filterTime1.csv")
	DBFilterName           = "db0"
	FilterMstName          = "average_temperature"
	FilterTimeName1        = "2019-08-25T09:18:00Z~2019-08-26T07:48:00Z"
	FilterTimeName2        = "2019-08-25T09:18:00Z~"
	FilterTimeName3        = "~2019-08-26T07:48:00Z"
	FilterTimeName4        = "1566724680000000000~1566805680000000000"
)

func TestFilterTimeExport(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export txt data using timefilter:"+FilterTimeName1, func(t *testing.T) {
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
			TimeFilter:        FilterTimeName1,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterTime1TxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test export txt data using timefilter:"+FilterTimeName2, func(t *testing.T) {
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
			TimeFilter:        FilterTimeName2,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterTime2TxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test export txt data using timefilter:"+FilterTimeName3, func(t *testing.T) {
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
			TimeFilter:        FilterTimeName3,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterTime3TxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test export txt data using timefilter:"+FilterTimeName4, func(t *testing.T) {
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
			TimeFilter:        FilterTimeName4,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterTime4TxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test export csv data using timefilter:"+FilterTimeName1, func(t *testing.T) {
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
			TimeFilter:        FilterTimeName1,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterTime1CsvFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}
