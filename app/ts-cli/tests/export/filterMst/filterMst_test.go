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
