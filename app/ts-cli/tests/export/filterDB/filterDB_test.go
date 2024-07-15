package filterDB

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
	FilterDBTxtFilePath = path.Join(export.GetCurrentPath(), "filterDB.txt")
	FilterDBCsvFilePath = path.Join(export.GetCurrentPath(), "filterDB.csv")
	DBFilterName        = "db0"
)

func TestFilterDBExport(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export txt data using dbfilter", func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.txt")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:   true,
			DataDir:  dir,
			WalDir:   dir,
			Out:      exportPath,
			Compress: false,
			Format:   export.TxtFormatExporter,
			DBFilter: DBFilterName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterDBTxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test export csv data using dbfilter", func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "export.csv")
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:   true,
			DataDir:  dir,
			WalDir:   dir,
			Out:      exportPath,
			Compress: false,
			Format:   export.CsvFormatExporter,
			DBFilter: DBFilterName,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(FilterDBCsvFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}
