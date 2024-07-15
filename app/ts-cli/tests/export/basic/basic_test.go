package basic

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
	BasicTxtFilePath = path.Join(export.GetCurrentPath(), "basic.txt")
	BasicCsvFilePath = path.Join(export.GetCurrentPath(), "basic.csv")
)

func TestBasicExport(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test basic export txt data", func(t *testing.T) {
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
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(BasicTxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	t.Run("test basic export csv data", func(t *testing.T) {
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
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		file, err := os.Open(BasicCsvFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile, err := os.Open(exportPath)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}
