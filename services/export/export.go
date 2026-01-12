// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package export

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/parquet"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	FileExtensionParquet = ".parquet"
	TmpFileSuffix        = ".tmp"

	FieldNameTime = "time"

	DirPermission = 0700

	DateLayoutYYYYMMDD = "20060102"
)

// TaskExportContext represents the export context, containing the resources required for export.
type TaskExportContext struct {
	Store  *LocalStore
	Engine engine.Engine
}

// Exporter is the interface that all export formats must implement.
type Exporter interface {
	Export(ctx *TaskExportContext, subTask *SubExportTask) error
}

// ParquetExporter is the exporter for the Parquet format.
type ParquetExporter struct {
	exportPath string
}

// NewParquetExporter creates a new ParquetExporter instance.
func NewParquetExporter() Exporter {
	return &ParquetExporter{
		exportPath: config.GetStoreConfig().DataExport.ExportDir,
	}
}

// Export implements the Export method for the ParquetExporter.
func (e *ParquetExporter) Export(ctx *TaskExportContext, subTask *SubExportTask) error {
	if subTask == nil {
		return errors.New("subTask is nil")
	}

	dir, fullPath := e.generateFilePath(subTask)
	if err := fileops.MkdirAll(dir, DirPermission); err != nil {
		return fmt.Errorf("failed to create dir: %w", err)
	}

	tmpPath := fullPath + TmpFileSuffix
	writer, err := parquet.NewWriter(fullPath, "",
		parquet.MetaData{Mst: subTask.Mst, Schemas: optsToSchemas(subTask.Opts)})
	if err != nil {
		return err
	}
	defer func() {
		writer.Close()
		if info, err := fileops.Stat(fullPath); err == nil {
			Stat.SubTaskExportedSize.Add(info.Size())
		}
		if _, statErr := fileops.Stat(tmpPath); statErr != nil {
			return
		}
		if removeErr := fileops.Remove(tmpPath); removeErr != nil {
			logger.GetLogger().Error("failed to check temporary file existence",
				zap.String("tempFile", tmpPath),
				zap.Error(removeErr))
		}
	}()

	ident := util.MeasurementIdent{
		DB:   subTask.Db,
		RP:   subTask.Rp,
		Name: subTask.Mst,
	}
	// Create consume iterator for specific ptID
	iterators, release := ctx.Engine.CreateConsumeIterator(ident, []uint32{subTask.Pt}, subTask.Opts)
	defer func() {
		for _, itr := range iterators {
			itr.Release()
		}
		if release != nil {
			release()
		}
	}()

	// Write records for the specific ptID
	for idx, itr := range iterators {
		for {
			rec, err := itr.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to read from iterator[%d]: %w", idx, err)
			}
			if err = writer.WriteRecord(tagsToMap(rec.Tags), rec.Rec); err != nil {
				return fmt.Errorf("failed to write record: %w", err)
			}
			Stat.SubTaskExportedRows.Add(int64(rec.Rec.RowNums()))
		}
	}

	err = writer.ForceFlush()
	if err != nil {
		return err
	}
	err = writer.WriteStop()
	if err != nil {
		return err
	}

	logger.GetLogger().Info("Successfully exported data",
		zap.String("db", subTask.Db),
		zap.String("rp", subTask.Rp),
		zap.Uint32("ptID", subTask.Pt),
		zap.String("file_path", fullPath))

	return nil
}

func (e *ParquetExporter) generateFilePath(subTask *SubExportTask) (dir string, fullPath string) {
	safeDb := filepath.Clean(subTask.Db)
	safeRp := filepath.Clean(subTask.Rp)

	// 1. Format the date as "YYYYMMDD" from the StartTime
	exportDate := time.Unix(0, subTask.Opts.StartTime).In(subTask.Timezone).Format(DateLayoutYYYYMMDD)

	// 2. Construct the directory path
	// Template: {export_path}/{db}/{rp}/{PtID}/{exportDate}/{taskId}/
	dir = filepath.Join(
		e.exportPath,
		safeDb,
		safeRp,
		fmt.Sprintf("%v", subTask.Pt),
		exportDate,
		fmt.Sprintf("%v", subTask.ParentID),
	)

	// 3. Construct the full file name
	// Template: {mst}_{begin}_{end}.parquet
	fileName := fmt.Sprintf(
		"%s_%d_%d%s",
		subTask.Mst,
		subTask.Opts.StartTime,
		subTask.Opts.EndTime,
		FileExtensionParquet,
	)

	// 4. Combine the directory and file name to get the final full path
	fullPath = filepath.Join(dir, fileName)

	return dir, fullPath
}

func tagsToMap(tags []*record.Tag) map[string]string {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		if tag == nil {
			continue
		}
		tagMap[tag.Key] = tag.Value
	}
	return tagMap
}

func optsToSchemas(opts *query.ProcessorOptions) map[string]uint8 {
	schemas := make(map[string]uint8)
	if opts == nil {
		return schemas
	}

	for _, varRef := range opts.FieldAux {
		schemas[varRef.Val] = uint8(record.ToModelTypes(varRef.Type))
	}
	for _, varRef := range opts.TagAux {
		schemas[varRef.Val] = uint8(record.ToModelTypes(varRef.Type))
	}
	schemas[FieldNameTime] = uint8(influx.Field_Type_Int)
	return schemas
}

var DefaultExporterFactory = NewExporterFactory()

func NewExporterFactory() *ExporterFactory {
	return &ExporterFactory{
		Exporters: map[DataFormat]Exporter{
			FormatParquet: NewParquetExporter(),
		},
	}
}

type ExporterFactory struct {
	Exporters map[DataFormat]Exporter
}

// GetExporter returns the exporter for the specified format.
func (f *ExporterFactory) GetExporter(format DataFormat) (Exporter, error) {
	exporter, ok := f.Exporters[format]
	if !ok {
		return nil, errors.New("unsupported export format: " + string(format))
	}
	return exporter, nil
}
