package pmetricotlp

import (
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

var NewExportRequest = pmetricotlp.NewExportRequest

type ExportRequest = pmetricotlp.ExportRequest
