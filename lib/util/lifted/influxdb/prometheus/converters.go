package prometheus

import (
	"github.com/influxdata/influxdb/models"
	"github.com/prometheus/prometheus/prompb"
)

// ModelTagsToLabelPairs converts models.Tags to a slice of Prometheus label pairs
func ModelTagsToLabelPairs(tags models.Tags) []prompb.Label {
	pairs := make([]prompb.Label, 0, len(tags))
	for _, t := range tags {
		if string(t.Value) == "" {
			continue
		}
		pairs = append(pairs, prompb.Label{
			Name:  string(t.Key),
			Value: string(t.Value),
		})
	}
	return pairs
}
