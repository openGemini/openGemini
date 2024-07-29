package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ModuleIndex 存放每个模块的指标
type ModuleIndex struct {
	MetricsMap    map[string]interface{}
	LabelValues map[string]string
}

type Metric struct {
	HelpMap map[string]string
	Labels  []string
}

func NewDesc(subsystem, name, help string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName("", subsystem, name),
		help,
		labels,
		nil,
	)
}

type BaseCollector struct {
	AllModulesDesc map[string]map[string]*prometheus.Desc
	IndexRegistry  map[string]*Metric
}

func NewBaseCollector(comIndexRegistry map[string]*Metric) *BaseCollector {
	c := &BaseCollector{
		AllModulesDesc: make(map[string]map[string]*prometheus.Desc),
		IndexRegistry:  comIndexRegistry,
	}

	return c
}

func (c *BaseCollector) Collect(ch chan<- prometheus.Metric) {
	return
}

func (c *BaseCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, v := range c.AllModulesDesc {
		for _, desc := range v {
			ch <- desc
		}
	}
}
