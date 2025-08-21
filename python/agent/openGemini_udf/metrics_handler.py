"""
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import logging
from typing import Dict, Any

from opentelemetry.metrics import Counter, Histogram, UpDownCounter
from multiprocessing import Queue

logger = logging.getLogger()


class MetricsHandler:
    def __init__(self):
        self.counters: Dict[str, Counter] = {}
        self.histograms: Dict[str, Histogram] = {}
        self.up_down_counters: Dict[str, UpDownCounter] = {}

    def register_counter(self, name: str, counter: Counter):
        self.counters[name] = counter

    def register_histogram(self, name: str, histogram: Histogram):
        self.histograms[name] = histogram

    def register_up_down_counter(self, name: str, up_down_counter: UpDownCounter):
        self.up_down_counters[name] = up_down_counter


class MetricPoint:
    def __init__(self, name: str, value: Any, attribute=None):
        self.name = name
        self.value = value
        self.attribute = attribute

    def consume(self, handler: MetricsHandler):
        pass


class CounterMetricPoint(MetricPoint):
    def consume(self, handler: MetricsHandler):
        if self.name not in handler.counters:
            logger.error(f"metric {self.name} not found")
            return
        handler.counters[self.name].add(self.value, attributes=self.attribute)


class HistogramMetricPoint(MetricPoint):
    def consume(self, handler: MetricsHandler):
        if self.name not in handler.histograms:
            logger.error(f"metric {self.name} not found")
            return
        handler.histograms[self.name].record(self.value, attributes=self.attribute)


class UpDownCounterMetricPoint(MetricPoint):
    def consume(self, handler: MetricsHandler):
        if self.name not in handler.up_down_counters:
            logger.error(f"metric {self.name} not found")
            return
        handler.up_down_counters[self.name].add(self.value)


def consume_metric(
        metric_queue: Queue,
        metric_handler: MetricsHandler,
):
    logger.info("start collecting metrics")
    while True:
        metric = metric_queue.get()
        try:
            metric.consume(metric_handler)
        except Exception as error:
            logger.error(f"record metric {metric.name} error: {str(error)}")
