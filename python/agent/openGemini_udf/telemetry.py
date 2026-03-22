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

import threading
import time

import psutil
from multiprocessing import get_context
from multiprocessing.queues import Queue
from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, ALWAYS_OFF
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation import tracecontext

# Agent Module Metrics
# Number of tasks received
METRIC_CASTOR_TASKS = "castor_tasks"
# Number of active task
METRIC_CASTOR_TASKS_ACTIVE = "castor_tasks_active"
# Total number of successful task
METRIC_CASTOR_TASKS_SUCCESS = "castor_tasks_success"
# Total number of fail task
METRIC_CASTOR_TASKS_FAIL = "castor_tasks_fail"
# Total bytes received
METRIC_CASTOR_RECEIVED_BYTES = "castor_received_bytes"
# Total bytes sent
METRIC_CASTOR_SENT_BYTES = "castor_sent_bytes"

# UDF Module Metrics
# how many successful runs of each algorithm
METRIC_ALGORITHM_RUNS = "castor_algorithm_runs"
# how many failed runs of each algorithm
METRIC_ALGORITHM_FAIL = "castor_algorithm_fail"
# how many active runs of each algorithm
METRIC_ALGORITHM_ACTIVE = "castor_algorithm_active"
# histogram of algorithm run duration
METRIC_RUN_DURATION_SECONDS = "castor_run_duration_seconds"

# System Module Metrics
# CPU usage percentage
METRIC_CASTOR_CPU_USAGE = "castor_cpu_usage"
# Memory in use
METRIC_CASTOR_MEM_IN_USE_MB = "castor_mem_in_use_MB"

# Attribute
ATTRIBUTE_ALGORITHM = "algorithm"
ATTRIBUTE_TASK_ID = "task_id"
ATTRIBUTE_PROCESS = "process"


def init_telemetry_main_process(monitor_addr: str, monitor_database: str, https_enabled: bool, sampling_interval: int, host):
    prot = "https" if https_enabled else "http"
    span_endpoint = f"{prot}://{monitor_addr}/api/v1/otlp/{monitor_database}/traces"
    metric_endpoint = f"{prot}://{monitor_addr}/api/v1/otlp/{monitor_database}/metrics"
    # configurate Resource
    resource = Resource.create({
        "service.name": "openGemini-castor",
        "host": f"{host[0]}:{host[1]}"
    })
    # configurate Tracing
    sampler = ParentBased(root=ALWAYS_OFF)
    trace.set_tracer_provider(TracerProvider(resource=resource, sampler=sampler))
    otlp_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=span_endpoint))
    trace.get_tracer_provider().add_span_processor(otlp_processor)
    set_global_textmap(tracecontext.TraceContextTextMapPropagator())
    # configurate Metrics
    otlp_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=metric_endpoint)
        , export_interval_millis=sampling_interval * 1000
    )
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[otlp_reader]))
    return MainProcessMetricsHandler(sampling_interval)


class MainProcessMetricsHandler:
    def __init__(self, sampling_interval: int):
        # agent metrics
        agent_meter = metrics.get_meter("castor_agent")
        self.castor_sent_bytes = agent_meter.create_counter(
            name=METRIC_CASTOR_SENT_BYTES,
            description="castor_sent_bytes")
        self.castor_received_bytes = agent_meter.create_counter(
            name=METRIC_CASTOR_RECEIVED_BYTES,
            description="Total bytes received")
        self.castor_tasks_fail = agent_meter.create_counter(
            name=METRIC_CASTOR_TASKS_FAIL,
            description="Total number of fail tasks"
        )
        self.castor_tasks_success = agent_meter.create_counter(
            name=METRIC_CASTOR_TASKS_SUCCESS,
            description="Total number of successful tasks"
        )
        self.castor_tasks_active = agent_meter.create_up_down_counter(
            name=METRIC_CASTOR_TASKS_ACTIVE,
            description="Number of active tasks"
        )
        self.castor_tasks = agent_meter.create_counter(
            name=METRIC_CASTOR_TASKS,
            description="Number of tasks received")

        # system metrics
        init_system_metrics(sampling_interval)


def init_telemetry():
    return MetricsHandler()


class MetricsHandler:
    def __init__(self):
        # udf metrics
        udf_meter = metrics.get_meter("castor_udf")
        self.castor_algorithm_active = udf_meter.create_up_down_counter(
            name=METRIC_ALGORITHM_ACTIVE,
            description="how many active runs of each algorithm"
        )
        self.castor_algorithm_fail = udf_meter.create_counter(
            name=METRIC_ALGORITHM_FAIL,
            description="how many failed runs of each algorithm"
        )
        self.castor_algorithm_run_duration = udf_meter.create_histogram(
            name=METRIC_RUN_DURATION_SECONDS,
            description="histogram of algorithm run duration"
        )
        self.castor_algorithm_runs = udf_meter.create_counter(
            name=METRIC_ALGORITHM_RUNS,
            description="how many successful runs of each algorithm"
        )


def init_system_metrics(sampling_interval: int):
    metric_thread = threading.Thread(target=collect_system_metrics, args=(sampling_interval,), daemon=True)
    metric_thread.start()


def collect_system_metrics(sampling_interval: int):
    system_meter = metrics.get_meter("castor_system")
    cpu_usage = system_meter.create_gauge(
        METRIC_CASTOR_CPU_USAGE,
        description="CPU usage percentage",
        unit="%"
    )
    memory_usage = system_meter.create_gauge(
        METRIC_CASTOR_MEM_IN_USE_MB,
        description="Memory in use",
        unit="MB"
    )
    # first time calling cpu_percent(), pass a non_zero interval value to perform actual measurement
    try:
        main_process = psutil.Process()
        main_process.cpu_percent(interval=0.1)
        for child in main_process.children(recursive=True):
            child.cpu_percent(interval=0.1)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return
    while True:
        try:
            main_process = psutil.Process()
            cpu_percent = main_process.cpu_percent(interval=0.1)
            cpu_total = cpu_percent
            memory_cost = main_process.memory_info().rss
            cpu_usage.set(cpu_percent, {ATTRIBUTE_PROCESS: main_process.pid})
            memory_usage.set(memory_cost / 1024 / 1024, {ATTRIBUTE_PROCESS: main_process.pid})
            for child in main_process.children(recursive=True):
                cpu_percent = child.cpu_percent(interval=0.1)
                cpu_usage.set(cpu_percent, {ATTRIBUTE_PROCESS: child.pid})
                memory_cost = child.memory_info().rss
                memory_usage.set(memory_cost / 1024 / 1024, {ATTRIBUTE_PROCESS: child.pid})
                cpu_total += cpu_percent
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        cpu_usage.set(cpu_total, {ATTRIBUTE_PROCESS: "total"})
        time.sleep(sampling_interval)
