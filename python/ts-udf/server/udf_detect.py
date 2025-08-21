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
import time
from typing import Dict, List

import pandas as pd
import pyarrow as pa

from . import const as con
from .data_interface import DataInterface, MetadataProcessor
from .udf.detect import DetectUDF
from .udf.find_abnormal_udf import FindAbnormalUDF
from opentelemetry import trace
from castor.utils.logger import logger
from opentelemetry.trace import Status, StatusCode
from openGemini_udf.metrics_handler import CounterMetricPoint, HistogramMetricPoint, UpDownCounterMetricPoint
from openGemini_udf.telemetry import METRIC_ALGORITHM_FAIL, METRIC_CASTOR_TASKS_FAIL, METRIC_ALGORITHM_RUNS, \
    METRIC_CASTOR_TASKS_SUCCESS, ATTRIBUTE_ALGORITHM, ATTRIBUTE_TASK_ID

udf_trace = trace.get_tracer("udf")


class UdfDetect(DataInterface):
    _detect_udf_table: Dict[str, DetectUDF] = {}

    @staticmethod
    def register_detect_udfs():
        UdfDetect.register_detect_udf("find_abnormal", FindAbnormalUDF())

    @staticmethod
    def register_detect_udf(udf_name, detect_udf):
        UdfDetect._detect_udf_table[udf_name] = detect_udf

    @staticmethod
    def _find_detect_udf(udf_name: str) -> DetectUDF:
        udf = UdfDetect._detect_udf_table.get(udf_name)
        if udf is None:
            raise Exception("Algorithm %s not found" % udf_name)
        return udf

    def __init__(self, metrics_queue):
        DataInterface.__init__(self)
        self._taskID = ""
        self._algorithm = None
        self.metrics_queue = metrics_queue

    def empty_arrow(self) -> pa.RecordBatch:
        tag_kv = self.g_tag_kv.copy()
        tag_kv.update(self.extra_tag_kv)
        df = pd.DataFrame()
        # create the schema
        schema = pa.schema({}, tag_kv)
        batch = pa.RecordBatch.from_pandas(
            df=df, schema=schema, preserve_index=False
        )
        batch = batch.replace_schema_metadata(schema.metadata)
        return batch

    def post_process(self, rb: pa.RecordBatch) -> pa.RecordBatch:
        tag_kv = self.g_tag_kv.copy()
        tag_kv.update(self.extra_tag_kv)
        tag_kv.update(rb.schema.metadata)
        new_schema = rb.schema.with_metadata(tag_kv)
        rb = rb.replace_schema_metadata(new_schema.metadata)
        return rb

    def init(self, info: MetadataProcessor):
        self._algorithm = info.get_value(con.ALGORITHM)
        self._taskID = info.get_value(con.TASK_ID)
        self.extra_tag_kv.update(info.get_output_metadata())
        self.g_tag_kv = info.get_other_metadata()

    @udf_trace.start_as_current_span("detect")
    def detect(self, data_req: Dict) -> List[pa.RecordBatch]:
        params = data_req[con.INFO]
        self.init(params)

        logger.info(f"start algorithm {self._algorithm} in task {self._taskID}")
        current_span = trace.get_current_span()
        current_span.set_attribute(ATTRIBUTE_ALGORITHM, self._algorithm)
        current_span.set_attribute(ATTRIBUTE_TASK_ID, self._taskID)

        df = data_req[con.DATA]
        batch_record_list = [self.empty_arrow()]
        start_time = time.time()
        try:
            udf = UdfDetect._find_detect_udf(self._algorithm)
            detect_result = udf.detect(df, params)
        except Exception as error:
            msg = f"Occur error when executing algorithm {self._algorithm} in task {self._taskID}: {str(error)}"
            logger.error(msg)
            meta = batch_record_list[0].schema.metadata
            meta[con.ERR_INFO] = msg.encode()

            current_span.set_status(Status(StatusCode.ERROR))
            self.metrics_queue.put(CounterMetricPoint(METRIC_ALGORITHM_FAIL, 1, {ATTRIBUTE_ALGORITHM: self._algorithm}))
            self.metrics_queue.put(CounterMetricPoint(METRIC_CASTOR_TASKS_FAIL, 1))

            batch_record_list[0] = batch_record_list[0].replace_schema_metadata(meta)
            return batch_record_list
        finally:
            duration = time.time() - start_time
            logger.info(f"algorithm {self._algorithm} spends {duration} seconds in task {self._taskID}")
            current_span.set_attribute("time_cost_seconds", duration)

        self.metrics_queue.put(CounterMetricPoint(METRIC_ALGORITHM_RUNS, 1, {ATTRIBUTE_ALGORITHM: self._algorithm}))
        self.metrics_queue.put(CounterMetricPoint(METRIC_CASTOR_TASKS_SUCCESS, 1))

        batch_record_list = [self.post_process(detect_result)]
        current_span.set_status(Status(StatusCode.OK))
        return batch_record_list
