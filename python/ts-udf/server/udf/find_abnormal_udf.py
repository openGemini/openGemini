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
import json

import pandas
import pyarrow as pa

from . import detect
from .detect import DetectUDF
from .sudden_increase_STL3 import single_metric_anomaly_STL, hyper_params

TIMESTAMP = "timestamp"
ENTITY_ID = "entity_id"
ID = "id"
TYPE = "type"
NAME = "name"
LEVEL = "level"
RULE_ID = "rule_id"
ANNOTATIONS = "annotations"

ALGO_ENTITY_ID = b"entity_id"
ALGO_METRIC = b"_metric"
ALGO_PARAMS = b"_algoParams"
TIME = "time"
ALGO_TIME = "ts"

class FindAbnormalUDF(DetectUDF):
    output_schema = pa.schema([
        pa.field(TIMESTAMP, pa.int64()),
        pa.field(ENTITY_ID, pa.string()),
        pa.field(ID, pa.string()),
        pa.field(TYPE, pa.string()),
        pa.field(NAME, pa.string()),
        pa.field(LEVEL, pa.int64()),
        pa.field(RULE_ID, pa.string()),
        pa.field(ANNOTATIONS, pa.string())
    ], [])

    def detect(self, data: pandas.DataFrame, params) -> pa.RecordBatch:
        data = data.rename(columns={TIME: ALGO_TIME})
        entity_id = params.get_value(ALGO_ENTITY_ID)
        if entity_id is None:
            raise Exception("entity_id not found")
        metric = params.get_value(ALGO_METRIC)
        if metric is None:
            raise Exception("metric not found")
        algo_params = params.get_value(ALGO_PARAMS)
        if algo_params is None:
            algo_params = {}
        else:
            algo_params = json.loads(algo_params)
        hyper_params_copy = hyper_params.copy()
        hyper_params_copy.update(algo_params)
        result = single_metric_anomaly_STL(entity_id, metric, data, hyper_params_copy)
        return self.pandas_to_arrow(result)

    def pandas_to_arrow(self, df: pandas.DataFrame) -> pa.RecordBatch:
        if df.empty:
            return self.empty_record(self.output_schema)
        self.validate_dataframe(df, self.output_schema)
        record = pa.RecordBatch.from_pandas(df, schema=self.output_schema, preserve_index=False)
        record = record.replace_schema_metadata(self.output_schema.metadata)
        return record

    def empty_record(self, output_schema) -> pa.RecordBatch:
            empty_arrays = [
                pa.array([], type=field.type) for field in output_schema
            ]
            return pa.RecordBatch.from_arrays(empty_arrays, schema=output_schema)

