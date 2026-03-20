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
from ts_udf.server.udf.detect import DetectUDF
from ts_udf.server.udf.forecast.model_factory import ModelFactory

ALGO_METRIC = b"_metric"
ALGO_PARAMS = b"_algoParams"
ALGO_CFG = b"_cfg"
TIMESTAMP = "timestamp"
COL_VALUE = "value"

optional_params = ["window_size", "horizon", "period", "ckpt_path"]


class ModelInferUDF(DetectUDF):
    output_schema = pa.schema([
        pa.field(TIMESTAMP, pa.int64()),
        pa.field(COL_VALUE, pa.float32()),
    ], [])

    def __init__(self):
        self.model_factory = ModelFactory()

    def detect(self, data: pandas.DataFrame, params) -> pa.RecordBatch:
        metric = params.get_value(ALGO_METRIC)
        if metric is None:
            raise Exception("metric_name not found in parameters")
        model_id = params.get_value(ALGO_CFG)
        if model_id is None:
            raise Exception("model_id not found in parameters")
        algo_params = params.get_value(ALGO_PARAMS)
        if algo_params is None:
            algo_params = {}
        else:
            algo_params = json.loads(algo_params)

        least_time = data["time"].iloc[-1]
        data.rename(columns={metric: COL_VALUE}, inplace=True)
        result = self.model_factory.find_model(model_id).infer(data, algo_params)
        result['timestamp'] = least_time + (result.index + 1) * 60 * 1000 * 1000 * 1000
        return self.pandas_to_arrow(result)

    def pandas_to_arrow(self, df: pandas.DataFrame) -> pa.RecordBatch:
        if df.empty:
            return self.empty_record(self.output_schema)
        self.validate_dataframe(df, self.output_schema)
        record = pa.RecordBatch.from_pandas(df, schema=self.output_schema, preserve_index=False)
        record = record.replace_schema_metadata(self.output_schema.metadata)
        return record
