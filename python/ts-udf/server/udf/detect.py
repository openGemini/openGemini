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
from abc import abstractmethod
from typing import Dict, Any

import pandas
import pyarrow as pa


class DetectUDF:

    @abstractmethod
    def detect(self, data: pandas.DataFrame, params: Dict[str, Any]) -> pa.RecordBatch:
        pass

    @abstractmethod
    def pandas_to_arrow(self, df: pandas.DataFrame) -> pa.RecordBatch:
        pass

    def validate_dataframe(self, df: pandas.DataFrame, schema: pa.Schema):
        schema_column_names = schema.names
        df_columns = list(df.columns)
        # check column missing
        missing = [col for col in schema_column_names if col not in df_columns]
        if missing:
            err_msg = ",".join(missing)
            raise Exception(f"missing columns: {err_msg}")
        mismatch = []
        for index in range(len(schema.types)):
            field_type = schema.types[index]
            col_name = schema_column_names[index]
            if col_name in df_columns:
                actual_dtype = df[col_name].dtype
                expected_pd_dtype = field_type.to_pandas_dtype()
                if not pandas.api.types.is_dtype_equal(actual_dtype, expected_pd_dtype):
                    mismatch.append(
                        f"column '{col_name}': expect {field_type}, actual {actual_dtype}"
                    )
        if mismatch:
            err_msg = ",".join(mismatch)
            raise Exception(f"type mismatch: {err_msg}")
