"""
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

# Handler for castor, which process input from agent and call castor to get the results
from __future__ import absolute_import
from __future__ import division
from typing import List

from openGemini_udf.agent import Handler
import pyarrow as pa
from castor.utils.logger import logger
from castor.detector.cache.cache import CacheSet, KeyCache

from . import const as con
from .server import run
from .data_interface import DataInterface, MetadataProcessor
from .detect import DetectorUDF
from .fit_detect import FitDetectorUDF
from .fit import FitUDF


class CastorHandler(Handler, DataInterface):
    def __init__(self, data_dir, model_config_dir, version, meta_data):
        """Constructor"""

        DataInterface.__init__(self)

        # parameters for this handler
        self.ip = "127.0.0.1"
        self.port = 0

        # (local)root dir for udf server
        self._data_dir = data_dir

        # model config dir
        self._model_config_dir = model_config_dir

        # version
        self._version = version

        self._meta = meta_data

        self.all_params = {}

    def info(self):
        pass

    def init(self, init_req: dict) -> None:
        pass

    def stop(self):
        pass

    def running(self):
        pass

    def stream_data(
        self, data_req: dict, cache=None, hook=None
    ) -> List[pa.RecordBatch]:
        pass

    def batch_data(
        self, data_req_list: List[pa.RecordBatch], cache=None, hook=None
    ) -> List[pa.RecordBatch]:
        """
        detect list of batch data
        :param data_req_list: list of data need to be processed, type: pyarrow.Table
            one example of list:
                schema:
                    time: int64
                    field1: float64
                    field2: float64
                    ...
                    -- schema metadata --
                        b"_algo": b"DIFFERENTIATEAD"
                        b"_cfg": b"xxx"
                        b"_processType": b"_detect"
                        b"_taskID": "stream_cpu"
                        b"_connID":	b"3"
                        b"_msgType": b"data"
                        b"_dataID":	b"xxx"
                        b"_queryMode": "0"   (0:discontinuously/1:continuously) (2.0 required)
                        b"_outputInfo": "anomalyScore,originalValue,triggerType" # apply to inform
                            which information of anomaly will output (2.0 required)
                        b"tagKey1": "tagValue1"  # a tag of series
                        b"tag2Key2": "tagValue2"  # a tag of series
                buffer:
                    [
                        [1, 2, 3, 4],
                        [0.1, 0.2, 0.3, 0.4]
                    ]
        :param cache: global shared memory
        :param hook:  hook function to safely recycle allocated memory
        :return: List[pa.RecordBatch] list of result. The result is a list, which contain anomaly information of
        multi fields
        one example of list of one result:
            schema:
                time: int64
                anomalyScore: float64
                originalValue: float64
                ...
                -- schema metadata --
                    b"_taskID": "stream_cpu"
                    b"tagKey1": "tagValue1"  # a tag of series
                    b"tag2Key2": "tagValue2"  # a tag of series
                    b"filed_keys": b"field1"
                    b"series": "agentSN=11"
                    b"type": "write_anomaly"
                    b"filed_keys": "field1"
                    b"_connID":	b"3"
                    b"_msgType": b"data"
                    b"_dataID":	b"xxx"
                    b"_taskID":	b"stream_cpu"
                    b"_anomalyNum": b"1"
                    b"_errInfo": "Occur error when detect series: ...."

            buffer:
                [
                    1655656320000000000, 1.0, 20.0, "ValueChangeAD"
                ]
        """
        batch_result_list = []

        for data_req in data_req_list:
            metadata = data_req.schema.metadata

            try:
                metadata_processor = MetadataProcessor(metadata)
                metadata_processor.process()
            except KeyError as error:
                logger.error(error)
                metadata[con.ERR_INFO] = (
                    "Occur error when process metadata: %s" % error
                ).encode()
                return [self._create_arrow_with_metadata(metadata)]

            # transform from pyarrow.RecordBatch to pd.DataFrame
            tags = metadata_processor.get_other_metadata()
            task_id = metadata.get(con.TASK_ID)
            df = self.arrow_to_pandas(data_req, tags, task_id, self._meta)

            # clear relative cache when the query mode if discontinuously
            if metadata_processor.get_value(con.QUERY_MODE) == str(0):
                self.clear_cache_by_keys(list(df.columns))
            # process the data
            req = {con.DATA: df, con.INFO: metadata_processor}
            process_type = metadata_processor.get_value(con.PROCESS_TYPE)
            if hasattr(self, process_type):
                result = getattr(self, process_type)(req, cache, hook)
                batch_result_list.extend(result)
            else:
                err_info = (
                    "Only fit, fit_detect and detect are valid process types, %s is invalid"
                    % process_type
                )
                logger.error(err_info)
                return_metadata = metadata_processor.get_output_metadata()
                return_metadata[con.ERR_INFO] = (
                    "Occur error when process metadata: %s" % err_info
                ).encode()
                return [self._create_arrow_with_metadata(return_metadata)]
        return batch_result_list

    def _fit(self, data_req: dict, cache, hook) -> List[pa.RecordBatch]:
        self.process_handler = FitUDF(
            self._data_dir,
            self._model_config_dir,
            self._version,
            self._meta,
            self.all_params,
        )
        return self.process_handler.data(data_req, cache, hook)

    def _detect(self, data_req: dict, cache, hook) -> List[pa.RecordBatch]:
        self.process_handler = DetectorUDF(
            self._data_dir,
            self._model_config_dir,
            self._version,
            self._meta,
            self.all_params,
        )
        return self.process_handler.data(data_req, cache, hook)

    def _fit_detect(self, data_req: dict, cache, hook) -> List[pa.RecordBatch]:
        self.process_handler = FitDetectorUDF(
            self._data_dir,
            self._model_config_dir,
            self._version,
            self._meta,
            self.all_params,
        )
        return self.process_handler.data(data_req, cache, hook)

    @staticmethod
    def clear_cache_by_keys(keys):
        cache_set = CacheSet()
        key_cache = KeyCache()
        keys = [str(key) for key in keys]
        for kv_cache in cache_set.values():
            kv_cache.remove_values_by_keys(keys)
        key_cache.remove_values_by_key(set(keys))


if __name__ == "__main__":
    run(CastorHandler)
