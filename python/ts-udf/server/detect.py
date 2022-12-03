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

# UDF Handler to Detect
from __future__ import absolute_import
from __future__ import division
import os
import datetime
import traceback
from typing import Dict, Hashable, List
import time

import pandas as pd
import pyarrow as pa
import numpy as np
from castor.utils import const as castor_con
from castor.utils.logger import logger
from castor.utils.common import ava
from castor.detector.pipeline_detector import PipelineDetector
from castor.utils import globalSymbol
from castor.utils.exceptions import (
    NoModelFileError,
    ValueMissError,
    ValueNotEnoughError,
    NoNewDataError,
)
from castor.detector.cache.cache import CacheSet

from .base_functions import TimeSeriesType
from .base import BaseUDF, algo_alarm_map
from . import const as con

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


class DetectorUDF(BaseUDF):
    def __init__(self, data_dir, model_config_dir, version, meta_data, all_params):
        """Constructor"""

        super().__init__(data_dir, model_config_dir, version, meta_data, all_params)

        # last refit time
        self._last_refit_time = None
        self._type = "_detect"
        self.duration = 0
        self.number_detection = 0
        self.freq = None

    def _write_anomaly(
        self,
        algo_anomaly_map: Dict[str, Dict[Hashable, pd.DataFrame]],
        total_batch_num: int,
    ) -> List[pa.RecordBatch]:
        # add type tag
        # add generateTime field
        # add series key tag
        # transfer anomaly type field to tag
        # get the pyarrow record batch
        total_batches = []

        # when error info is in cache and no anomaly in result, add the error info in tag and return arrow
        # when error info not is in cache and no anomaly in result, return empty list
        if not algo_anomaly_map:
            error_info_cache = CacheSet().get_cache(castor_con.ERROR_INFO)
            if list(error_info_cache.keys()):
                self.add_tag(con.ERR_INFO, ", ".join(error_info_cache.values()))
                error_info_cache.clear()
                return [self.pandas_to_arrow()]
            else:
                return total_batches

        # when where are some anomalies in result, add tags and return arrow
        self.add_tag(con.ANOMALY_NUM, str(total_batch_num))

        now_time = np.int64(time.time() * con.SEC_TO_NS)
        if castor_con.GENERATE_TIME in self._output_info:
            self.add_field(castor_con.GENERATE_TIME, now_time, pa.int64())

        self.extra_tag_kv.update(
            {con.WRITE_TYPE: "write_anomaly", con.SERIES: self.series_key}
        )

        for algo, detect_results in algo_anomaly_map.items():
            self.add_tag(castor_con.TYPE, algo_alarm_map.get(algo, algo))
            for field, value in detect_results.items():
                if castor_con.FREQ in self._output_info:
                    self.add_field(castor_con.FREQ, self.freq, pa.int64())
                self.add_tag(con.FIELD_NAME, self._meta.get_meta_data_by_id(field)[2])
                batch = self.pandas_to_arrow(value)
                total_batches.append(batch)
        return total_batches

    def data(self, data_req: dict, cache, hook) -> List[pa.RecordBatch]:
        """
         data: detect the data

        :params data_req: the information of the data
         one example:
         {
             "info": InfoProcessor
             "data": pd.dataframe
         }
        """
        self.batch_init(data_req.get(con.INFO), cache, hook)
        self._check_algorithm()
        df = data_req.get(con.DATA)
        total_data_nums = df.shape[0] * df.shape[1]
        batch_record_list = [self.pandas_to_arrow()]

        try:
            params = self._get_mapped_algorithm_params(df.columns)
            # the detect result is a pandas dataframe which already have anomaly_score
            detect_results = self._detect(df, params)
        except Exception as error:
            meta = batch_record_list[0].schema.metadata
            meta[con.ERR_INFO] = (
                "Occur error when detect series: " + str(error)
            ).encode()
            batch_record_list[0] = batch_record_list[0].replace_schema_metadata(meta)
            self._clear_data()
            return batch_record_list

        try:
            algo_anomalies_map, total_batch_num = self._post_process(
                detect_results, total_data_nums
            )
            anomaly_batch_record = self._write_anomaly(
                algo_anomalies_map, total_batch_num
            )
            if anomaly_batch_record:
                batch_record_list = anomaly_batch_record
        except Warning:
            pass
        except Exception as error:
            logger.error(traceback.format_exc())
            logger.error(
                "%s write response raise error %s in %s task",
                data_req,
                error,
                self._taskID,
            )
            meta = batch_record_list[0].schema.metadata
            meta[con.ERR_INFO] = (
                "Occur error when detect series: " + str(error)
            ).encode()
            batch_record_list[0] = batch_record_list[0].replace_schema_metadata(meta)
        finally:
            self._clear_data()
        return batch_record_list

    def _load_detector(self, params: dict):
        model_file = os.path.join(
            self._model_path, self.series_key + "_" + self._version
        )

        if set(self._algorithm) <= castor_con.DETECTOR_FOR_DETECT:
            detector = PipelineDetector(algo=self._algorithm, params=params)
        else:
            msg = "%s not support, please use a valid algorithm."
            logger.error(msg, self._algorithm)
            raise TypeError(msg % self._algorithm)

        if os.path.exists(model_file):
            detector.load_model_file(model_file=model_file)

        return detector

    def _detect(self, data: pd.DataFrame, params: dict) -> List[TimeSeriesType]:
        start_time = datetime.datetime.now()

        # the model is loaded from disk each time

        anomalies = []
        try:
            hd_detector = self._load_detector(params)

            anomalies = self._detect_core(hd_detector, data)
            self.freq = self.get_freq(hd_detector.freq)
            self.duration = ava(
                self.duration, (datetime.datetime.now() - start_time).total_seconds()
            )
            self.number_detection += 1
            stats = globalSymbol.Stats()
            stats.set_stats(self.duration, self.number_detection)
            duration = self.duration

            speed = len(data) / (duration + 1e-6)

            logger.info(
                "detect series %s in %s task with " "time: %ss, speed:%s p/s",
                self.series_key,
                self._taskID,
                duration,
                speed,
            )
        except Warning:
            pass
        except NoModelFileError:
            logger.warning("no detector to detect series: %s", self.series_key)
        except (ValueNotEnoughError, ValueMissError, NoNewDataError) as error:
            logger.info(
                "%s: detect series %s catch exception: %s",
                self._taskID,
                self.series_key,
                error,
            )
            raise error
        except Exception as error:
            logger.error(traceback.format_exc())
            logger.error(
                "%s: detect series %s catch exception: %s",
                self._taskID,
                self.series_key,
                error,
            )
            raise error
        return anomalies

    def _detect_core(
        self, hd_detector: PipelineDetector, data: pd.DataFrame
    ) -> List[TimeSeriesType]:
        return hd_detector.run(data)
