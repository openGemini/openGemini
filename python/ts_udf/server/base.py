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
# UDF Handler of Castor Anomaly Detector

from __future__ import absolute_import
import os
import traceback
from typing import Dict, Hashable, List

import pandas as pd
from castor.utils import globalSymbol
from castor.utils.logger import logger
from castor.utils.base_functions import load_params_from_yaml
from castor.utils.common import get_freq, TimeSeriesType

from .base_functions import filter_and_format, get_mapped_params
from .data_interface import DataInterface, MetadataProcessor
from . import const as con

algo_alarm_map = {
    "DIFFERENTIATEAD": "突增/突降",
    "ThresholdAD": "超过/低于阈值",
    "IncrementalAD": "持续增长",
    "ValueChangeAD": "数值变化",
}


class BaseUDF(DataInterface):
    def __init__(self, data_dir, model_config_dir, version, meta_data, all_params):
        """Constructor"""
        DataInterface.__init__(self)

        # model directory for this task
        self._model_path = ""

        # name of this task
        self._taskID = ""

        # correspond fit task
        self._fit_taskID = ""

        # (local)root dir for udf server
        self._data_dir = data_dir

        # model config dir
        self._model_config_dir = model_config_dir

        # version
        self._version = version

        # config file
        self._cfg = None

        # config string: json format

        # config string
        self._cfg_str = None

        # detector or predictor selected according to tick
        self._algorithm = None

        self._user_func = ""

        self._save_model = False

        self.stag_keys = []

        self._type = ""

        self._tags = None

        self._cache = None

        self._hook = None

        # parameters for this task
        self._params = None

        self._all_params = all_params

        self._meta = meta_data

        self._output_info = None  # table field keys in output

        self._output_metadata = None  # tags_key in output

    def init(self, info: MetadataProcessor, cache, hook):
        """init: Define what your UDF expects as parameters when
        parsing the TICKScript"""

        symbol = globalSymbol.Symbol()
        symbol.clear_all()
        self._cfg = info.get_value(con.CONFIG_FILE)
        self._algorithm = info.get_value(con.ALGORITHM)
        self._taskID = info.get_value(con.TASK_ID)
        self._cache = cache
        self._hook = hook

        self._output_info = info.get_value(con.OUTPUT_INFO)
        if self._output_info is None:
            self._output_info = con.DEFAULT_OUTPUT_INFO
        else:
            self._output_info = self._output_info.split(",")

        self.extra_tag_kv.update(info.get_output_metadata())

    def stream_init(self, info: MetadataProcessor, cache, hook) -> None:
        """ "
        init for stream
        add output info to gtag
        """
        self.init(info, cache, hook)

    def batch_init(self, info: MetadataProcessor, cache, hook) -> None:
        """ "
        init for batch
        add output info and tags into gtag
        """
        self.init(info, cache, hook)
        self._tags = info.get_other_metadata()
        self._fit_taskID = self._taskID
        self.g_tag_kv = self._tags

        self._get_series_key(self._tags)
        # model path and anomaly path
        self._model_path = os.path.join(self._data_dir, "models", self._fit_taskID)

    @staticmethod
    def is_stop():
        symbol = globalSymbol.Symbol()
        stop = symbol.get_symbol("stop")
        if stop:
            logger.debug("stop, skip data")
            return True
        symbol.set_symbol("run", True)

        return False

    def _parse_params(self):
        """
        if params have already in memory, get it and return
        if params are not in memory, load it from model config dir by file name
        if the file not in the model config dir, raise error
        """
        if (self._taskID, self._cfg) in self._all_params:
            self._params = self._all_params.get((self._taskID, self._cfg))
        elif self._cfg in self._all_params:
            self._params = self._all_params.get(self._cfg)
        elif self.is_config_file_exists(self._cfg):
            config_file_path = os.path.join(self._model_config_dir, self._cfg + ".yaml")
            self._params = load_params_from_yaml(
                config_string="", config_file=config_file_path
            )
            self._all_params[self._cfg] = self._params
        else:
            raise FileNotFoundError(
                "For %s task, %s is not found in config path" % (self._taskID, self._cfg)
            )

    def is_config_file_exists(self, config_file):
        dir_list = os.listdir(self._model_config_dir)
        if config_file + ".yaml" in dir_list:
            return True
        return False

    def _get_mapped_algorithm_params(self, ids):
        self._parse_params()
        params = get_mapped_params(self._meta, ids, self._params)
        return params

    def _check_algorithm(self):
        try:
            self._algorithm = self._algorithm.split(",")
            logger.debug("the %s is applied to %s task", self._algorithm, self._taskID)
        except AttributeError as e:
            logger.error(traceback.format_exc())
            logger.error(
                "error when splitting algorithm string: "
                "%s, original algorithm is %s",
                e,
                self._algorithm,
            )

    def _post_process(
        self, detect_results: List[TimeSeriesType], raw_len: int
    ) -> (Dict[str, Dict[Hashable, pd.DataFrame]], int):
        """ "
        count anomaly length and compute anomaly ratio
        """
        total_anomalies_num = 0
        algo_anomalies_map = {}
        total_batch_num = 0
        for algo, result in zip(self._algorithm, detect_results):

            field_anomaly_map = filter_and_format(result, self._output_info)
            anomaly_length = 0
            for value in field_anomaly_map.values():
                value_len = len(value)
                if value_len:
                    anomaly_length += value_len

            try:
                anomalies_ratio = anomaly_length / raw_len
            except ZeroDivisionError:
                anomalies_ratio = 0

            logger.debug(
                "anomalies_ratio: %s after suppress, algorithm: %s",
                anomalies_ratio,
                algo,
            )
            total_anomalies_num += anomaly_length
            if field_anomaly_map:
                algo_anomalies_map[algo] = field_anomaly_map
                total_batch_num += len(field_anomaly_map)
        logger.debug("%s: anomaly length in %s task", total_anomalies_num, self._taskID)
        return algo_anomalies_map, total_batch_num

    @staticmethod
    def get_freq(infer_freq):
        freq = get_freq(infer_freq)
        if freq == 0:
            return 60 * 1000
        else:
            return freq

    def __repr__(self):
        return "this is {}".format(self.__class__.__name__)
