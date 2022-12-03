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

# UDF Handler to Fit

from __future__ import absolute_import
from __future__ import division
import os
import threading
import time
import traceback
from typing import List

import pandas as pd
import pyarrow as pa
from castor.utils.logger import logger
from castor.detector.pipeline_detector import PipelineDetector
from castor.utils import globalSymbol, const as castor_con
from castor.utils.common import ava
from castor.utils.const import MODEL_EXPIRING_TIME
from castor.utils.exceptions import ValueMissError, ValueNotEnoughError

from .base import BaseUDF
from . import const as con


class FitUDF(BaseUDF):
    def __init__(self, data_dir, model_config_dir, version, meta_data, all_params):
        """Constructor"""

        super().__init__(data_dir, model_config_dir, version, meta_data, all_params)
        self._algorithm = None
        self._type = "_fit"
        self.duration = 0
        self.number_fit = 0

    def data(self, data_req: dict, cache, hook) -> List[pa.RecordBatch]:
        """fit data"""
        super().batch_init(data_req.get(con.INFO), cache, hook)

        if not os.path.exists(self._model_path):
            os.makedirs(self._model_path)

        self._remove_old_model(self._model_path)

        start_time = time.time()
        df = data_req.get(con.DATA)
        batch_record_list = [self.pandas_to_arrow()]
        try:
            params = self._get_mapped_algorithm_params(df.columns)
        except FileNotFoundError as error:
            logger.error(error)
            meta = batch_record_list[0].schema.metadata
            meta[con.ERR_INFO] = (
                "Occur error when fit series: missing %s" % error
            ).encode()
            batch_record_list[0] = batch_record_list[0].replace_schema_metadata(meta)
            self._clear_data()
            return batch_record_list

        fit_results = self._fit(df, params)
        end_time = time.time()

        self.duration = ava(self.duration, end_time - start_time)
        self.number_fit += 1
        stats = globalSymbol.Stats()
        stats.set_stats(self.duration, self.number_fit)

        try:
            self.add_tag("modelVersion", fit_results["model_version"])
            self.add_tag("modelName", fit_results["model_name"])
            self.add_tag("modelStatus", fit_results["model_status"])
            self.add_field("modelSize", fit_results["model_size"], pa.int64())
            self.add_field("message", fit_results["message"], pa.utf8())
            self.add_field("fitDuration", fit_results["fit_duration"], pa.float64())
            self.add_tag("taskName", fit_results["task_name"])
            self.add_field("type", "modelStat", pa.utf8())
            batch_record_list = [self.pandas_to_arrow()]
        except Exception as error:
            logger.error(traceback.format_exc())
            logger.error("%s get model information raise error", error)
            meta = batch_record_list[0].schema.metadata
            meta[con.ERR_INFO] = (
                "%s get model information raise error" % error
            ).encode()
            batch_record_list[0] = batch_record_list[0].replace_schema_metadata(meta)
        finally:
            self._clear_data()
            symbol = globalSymbol.Symbol()
            symbol.set_symbol("run", False)
        return batch_record_list

    def _fit(self, df: pd.DataFrame, params: dict) -> dict:
        # remove all expiring model
        remove_thread = threading.Thread(
            target=self._remove_expiring_model_with_symbol, args=(MODEL_EXPIRING_TIME,)
        )
        remove_thread.start()

        # the results are initialized
        fit_results = {
            "model_version": self._version,
            "model_name": self.series_key,
            "model_status": "success",
            "model_size": 0,
            "message": "ok",
            "fit_duration": 0,
            "task_name": self._fit_taskID,
            "model_file": "",
        }

        try:
            self._check_algorithm()
            if set(self._algorithm) <= castor_con.DETECTOR_FOR_FIT:
                model = PipelineDetector(algo=self._algorithm, params=params)
            else:
                msg = "%s not support, please use a valid algorithm."
                logger.error(msg, self._algorithm)
                raise TypeError(msg % self._algorithm)

            # model fit
            _fit_start = time.time()

            model.fit(df)

            fit_time = time.time() - _fit_start

            # get the fitted time of model
            fit_results["fit_duration"] = round(fit_time, 2)

            # store fitted model
            model_file = os.path.join(
                self._model_path, self.series_key + "_" + self._version
            )
            model.dump_model_file(model_file)

            # get the model size
            fit_results["model_size"] = (
                os.path.getsize(model_file) // 1024 if os.path.exists(model_file) else 0
            )

        except Warning:
            pass
        except (ValueNotEnoughError, ValueMissError) as error:
            logger.info(
                "%s: fit %s catch exception: %s", self._taskID, self.series_key, error
            )
        except Exception as error:
            logger.error(traceback.format_exc())
            logger.error(
                "%s: fit %s catch exception: %s", self._taskID, self.series_key, error
            )
            fit_results["model_status"] = "failure"
            fit_results["message"] = error.__str__()

        return fit_results

    def _remove_old_model(self, model_path):
        try:
            g = os.walk(model_path)
            for path, _, file_list in g:
                for file_name in file_list:
                    if file_name.split("_")[-1] != self._version:
                        file_path = os.path.join(path, file_name)
                        os.remove(file_path)
                        logger.info("remove old model file: %s", file_path)
        except Exception as error:
            logger.error(traceback.format_exc())
            logger.error(
                "%s remove old model file catch exception: %s", self._taskID, error
            )

    def _remove_expiring_model(self, ttl):
        try:
            g = os.walk(self._model_path)
            for path, _, file_list in g:
                for file_name in file_list:
                    file_path = os.path.join(path, file_name)
                    if time.time() - os.path.getmtime(file_path) > ttl:
                        os.remove(file_path)
                        logger.info("remove expiring model file: %s", file_path)
        except Exception as error:
            logger.error(traceback.format_exc())
            logger.error(
                "%s remove expiring model file catch exception: %s", self._taskID, error
            )

    def _remove_expiring_model_with_symbol(self, ttl):
        symbol = globalSymbol.Symbol()
        symbol_del = symbol.get_symbol("del_model")
        if symbol_del:
            self._remove_expiring_model(ttl)
            symbol.set_symbol("del_model", False)
