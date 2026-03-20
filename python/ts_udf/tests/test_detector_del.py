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

from pathlib import Path
import os
import json
import logging

import pytest
import pandas as pd
import shutil
from castor.detector.pipeline_detector import PipelineDetector
from castor.utils import logger as llogger
from castor.utils import globalSymbol
from castor.detector.cache.cache import KeyCache, CacheSet
from castor.utils import const as castor_con
from castor.utils.base_functions import load_params_from_yaml
from castor.detector.cache.organize_cache import clear_cache
from server import const as con
from server.fit import FitUDF

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
TESTS_PATH = os.path.split(CURRENT_PATH)[0]
DATA_PATH = os.path.join(TESTS_PATH, "../openGemini-castor/tests/data")
with open(os.path.join(TESTS_PATH, "../openGemini-castor/tests/conf", "test.json")) as f:
    tests_params = json.load(f)
llogger.basic_config(level="DEBUG")
llogger.logger.addHandler(logging.StreamHandler())


@pytest.fixture(params=tests_params.get("DEL_MODEL"))
def params(request):
    return request.param


@pytest.mark.usefixtures("env_ready")
class TestModelCacheDel:
    @pytest.fixture()
    def env_ready(self, params):
        self.tear_up(params)
        yield
        self.tear_down()

    def tear_up(self, params):
        self.config = os.path.join(
            Path(__file__).parents[1], "../openGemini-castor/conf", params.get("conf_file")
        )
        self.params = load_params_from_yaml(config_file=self.config)
        self.data_file = os.path.join(DATA_PATH, params.get("data_file"))
        df = pd.read_csv(self.data_file, index_col=con.DATA_TIME, parse_dates=True)

        df.index = df.index.tz_localize(None)
        self.index = df.index.drop_duplicates()
        self.namespace = [
            "Service.CloudVR",
            "SYS.DAYU",
            "SYS.DMS",
        ]  # list(set(df['namespace']))

        self.col_int_map = {col: (i + 1000) for i, col in enumerate(self.namespace)}
        self.int_col_map = {v: k for k, v in self.col_int_map.items()}
        self.df = df.to_frame() if isinstance(df, pd.Series) else df
        self.len_df = self.df.shape[0]
        self.log_file = params.get("log_file")
        self.model_file = os.path.join(CURRENT_PATH, params.get("model_file"))
        self.model_size = params.get("model_size")
        self._version = "0.0.2"
        self.one_period_len = 54
        folder = os.path.exists(self.model_file)
        if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
            os.makedirs(self.model_file)
        self.algo = ""

    def tear_down(self):
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        if os.path.exists(self.model_file):
            shutil.rmtree(self.model_file)
        clear_cache()

    @pytest.mark.important
    @pytest.mark.run(order=1)
    def test_pipeline_detector_del_model_should_remove_cloudvr_model_file_and_remove_dayu_and_cloudvr_cache(
        self,
    ):
        detect_class = PipelineDetector
        self.algo = [castor_con.BATCH_DIFFERENTIATE_AD, castor_con.DIFFERENTIATE_AD]
        self.fit_detect_task(detect_class)
        cache = CacheSet()

        assert len(cache.get_cache(castor_con.SUPPRESS_CACHE)) == 2
        assert len(cache.get_cache(castor_con.SIGMA_EWM_THRESHOLD_CACHE)) == 1
        assert len(cache.get_cache(castor_con.STREAM_FILTER_CACHE)) == 1

    def fit_detect_once(self, t_start: int, period_len: int, detect_class):
        self.fit_once(t_start, period_len, detect_class)
        self.detect_once(t_start, period_len, detect_class)

    def fit_detect_task(self, detect_class) -> None:
        symbol = globalSymbol.Symbol()
        symbol.set_symbol("del_cache", True)
        cache = CacheSet()
        key_cache = KeyCache()
        self.fit_detect_once(self.one_period_len * 2, self.one_period_len, detect_class)
        symbol = globalSymbol.Symbol()
        symbol.set_symbol("del_cache", True)
        self.namespace = ["SYS.DAYU", "SYS.DMS"]
        self.fit_detect_once(self.one_period_len * 3, self.one_period_len, detect_class)
        symbol = globalSymbol.Symbol()
        symbol.set_symbol("del_cache", True)
        self.namespace = ["SYS.DMS"]
        self.fit_detect_once(self.one_period_len * 4, self.one_period_len, detect_class)
        symbol = globalSymbol.Symbol()
        symbol.set_symbol("del_cache", True)
        self.fit_detect_once(self.one_period_len * 5, self.one_period_len, detect_class)

    def fit_once(self, index_start: int, period_len: int, detector) -> None:
        fit_handler = FitUDF("", "", "", "", "")
        fit_handler._taskID = "test"
        symbol = globalSymbol.Symbol()
        symbol.set_symbol("del_model", True)
        fit_handler._model_path = self.model_file
        fit_handler._remove_expiring_model_with_symbol(ttl=0.65)
        for col in self.namespace:
            data = self.df[self.df["namespace"] == col][["cu_cpu.mean_value"]]
            data.columns = [self.col_int_map.get(col)]

            model = detector(params=self.params, algo=self.algo)
            model.fit(
                data.loc[
                    self.index[index_start - period_len * 2] : self.index[
                        index_start - 1
                    ]
                ]
            )
            file = os.path.join(self.model_file, col + "_" + self._version)
            model.dump_model_file(file)

    def detect_once(self, index_start: int, period_len: int, detector) -> None:
        t_start = (
            self.index[index_start - 100] if (index_start - 100) >= 0 else self.index[0]
        )
        t_end = (
            self.index[index_start + period_len - 1]
            if (index_start + period_len) < len(self.index)
            else self.index[len(self.index) - 1]
        )
        for col in self.namespace:
            data = self.df[self.df["namespace"] == col][["cu_cpu.mean_value"]]

            data.columns = [self.col_int_map.get(col)]
            model = detector(params=self.params, algo=self.algo)

            file = os.path.join(
                self.model_file, str(self.col_int_map.get(col)) + "_" + self._version
            )
            model.load_model_file(file)

            test_x = data.loc[t_start:t_end]
            model.run(test_x)
