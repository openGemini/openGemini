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

from __future__ import absolute_import
import os
from os.path import dirname, abspath
import logging

import pytest
import numpy as np
import pyarrow as pa
import pandas as pd
from castor.utils import const as castor_con
from castor.utils import logger as llogger
from openGemini_udf.metadata import MetaData
from castor.detector.cache.organize_cache import clear_cache

from server import const as con
from server.handler import CastorHandler
from server.base import algo_alarm_map

llogger.basic_config(level="DEBUG")
llogger.logger.addHandler(logging.StreamHandler())

fit_detect_algo_list = [
    castor_con.BATCH_DIFFERENTIATE_AD,
    castor_con.INCREMENTAL_AD,
    castor_con.THRESHOLD_AD,
    castor_con.VALUE_CHANGE_AD,
]

diff_detect_algo_list = [
    castor_con.DIFFERENTIATE_AD,
    castor_con.BATCH_DIFFERENTIATE_AD,
]
detect_algo_list = [
    castor_con.BATCH_DIFFERENTIATE_AD,
    castor_con.INCREMENTAL_AD,
    castor_con.THRESHOLD_AD,
    castor_con.VALUE_CHANGE_AD,
]
special_detect_algo_list = [
    castor_con.DIFFERENTIATE_AD,
    ",".join([castor_con.DIFFERENTIATE_AD, castor_con.INCREMENTAL_AD]),
]
has_output_info = [True, False]


class TestHandler:
    @pytest.fixture()
    def fixture_with_special_conf(self):
        self.conf_path = os.path.realpath(os.path.join(self.cur_path + "/conf"))
        self.metadata.update({con.CONFIG_FILE: "batch_detect"})
        self.handler = CastorHandler(
            self.cur_path, self.conf_path, self.version, self.META_DATA
        )

    @pytest.fixture()
    def fixture_with_demo_conf(self):
        self.conf_path = os.path.realpath(
            os.path.join(self.cur_path + "/../../openGemini-castor/conf")
        )
        self.metadata.update({con.CONFIG_FILE: "detect_base"})
        self.handler = CastorHandler(
            self.cur_path, self.conf_path, self.version, self.META_DATA
        )

    @pytest.fixture()
    def fixture_start(self):
        self.cur_path = os.path.split(os.path.realpath(__file__))[0]
        self.parent_path = dirname(dirname(abspath(__file__)))

        self.log_file = "test.log"
        self.version = "0.1.0"
        self.models_path = os.path.join(self.cur_path + "/models/")
        self.data_length = 90
        self.dim = 6
        self.META_DATA = MetaData(100, 1)
        tags = {"agentSN": "11"}
        self.metadata = {
            con.MSG_TYPE: "data",
            con.TASK_ID: "memory",
            con.DATA_ID: "xxx",
            con.OUTPUT_INFO: castor_con.SCORE,
            con.CONN_ID: "12",
        }
        self.metadata.update(tags)

        yield
        clear_cache()

    @staticmethod
    def _schema_extraction_from_dataframe(df: pd.DataFrame):
        mapping = {"str": pa.utf8(), "int": pa.int64(), "float": pa.float64()}
        schema_list = []
        for column in list(df.columns):
            for key in mapping.keys():
                if key in type(df.loc[df.index[0], column]).__name__:
                    schema_list.append((column, mapping.get(key)))
        return schema_list

    def _convert_data_from_pd_to_pa(
        self, data_df, metadata, detect_period=180, step=60, split=False
    ):
        if not split:
            detect_period = len(data_df)
            step = len(data_df)

        # received data
        data_list = []

        data_df.index = data_df.index.values.astype(np.int64)
        data_df.index.name = con.DATA_TIME
        data_df.reset_index(inplace=True)

        schema = pa.schema(self._schema_extraction_from_dataframe(data_df), metadata)

        for i in range(0, len(data_df), step):
            data = data_df.iloc[i : i + detect_period, :]
            data_list.append([pa.record_batch(data.values.T.tolist(), schema=schema)])
        return data_list

    def detect_data_generation(
        self, anomaly_start: int = 20, anomaly_end: int = 25, algo: str = ""
    ) -> pd.DataFrame:
        Gx = np.ones((self.data_length, 1))
        if algo == castor_con.INCREMENTAL_AD:
            Gx[anomaly_start : anomaly_end + 40, 0] = [
                (i * 1 + 10) for i in range(20, 65)
            ]
        if algo in [
            castor_con.THRESHOLD_AD,
            castor_con.VALUE_CHANGE_AD,
            castor_con.BATCH_DIFFERENTIATE_AD,
        ]:
            Gx[anomaly_start + 40 : anomaly_end + 45, 0] = [
                0,
                1,
                1,
                34,
                11,
                110,
                3,
                70,
                111,
                111,
            ]
        data_df = self._convert_np_to_pd(Gx)
        return data_df

    @staticmethod
    def _convert_np_to_pd(data: np.ndarray) -> pd.DataFrame:
        data_df = pd.DataFrame(data)
        data_df.columns = [str(value) + "df" for value in data_df]
        data_df.index = pd.date_range(
            start="2020-01-01 00:00:00", periods=len(data_df), freq="15S"
        )
        return data_df

    def get_actual(self, data_list):
        algo_actual_map = {}
        for record_batch in data_list:
            actual_list = self.handler.batch_data(record_batch)
            for actual in actual_list:
                self.update_actual(actual, algo_actual_map)

        for algo, actual_list in algo_actual_map.items():
            algo_actual_map[algo] = pd.concat(actual_list, axis=0)

        return algo_actual_map

    @staticmethod
    def update_actual(actual, algo_actual_map):
        metadata = actual.schema.metadata

        actual_pd = actual.to_pandas()
        if not actual_pd.empty:
            algo = metadata.get(castor_con.TYPE.encode()).decode()
            if algo in algo_actual_map:
                algo_actual_map[algo].append(actual_pd)
            else:
                algo_actual_map[algo] = [actual_pd]

    @pytest.mark.parametrize("algorithm", detect_algo_list)
    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_detect4detect_should_return_anomaly(self, algorithm):
        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update({con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_detect"})
        self.data_list = self._convert_data_from_pd_to_pa(
            data_df, split=True, metadata=self.metadata
        )
        actual = self.get_actual(self.data_list)
        algo_name = algo_alarm_map.get(algorithm, algorithm)
        tmp = actual[algo_name]
        tmp[con.DATA_TIME] = pd.to_datetime(
            tmp[con.DATA_TIME] // int(con.SEC_TO_NS), unit="s"
        )
        llogger.logger.info("actual: %s", tmp)
        assert len(actual[algo_name]) > 0

    @pytest.mark.parametrize("algorithm", fit_detect_algo_list)
    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_fit_detect_should_return_all_labels(self, algorithm):

        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update(
            {con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_fit_detect"}
        )

        data_list = self._convert_data_from_pd_to_pa(
            data_df, split=True, metadata=self.metadata
        )
        llogger.logger.info("metadata: %s", self.metadata)
        actual = self.get_actual(data_list)
        algo_name = algo_alarm_map.get(algorithm, algorithm)
        actual = actual.get(algo_name)

        if algorithm not in ["PcaAD"]:
            assert len(actual) > 0
            assert castor_con.SCORE in actual.columns

    def _read_data(self):
        data_df = pd.read_csv(self.data_path, parse_dates=True, index_col=0)
        data_df.index = pd.date_range(
            start="2020-01-01 05:00:00", periods=len(data_df), freq="15S"
        )
        return data_df

    @pytest.mark.usefixtures("fixture_with_special_conf")
    @pytest.mark.usefixtures("fixture_start")
    @pytest.mark.parametrize("algorithm", special_detect_algo_list)
    def test_detect_incremental_and_differentiate_ad_return_special_anomaly(
        self, algorithm
    ):
        self.data_path = os.path.join(self.cur_path, "data/fluctuate_and_increase.dat")
        df_data = self._read_data()
        self.metadata.update({con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_detect"})
        self.metadata.pop(con.OUTPUT_INFO)
        data_list = self._convert_data_from_pd_to_pa(
            df_data, metadata=self.metadata, split=True
        )
        actual = self.get_actual(data_list)
        llogger.logger.info("actual: %s", actual)

        results = {
            castor_con.DIFFERENTIATE_AD: pd.to_datetime(
                [
                    "2020-01-01 06:30:45",
                    "2020-01-01 07:09:15",
                    "2020-01-01 07:23:30",
                    "2020-01-01 09:14:30",
                    "2020-01-01 09:28:30",
                    "2020-01-01 09:43:30",
                ]
            ),
            castor_con.INCREMENTAL_AD: pd.to_datetime(
                ["2020-01-01 10:21:00", "2020-01-01 10:37:15", "2020-01-01 10:55:30"]
            ),
        }
        for algo, result in results.items():
            if algo in algorithm:
                algo_name = algo_alarm_map.get(algo, algo)
                value = actual.get(algo_name)
                llogger.logger.info("value, %s, %s", value, result)
                actual_time = pd.to_datetime(
                    value[con.DATA_TIME] // int(con.SEC_TO_NS), unit="s"
                )

                assert all(actual_time == result)

    @pytest.mark.parametrize("algorithm", castor_con.TRAINABLE_AD)
    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_fit4detect_should_save_model(self, algorithm):
        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update({con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_fit"})
        self.metadata.update({con.TASK_ID: "test_fit_%s" % algorithm})
        self.data_list = self._convert_data_from_pd_to_pa(
            data_df, split=True, metadata=self.metadata
        )

        self.get_actual(self.data_list)
        assert os.path.isfile(
            os.path.realpath(
                os.path.join(
                    self.cur_path + "/models/" + self.metadata.get(con.TASK_ID),
                    "agentSN=11_" + self.version,
                )
            )
        )

    @pytest.mark.parametrize("algorithm", diff_detect_algo_list)
    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_fit_detect_should_return_different_anomaly_when_use_batch_or_stream_differentiated(
        self, algorithm
    ):

        data_df = self.detect_data_generation(algo=algorithm)

        self.metadata.update(
            {con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_fit_detect"}
        )
        data_list = self._convert_data_from_pd_to_pa(data_df, metadata=self.metadata)

        for record_batch in data_list:
            results = self.handler.batch_data(record_batch)
            for result in results:
                metadata_result = result.schema.metadata
                llogger.logger.info(metadata_result)
                llogger.logger.info(result.to_pandas())
                if algorithm == "DIFFERENTIATEAD":
                    assert len(metadata_result) == len(self.metadata) - 3
                    if result:
                        assert False
                else:
                    assert len(metadata_result) == len(self.metadata) + 1
                    if not result:
                        assert False

                assert metadata_result.get(b"agentSN") is not None

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_fit_detect_should_return_err_info_when_input_unsupported_algorithm(self):
        algorithm = "TestAD"

        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update(
            {con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_fit_detect"}
        )
        data_list = self._convert_data_from_pd_to_pa(
            data_df, split=True, metadata=self.metadata
        )

        for record_batch in data_list:
            result_list = self.handler.batch_data(record_batch)
            for result in result_list:
                metadata_result = result.schema.metadata
                print(metadata_result)
                print(self.metadata)
                print(result.to_pandas())
                assert metadata_result.get(b"agentSN") is not None
                assert len(metadata_result) == len(self.metadata) - 2

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_detect_should_return_err_info_when_missing_necessary_info(self):
        algorithm = castor_con.DIFFERENTIATE_AD

        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update({con.PROCESS_TYPE: "_detect"})
        data_list = self._convert_data_from_pd_to_pa(
            data_df, split=True, metadata=self.metadata
        )

        for record_batch in data_list:
            result_list = self.handler.batch_data(record_batch)
            for result in result_list:
                metadata_result = result.schema.metadata
                print(metadata_result)
                assert metadata_result.get(con.TASK_ID) is not None
                assert len(metadata_result) == 10
                assert (
                    metadata_result.get(con.ERR_INFO)
                    .decode()
                    .startswith("Occur error when process metadata")
                )

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_detect_should_return_err_info_when_input_invalid_process_type(self):
        algorithm = castor_con.DIFFERENTIATE_AD

        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update({con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_f_detect"})
        data_list = self._convert_data_from_pd_to_pa(
            data_df, split=True, metadata=self.metadata
        )

        for record_batch in data_list:
            result_list = self.handler.batch_data(record_batch)
            for result in result_list:
                metadata_result = result.schema.metadata
                print(metadata_result)
                assert metadata_result.get(con.TASK_ID) is not None
                assert len(metadata_result) == 6
                assert (
                    metadata_result.get(con.ERR_INFO)
                    .decode()
                    .startswith("Occur error when process metadata:")
                )

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_detect_should_return_anomaly_when_detect_without_init(self):
        algorithm = castor_con.THRESHOLD_AD
        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update({con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_detect"})
        data_list = self._convert_data_from_pd_to_pa(data_df, metadata=self.metadata)

        for record_batch in data_list:
            result_list = self.handler.batch_data(record_batch)
            for result in result_list:
                metadata_result = result.schema.metadata
                print(metadata_result)
                assert metadata_result.get(con.TASK_ID) is not None
                assert len(metadata_result) == 10

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_detect_should_return_error_when_data_is_not_enough(self):
        algorithm = castor_con.BATCH_DIFFERENTIATE_AD
        data_df = self.detect_data_generation(algo=algorithm).iloc[1:14, :]
        self.metadata.update({con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_detect"})
        data_list = self._convert_data_from_pd_to_pa(data_df, metadata=self.metadata, split=True, detect_period=7, step=6)

        record_batch = data_list[0]
        result_list = self.handler.batch_data(record_batch)
        for result in result_list:
            metadata_result = result.schema.metadata
            print(metadata_result)
            assert metadata_result.get(con.TASK_ID) is not None
            assert len(metadata_result) == 7
            assert con.ERR_INFO in metadata_result

        record_batch = data_list[1]
        result_list = self.handler.batch_data(record_batch)
        for result in result_list:
            metadata_result = result.schema.metadata
            print(metadata_result)
            assert metadata_result.get(con.TASK_ID) is not None
            assert len(metadata_result) == 6
            assert con.ERR_INFO not in metadata_result

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_detect_should_return_error_when_input_error_config_file(self):
        algorithm = castor_con.BATCH_DIFFERENTIATE_AD
        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update({con.ALGORITHM: algorithm, con.PROCESS_TYPE: "_detect", con.CONFIG_FILE: "detect"})
        data_list = self._convert_data_from_pd_to_pa(data_df, metadata=self.metadata, split=True)

        for record_batch in data_list:
            result_list = self.handler.batch_data(record_batch)
            for result in result_list:
                metadata_result = result.schema.metadata
                print(metadata_result)
                assert metadata_result.get(con.TASK_ID) is not None
                assert len(metadata_result) == 7
                assert con.ERR_INFO in metadata_result

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    @pytest.mark.parametrize("with_output_info", has_output_info)
    def test_output_info(self, with_output_info):
        algorithm = castor_con.THRESHOLD_AD
        data_df = self.detect_data_generation(algo=algorithm)
        self.metadata.update(
            {
                con.ALGORITHM: ",".join(
                    [castor_con.THRESHOLD_AD, castor_con.VALUE_CHANGE_AD]
                ),
                con.PROCESS_TYPE: "_detect",
            }
        )
        if not with_output_info:
            self.metadata.pop(con.OUTPUT_INFO)
        data_list = self._convert_data_from_pd_to_pa(
            data_df, metadata=self.metadata, split=True
        )

        actual = self.get_actual(data_list)
        for algo, value in actual.items():
            if with_output_info:
                assert castor_con.SCORE in value.columns
            else:
                assert castor_con.GENERATE_TIME in value.columns
                assert castor_con.SCORE in value.columns
                assert castor_con.FREQ in value.columns

    @pytest.mark.usefixtures("fixture_with_demo_conf")
    @pytest.mark.usefixtures("fixture_start")
    def test_query_mode(self):
        algorithm = castor_con.VALUE_CHANGE_AD
        data_df = pd.DataFrame(
            [0, 0, 0, 1, 0, 1, 0, 0, 0, 0],
            columns=["field1"],
            index=pd.date_range(start="2022-01", periods=10, freq="1d"),
        )
        self.metadata.update(
            {
                con.ALGORITHM: ",".join([algorithm]),
                con.PROCESS_TYPE: "_detect",
                con.QUERY_MODE: b"0",
            }
        )
        data_list1 = self._convert_data_from_pd_to_pa(
            data_df.iloc[:5, :],
            metadata=self.metadata,
            split=True,
            detect_period=5,
            step=5,
        )
        data_list2 = self._convert_data_from_pd_to_pa(
            data_df.iloc[5:, :],
            metadata=self.metadata,
            split=True,
            detect_period=5,
            step=5,
        )

        actual_list1 = self.handler.batch_data(data_list1[0])
        actual_time = pd.to_datetime(
            actual_list1[0].to_pandas()[con.DATA_TIME] // int(con.SEC_TO_NS), unit="s"
        )
        assert len(actual_time) == 2
        actual_list2 = self.handler.batch_data(data_list2[0])
        actual_time = pd.to_datetime(
            actual_list2[0].to_pandas()[con.DATA_TIME] // int(con.SEC_TO_NS), unit="s"
        )
        assert len(actual_time) == 1
