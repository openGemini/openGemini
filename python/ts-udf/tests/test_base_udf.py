import logging

import pytest
from pandas.testing import assert_frame_equal
import pandas as pd
from castor.utils import const as castor_con
from castor.utils import logger as llogger

from server.base import BaseUDF

llogger.basic_config(level="DEBUG")
llogger.logger.addHandler(logging.StreamHandler())


class TestPostProcess:
    @pytest.fixture()
    def fixture_start(self):
        columns = ["field1", "field2"]
        self.algo1_level = pd.DataFrame(
            [[0, -1], [-1, 1]], columns=columns, dtype=float
        )
        self.algo1_label = pd.DataFrame(
            [[True, False], [False, False]], columns=columns
        )
        self.algo2_level = pd.DataFrame([[1, 0], [0, -1]], columns=columns, dtype=float)
        self.algo2_label = pd.DataFrame([[True, True], [True, False]], columns=columns)
        self.algo3_level = pd.DataFrame(
            [[-1, -1], [-1, -1]], columns=columns, dtype=float
        )
        self.algo3_label = pd.DataFrame(
            [[False, False], [False, False]], columns=columns
        )

        base_udf = BaseUDF(None, None, None, None, None)
        base_udf._output_info = [castor_con.SCORE, castor_con.LEVEL]
        base_udf._algorithm = ["algo1", "algo2", "algo3"]
        self.base_udf = base_udf

    @pytest.mark.usefixtures("fixture_start")
    def test_post_process(self):

        detect_results = [
            {
                castor_con.LEVEL: self.algo1_level,
                castor_con.LABEL: self.algo1_label,
            },
            {
                castor_con.LEVEL: self.algo2_level,
                castor_con.LABEL: self.algo2_label,
            },
            {
                castor_con.LEVEL: self.algo3_level,
                castor_con.LABEL: self.algo3_label,
            },
        ]

        (
            algo_anomalies_map,
            total_batch_num,
        ) = self.base_udf._post_process(detect_results, 4)
        assert len(algo_anomalies_map) == 2
        assert total_batch_num == 3
        info_columns = [castor_con.LEVEL, castor_con.SCORE]
        result = {
            "algo1": {
                "field1": pd.DataFrame([[0.0, -1.0]], columns=info_columns, dtype=float)
            },
            "algo2": {
                "field1": pd.DataFrame(
                    [[1.0, -1.0], [0, -1.0]], columns=info_columns, dtype=float
                ),
                "field2": pd.DataFrame(
                    [[0.0, -1.0]], columns=info_columns, dtype=float
                ),
            },
        }
        for algo, field_value in result.items():
            for field, value in field_value.items():
                assert_frame_equal(value, algo_anomalies_map[algo][field])

    @pytest.mark.usefixtures("fixture_start")
    def test_post_process_without_level_and_score(self):
        detect_results = [
            {
                castor_con.LABEL: self.algo1_label,
            },
            {
                castor_con.LABEL: self.algo2_label,
            },
            {
                castor_con.LABEL: self.algo3_label,
            },
        ]

        (
            algo_anomalies_map,
            total_batch_num,
        ) = self.base_udf._post_process(detect_results, 4)
        assert len(algo_anomalies_map) == 2
        assert total_batch_num == 3
        info_columns = [castor_con.SCORE, castor_con.LEVEL]
        result = {
            "algo1": {
                "field1": pd.DataFrame([[-1.0, 0.0]], columns=info_columns, dtype=float)
            },
            "algo2": {
                "field1": pd.DataFrame(
                    [[-1.0, 0.0], [-1.0, 0.0]], columns=info_columns, dtype=float
                ),
                "field2": pd.DataFrame(
                    [[-1.0, 0.0]], columns=info_columns, dtype=float
                ),
            },
        }
        for algo, field_value in result.items():
            for field, value in field_value.items():
                assert_frame_equal(value, algo_anomalies_map[algo][field])
