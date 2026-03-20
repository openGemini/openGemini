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

# UDF Handler to Fit and Detect

from __future__ import absolute_import
from __future__ import division
from typing import List

import pandas as pd
from castor.utils.logger import logger
from castor.detector.pipeline_detector import PipelineDetector
from castor.utils import const as con
from castor.utils.common import TimeSeriesType

from .detect import DetectorUDF


class FitDetectorUDF(DetectorUDF):
    """The different of fitDetectorHandler and DetectorHandler is execution frequency and run process.
    The execution frequency of FitDetectorHandler is once, and run fit and detect process.
    """

    def __init__(self, data_dir, model_config_dir, version, meta_data, all_params):
        """Constructor"""

        super().__init__(data_dir, model_config_dir, version, meta_data, all_params)

        self._type = "fit_detect"

    def _load_detector(self, params: dict) -> PipelineDetector:

        if set(self._algorithm) <= con.DETECTOR_FOR_FIT_DETECT:
            detector = PipelineDetector(algo=self._algorithm, params=params)
        else:
            msg = "%s not support, please use a valid algorithm."
            logger.error(msg, self._algorithm)
            raise TypeError(msg % self._algorithm)

        return detector

    def _detect_core(
        self, hd_detector: PipelineDetector, data: pd.DataFrame
    ) -> List[TimeSeriesType]:
        return hd_detector.fit_run(data)
