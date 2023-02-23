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

from castor.utils import const as castor_con

INFO = "info"
DATA = "data"
DATA_VALUE = "data"
FIELD_IDS = "field_ids"

ALGORITHM = b"_algo"
CONFIG_FILE = b"_cfg"
PROCESS_TYPE = b"_processType"
DATA_ID = b"_dataID"
TASK_ID = b"_taskID"
OUTPUT_INFO = b"_outputInfo"
QUERY_MODE = b"_queryMode"
CONN_ID = b"_connID"
MSG_TYPE = b"_msgType"
RULE_ID = b"ruleID"

ERR_INFO = b"_errInfo"
ANOMALY_NUM = b"_anomalyNum"

DATA_TIME = "time"
FIELD_NAME = b"field_keys"
SERIES = b"series"
WRITE_TYPE = b"type"

INIT_CONFIG_FILE = "cfg"
INIT_CONFIG_STRING = "cfg_str"
INIT_TASK_ID = "taskID"


DEFAULT_OUTPUT_INFO = [
    castor_con.GENERATE_TIME,
    castor_con.FREQ,
    castor_con.SCORE,
    castor_con.ORIGIN,
    castor_con.LEVEL,
]

SEC_TO_NS = 1e9
