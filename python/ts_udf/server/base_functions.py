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
from typing import List, Dict, Hashable, Tuple
import copy

import numpy as np
import pandas as pd
from castor.utils import const as con
from castor.utils.common import TimeSeriesType


def filter_and_format(
    time_series: TimeSeriesType, output_info: List[str]
) -> Dict[Hashable, pd.DataFrame]:
    """
    filter with label to get anomalies with information
    concat anomaly information, which in output_info, to get field_anomaly_map
    """
    labels = time_series.get(con.LABEL)
    if labels is None:
        return {}
    labels_with_anomaly, fields = filter_columns(labels)
    indexes = labels.index
    if output_info is None:
        output_info = list(time_series.keys())
    field_info_map = get_field_info_map(time_series, indexes, fields, output_info)
    field_pos_map = dict(zip(fields, range(len(fields))))
    # get map of field and anomaly information. The anomaly information is a DataFrame, whose columns is info keys.
    # filter with label to get anomaly and construct it into DataFrame.
    field_anomaly_map = {}
    for field, info in field_info_map.items():
        field_pos = field_pos_map.get(field)
        is_anomaly = labels_with_anomaly[:, field_pos]
        for k, v in info.items():
            info[k] = v[is_anomaly]
        field_anomaly_map[field] = pd.DataFrame(info, index=indexes[is_anomaly])

    return field_anomaly_map


def get_field_info_map(
    time_series: TimeSeriesType,
    indexes: pd.Index,
    fields: np.ndarray,
    info_keys: List[str],
) -> Dict[Hashable, Dict[str, pd.DataFrame]]:
    """
    get map of field and data information. The data information is a map {info_keys: info_series}
    :return: Dict[Hashable, Dict[str, pd.DataFrame]]
        example:
        {
            'field1':
                {
                    'anomalyScore': series (pd.Series)
                    'originalValue': series (pd.Series)
                    ...
                },
            ...
        }
    """
    field_info_map = {}
    for info_key, value in time_series.items():
        if info_key in info_keys:
            for i, field_value in enumerate(
                value.loc[indexes, fields].values.transpose()
            ):
                field = fields[i]
                update_value = {info_key: field_value}
                update_for_two_level_dict(field_info_map, field, update_value)

    if (con.SCORE in info_keys) and (con.SCORE not in time_series):
        add_info(time_series, con.SCORE, fields, field_info_map, -1)
    if (con.LEVEL in info_keys) and (con.LEVEL not in time_series):
        add_info(time_series, con.LEVEL, fields, field_info_map, 0)

    return field_info_map


def add_info(
    time_series: TimeSeriesType,
    key: str,
    fields: np.ndarray,
    field_info_map: Dict[Hashable, Dict[str, np.ndarray]],
    value: float,
) -> None:
    for field in fields:
        scores = np.ones(len(time_series.get(con.LABEL))) * value
        if field in field_info_map:
            field_info_map[field][key] = scores
        else:
            field_info_map[field] = {key: scores}


def update_for_two_level_dict(
    kv: Dict[str, dict], first_level_key: str, value: dict
) -> None:
    """ "
    update value for two level dictionary, whose element is a dictionary
    """
    if first_level_key not in kv:
        kv[first_level_key] = value
    else:
        kv.get(first_level_key).update(value)


def filter_columns(data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """ "
    filter dataframe columns which have not ture value
    """
    columns = data.columns
    value = data.values
    columns_with_anomaly = np.any(value, axis=0)
    columns_pos = np.array([i for i in range(len(columns))])[columns_with_anomaly]
    columns = columns[columns_pos]
    if not columns.empty:
        value_with_true = value[:, columns_pos]
    else:
        value_with_true = np.array([])
    return value_with_true, columns.values


def get_mapped_params(meta, field_ids, params) -> dict:
    filed_id_str_map = {
        key: meta.get_meta_data_by_id(key)[2]
        for key in field_ids
        if meta.get_meta_data_by_id(key) is not None
    }
    mapped_params = copy.deepcopy(params)
    _map_string_to_filed_id_for_param(
        params, mapped_params, field_ids, filed_id_str_map
    )
    return mapped_params


def _map_string_to_filed_id_for_param(params, new_params, filed_ids, filed_id_str_map):
    for param_key, param in params.items():
        if param_key in con.KV_PARAM_KEY:
            new_params[param_key] = {
                field_id: param.get(filed_id_str_map.get(field_id).decode())
                for field_id in filed_ids
                if param.get(filed_id_str_map.get(field_id).decode()) is not None
            }
        elif isinstance(param, dict):
            _map_string_to_filed_id_for_param(
                param, new_params[param_key], filed_ids, filed_id_str_map
            )
