# -*- coding: utf-8 -*-
"""
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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
import json
import time
import uuid

import numpy as np
import pandas as pd
from pandas import DataFrame
from statsmodels.tsa.seasonal import STL

hyper_params = {
    'std_window': 15,
    'sensitivity': 3,
    "resid_weight": 2,
    "trend_weight": 3,
    "all_weight": 3,
    "min_th": None,
    'anomaly_type': 'both'  ## 枚举值，upper表示突增异常，lower表示突降异常，both表示突增突降异常
}


def find_indices(sequence, weight, is_up=True):
    """识别STL周期项和残差项中的较大/较小值下标"""
    # 计算均值和标准差
    mean = np.mean(sequence)
    std = np.std(sequence)

    # 找出满足条件的元素的下标
    if is_up:
        threshold = mean + weight * std
        indices = [i for i, x in enumerate(sequence) if x > threshold]
    else:
        threshold = mean - weight * std
        indices = [i for i, x in enumerate(sequence) if x < threshold]
    return indices


def find_indices2(sequence, sequence2, weight, is_up=True):
    """识别STL周期项和残差项中的较大/较小值下标"""
    # 计算均值和标准差
    mean = np.mean(sequence)
    std = np.std(sequence)
    # 找出满足条件的元素的下标
    if is_up:
        threshold = mean + weight * std
        indices = [len(sequence) + i for i, x in enumerate(sequence2) if x > threshold]
    else:
        threshold = mean - weight * std
        indices = [len(sequence) + i for i, x in enumerate(sequence2) if x < threshold]
    return indices


def filter_indices_by_score(up_indices, up_scores, down_indices, down_scores, percent):
    max_up = max(up_scores) if up_scores else float('-inf')
    max_down = max(down_scores) if down_scores else float('-inf')
    overall_max = max(max_up, max_down)
    # 2. 计算阈值
    threshold = overall_max * percent
    # 3. 筛选上标索引
    filtered_up = [idx for idx, score in zip(up_indices, up_scores) if score >= threshold]
    # 4. 筛选下标索引
    filtered_down = [idx for idx, score in zip(down_indices, down_scores) if score >= threshold]
    return filtered_up, filtered_down


def stl_sliding_window(df, metric, params):
    start_index = max(int(len(df) / 2 if len(df) > 60 else len(df) - 30), 0)
    stl = STL(df[metric], period=3, robust=True)
    result = stl.fit()

    # 获取不同指标的异常点索引
    resid_up_index = find_indices(result.resid, params['resid_weight'], is_up=True)
    resid_down_index = find_indices(result.resid, params['resid_weight'], is_up=False)
    trend_up_index = find_indices(result.trend, params['trend_weight'], is_up=True)
    trend_down_index = find_indices(result.trend, params['trend_weight'], is_up=False)
    all_up_index = find_indices2(df[metric][:start_index], df[metric][start_index:], params['all_weight'], is_up=True)
    all_down_index = find_indices2(df[metric][:start_index], df[metric][start_index:], params['all_weight'],
                                   is_up=False)

    up_index = list(set(resid_up_index) | set(trend_up_index) | set(all_up_index))
    down_index = list(set(resid_down_index) | set(trend_down_index) | set(all_down_index))
    up_index.sort()
    down_index.sort()

    anomaly_up_indices = []
    anomaly_down_indices = []
    anomaly_up_score = []
    anomaly_down_score = []

    def process_anomalies(indices, is_upper=True):
        processed_indices = []
        scores = []

        base_mean = np.mean(df[metric].iloc[0:start_index])
        base_std = np.std(df[metric].iloc[0:start_index])

        for tmp_index in indices:
            if tmp_index < start_index:
                continue
            if params.get('min_th', None) and df[metric].iloc[tmp_index] < params['min_th']:
                continue

            subset = df.iloc[tmp_index - params['std_window']:tmp_index]
            filtered_subset = subset[~subset.index.isin(up_index) & ~subset.index.isin(down_index)]

            mean = base_mean
            std = base_std

            if not filtered_subset.empty:
                mean = np.mean(filtered_subset[metric])
                std = np.std(filtered_subset[metric])
                std = max(std, mean * 0.1)

            threshold = mean + params['sensitivity'] * std if is_upper else mean - params['sensitivity'] * std
            value = df[metric].iloc[tmp_index]

            if (is_upper and value > threshold) or (not is_upper and value < threshold):
                processed_indices.append(tmp_index)
                score = (value - mean) / (std + 1) if is_upper else (mean - value) / (std + 1)
                scores.append(score)

        return processed_indices, scores

    if params['anomaly_type'] in ['both', 'upper']:
        anomaly_up_indices, anomaly_up_score = process_anomalies(up_index, is_upper=True)

    if params['anomaly_type'] in ['both', 'lower']:
        anomaly_down_indices, anomaly_down_score = process_anomalies(down_index, is_upper=False)

    # 合并结果
    anomaly_up_indices, anomaly_down_indices = filter_indices_by_score(
        anomaly_up_indices, anomaly_up_score,
        anomaly_down_indices, anomaly_down_score,
        min(params['sensitivity'] / 15.0, 0.9)
    )

    anomaly_indices = sorted(anomaly_up_indices + anomaly_down_indices)
    return anomaly_indices, anomaly_up_indices, anomaly_down_indices


def sigma_sliding_window(df, metric, params):
    anomaly_up_score = []
    anomaly_down_score = []
    anomaly_up_indices = []
    anomaly_down_indices = []
    anomaly_indices = []
    mean = np.mean(df[metric].iloc[0:0 + params['std_window']])
    std = np.std(df[metric].iloc[0:0 + params['std_window']])
    for tmp_index in range(len(df) - params['std_window']):
        if params.get('min_th', None):
            if df[metric].iloc[tmp_index + params['std_window']] < params['min_th']:
                continue
        subset = df.iloc[tmp_index:tmp_index + params['std_window']]
        filtered_subset = subset[~subset.index.isin(anomaly_up_indices) & ~subset.index.isin(anomaly_down_indices)]
        if not filtered_subset.empty:
            mean = np.mean(filtered_subset[metric])
            std = np.std(filtered_subset[metric])
            std = mean * 0.1 if std < mean * 0.1 else std
        if params['anomaly_type'] == 'both' or params['anomaly_type'] == 'upper':
            threshold = mean + params['sensitivity'] * std
            if df[metric].iloc[tmp_index + params['std_window']] > threshold:
                anomaly_up_indices.append(tmp_index + params['std_window'])
                anomaly_up_score.append((df[metric].iloc[tmp_index + params['std_window']] - mean) / (std + 1))
        if params['anomaly_type'] == 'both' or params['anomaly_type'] == 'lower':
            threshold = mean - params['sensitivity'] * std
            if df[metric].iloc[tmp_index + params['std_window']] < threshold:
                anomaly_down_indices.append(tmp_index + params['std_window'])
                anomaly_down_score.append((mean - df[metric].iloc[tmp_index + params['std_window']]) / (std + 1))
    anomaly_up_indices, anomaly_down_indices = filter_indices_by_score(anomaly_up_indices, anomaly_up_score,
                                                                       anomaly_down_indices, anomaly_down_score,
                                                                       min(params['sensitivity'] / 15.0, 0.9))
    anomaly_indices.extend(anomaly_up_indices)
    anomaly_indices.extend(anomaly_down_indices)
    anomaly_indices.sort()
    return anomaly_indices, anomaly_up_indices, anomaly_down_indices


def single_metric_anomaly_stl(entity_id: str, metric: str, df: DataFrame, params):
    anomaly_result = pd.DataFrame()
    if len(df) <= params['std_window']:
        return anomaly_result

    if len(df) < 30 + params['std_window']:
        anomaly_indices, anomaly_up_indices, anomaly_down_indices = sigma_sliding_window(df, metric, params)
    else:
        anomaly_indices, anomaly_up_indices, anomaly_down_indices = stl_sliding_window(df, metric, params)

    if anomaly_indices:
        annotation_data = {
            "timestamps": [],
            "values": [],
            "anomaly_index": [],
            "anomaly_type": []
        }
        for index in anomaly_indices:
            annotation_data["timestamps"].append(df.iloc[index]['ts'])
            annotation_data["values"].append(df.iloc[index][metric])
            annotation_data["anomaly_index"].append(index)
            if index in anomaly_up_indices:
                annotation_data['anomaly_type'].append('upper')
            elif index in anomaly_down_indices:
                annotation_data['anomaly_type'].append('lower')
        annotation_data = json.dumps(annotation_data)
        anomaly_result = pd.DataFrame({
            'timestamp': int(time.time() * 1000),
            'entity_id': entity_id,
            'id': 'anomaly-' + str(uuid.uuid4()),
            'type': 'anomaly',
            'name': metric + '_anomaly',
            'level': 3,
            'rule_id': 'single_metric_anomaly_STL',
            'annotations': annotation_data
        }, index=[0])

    return anomaly_result
