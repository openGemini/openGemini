# -- coding: utf-8 --
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
from ast import arg
import os
import logging
import numpy as np
import torch
from pytorch_lightning import Trainer
from pytorch_lightning.callbacks import EarlyStopping, ModelCheckpoint
from .model import MyATF
from pytorch_lightning.loggers import TensorBoardLogger
from pandas import DataFrame
import pandas as pd
import argparse
import time
import json
from collections import defaultdict
import random

logger = TensorBoardLogger(name="logs", save_dir="./")


def set_seed(seed=42):
    """Set all random seeds to ensure reproducible results."""
    random.seed(seed)
    np.random.seed(seed)

    torch.manual_seed(seed)

    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)  # if use multiple GPU

    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

    os.environ['PYTHONHASHSEED'] = str(seed)


def model_infer(df_origin: DataFrame, model, hparams):
    model.hp = hparams
    df_origin = df_origin.bfill()
    df_origin = df_origin.fillna(0)
    input_data = np.asarray(df_origin["value"].values, dtype=np.float32)
    mean = input_data.mean()
    std = input_data.std()
    input_data = (input_data - mean) / std
    input_data1 = input_data[-hparams.window_size - hparams.horizon:]
    refer_data = input_data[-hparams.horizon - hparams.period:-hparams.period]
    input_data2 = torch.cat((torch.from_numpy(input_data1), torch.from_numpy(refer_data)))
    input_data2 = input_data2.view(1, 1, -1)

    model.eval()
    with torch.no_grad():
        out = model(input_data2, "test")
    out = np.array(out[1])
    out = out * std + mean
    result = pd.DataFrame()
    result["recon_v"] = out.reshape(-1)
    return result


def model_save(trainer, path):
    trainer.save_checkpoint(path)


def model_load(path):
    model = MyATF.load_from_checkpoint(path)
    return model


def udf_model_infer(df: DataFrame, hparams, model):
    parser = MyATF.add_model_specific_args()
    hyperparams = parser.parse_args(hparams)
    if hyperparams.window_size <= 0:
        raise ValueError("window_size must be larger than 0")
    if hyperparams.horizon <= 0:
        raise ValueError("horizon must be larger than 0")
    if hyperparams.period <= 0:
        raise ValueError("period must be larger than 0")
    start = time.time()
    result = model_infer(df, model, hyperparams)
    # rename col recon_v to value to align output columns
    result = result.rename(columns={"recon_v": "value"})
    return result
