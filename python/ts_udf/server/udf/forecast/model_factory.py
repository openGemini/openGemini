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
import os.path
from abc import abstractmethod
from enum import Enum
from typing import Dict

import pandas as pd
import torch
from transformers import AutoModelForCausalLM

from ts_udf.server.server import get_config
from ts_udf.server.udf.forecast.lib.CS_LSTMs.interface import model_load, udf_model_infer

MODEL_SUFFIX = ".ckpt"


class ModelType(Enum):
    BUILT_IN = "BUILT-IN"
    USER_DEFINED = "USER-DEFINED"
    LTSM = "LTSM"  # large time series model


class Model:
    required_params = ["window_size", "horizon", "period"]

    def __init__(self, model_id: str, model_type: ModelType):
        self.id = model_id
        self.type = model_type
        self._model = None

    @abstractmethod
    def infer(self, data, hyper_params) -> pd.DataFrame:
        pass


class BuiltInModel(Model):
    def __init__(self, model_id):
        super().__init__(model_id, ModelType.BUILT_IN)
        self.path = self.get_model_path()

    def get_model_path(self):
        return os.path.join(get_config("model_store_dir"), self.id + MODEL_SUFFIX)

    def infer(self, data, params):
        if self._model is None:
            self._model = model_load(self.path)
        hyper_params = []
        for option in BuiltInModel.required_params:
            if option in params:
                hyper_params.append(f"--{option}")
                hyper_params.append(f"{params[option]}")
        return udf_model_infer(data, hyper_params, self._model)


class UDFModel(Model):
    def __init__(self, model_id: str, model_path: str):
        super().__init__(model_id, ModelType.USER_DEFINED)
        self.path = model_path

    def infer(self, data, params):
        pass


class LTSMModel(Model):
    def __init__(self, model_id, model_path: str):
        super().__init__(model_id, ModelType.LTSM)
        self.path = model_path

    def infer(self, data, params):
        if self._model is None:
            self._model = AutoModelForCausalLM.from_pretrained(self.path, trust_remote_code=True)
        batch_size, lookback_length = 1, params["window_size"]
        forecast_length = params["horizon"]
        num_samples = 10
        if "samples_num" in params:
            num_samples = params["samples_num"]
        lookback = torch.tensor(data["value"][:lookback_length]).unsqueeze(0).float()
        output = self._model.generate(lookback, max_new_tokens=forecast_length, num_samples=num_samples) # generate num_samples probable predictions
        out = output[0].mean(dim=0)
        result = pd.DataFrame(out.numpy(), columns=["value"])
        return result


class ModelFactory:
    def __init__(self):
        self._model_map: Dict[str, Model] = {}
        self.built_in_model_path = os.path.join("")
        self._register_built_in_models()

    def _register_built_in_models(self):
        self._model_map["CS-LTSMs"] = BuiltInModel(model_id="AIOPS_1440_120_V11")
        self._model_map["sundial"] = LTSMModel(model_id="sundial", model_path=os.path.join(get_config("model_store_dir"), "sundial-base-128m"))

    def find_model(self, model_id: str) -> Model:
        if model_id not in self._model_map:
            raise KeyError(f"model {model_id} not found")
        return self._model_map[model_id]
