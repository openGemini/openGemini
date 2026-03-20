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
import pywt
import numpy as np
import pandas as pd
import torch.nn as nn
import torch
from networkx.algorithms.cuts import normalized_cut_size
from sympy.codegen.fnodes import dimension


class Series_decomp(nn.Module):
    def __init__(self, wavelet='db4', level=None):
        super(Series_decomp,self).__init__()
        self.wavelet = wavelet
        self.level = level

    def forward(self, x):
        signal = x.clone().numpy()
        cA, *details = pywt.wavedec(signal[:,:], self.wavelet, level = self.level)
        for i in range(1,self.level):
            d = details[i]
            sigma = np.median(np.abs(d))/0.6745
            sigma = max(sigma, 1e-7)
            T = sigma * np.sqrt(2*np.log(signal.shape[-1]))
            details[i] = pywt.threshold(details[i],T,mode='soft')
        denoise_signal = torch.Tensor(pywt.waverec([cA]+details, self.wavelet))
        noise = x - denoise_signal
        return denoise_signal, noise






