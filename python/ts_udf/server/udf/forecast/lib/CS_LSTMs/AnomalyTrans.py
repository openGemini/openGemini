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
import torch

from torch import nn
import math
from .time_series_decomp import Series_decomp


class AnomalyTransformer(nn.Module):
    def __init__(
            self,
            hp,
    ):
        super(AnomalyTransformer, self).__init__()
        self.hp = hp
        self.td_linear = Series_decomp(wavelet='db4', level = math.floor(math.log2(self.hp.window_size)/2))
        self.dropout = nn.Dropout(p=0.1)

        self.trend_attn = nn.ModuleList([
            nn.MultiheadAttention(
                embed_dim=self.hp.model_dim + self.hp.model_dim // 4,
                num_heads=8,
                dropout=0.1,
                bias=True,
            ) for _ in range(3)
        ])

        self.long_embed = nn.Linear(self.hp.horizon, self.hp.input_dim)
        self.short_embed = nn.Linear(self.hp.horizon, self.hp.stride)
        self.freq_embed = nn.Linear(self.hp.stride + self.hp.input_dim + 4, self.hp.model_dim//4)
        self.time_embed = nn.Linear(self.hp.horizon, self.hp.model_dim)
        self.trend_norm = nn.LayerNorm(self.hp.model_dim + self.hp.model_dim//4)


        self.trend_layer = nn.Sequential(
            nn.Linear(self.hp.model_dim + self.hp.model_dim//4+ self.hp.horizon, self.hp.model_dim),
            nn.GELU(),
            nn.Linear(self.hp.model_dim, self.hp.horizon)
        )

        self.noise_linear = nn.Linear(self.hp.window_size, self.hp.horizon)


    def get_input(self, input):
        """
        Further divide the window for subsequent input into attention processing.
        """
        # Finally, reset to zero and do not participate in the prediction.
        input_copy = input.clone()
        trend_input, noise_input = self.td_linear(input_copy)

        return trend_input, noise_input

    def encode(self, input):
        required_dim = 2 * self.hp.horizon
        if input.shape[2] < required_dim:
            raise ValueError(
                f"input illegal: dimension {input.shape[2]} mismatch"
            )
        trend_input, noise_input = self.get_input(input[:,:,:-2*self.hp.horizon])

        trend_input = trend_input.unfold(dimension=2, size=self.hp.horizon, step=self.hp.horizon).squeeze(1)
        trend_freq = self.freq_embed(self.get_freq(trend_input))
        trend_time = self.time_embed(trend_input)
        trend_input = torch.cat((trend_time, trend_freq),dim=-1)
        for i in range(len(self.trend_attn)):
            out = self.trend_attn[i](trend_input, trend_input, trend_input)[0]
            trend_input = trend_input + self.dropout(out)
            trend_input = self.trend_norm(trend_input)
        trend_input = torch.cat((trend_input[:,-1,:].unsqueeze(1), input[:,:,-self.hp.horizon:]), dim=-1)
        trend_mu = self.trend_layer(trend_input)

        noise_mu = self.noise_linear(noise_input)

        return trend_mu, noise_mu

    def forward(self, input, mode):
        """
        Forward Propagation
        :param input: Input Tensor，shape = (batch, 1, window_size)
        """
        if mode == "train" or mode == "valid":
            loss = self.loss_func(input)
            return loss
        else:
            return self.MCMC2(input)

    def loss_func(self, input):
        trend_mu, noise_mu = self.encode(input)
        input = input.squeeze(1)
        trend_mu = trend_mu.squeeze(1)
        noise_mu = noise_mu.squeeze(1)

        recon_x = trend_mu + noise_mu

        mse = torch.mean(
            torch.mean((input[:,-2*self.hp.horizon:-self.hp.horizon] - recon_x) ** 2, dim=-1),
            dim=0
        )

        if torch.isinf(mse):
            raise ValueError("MSE is infinite")
        return mse

    def MCMC2(self, input):
        trend_mu, noise_mu = self.encode(input)
        recon_x = trend_mu + noise_mu
        loss = (input[:,:,-2*self.hp.horizon:-self.hp.horizon] - recon_x) ** 2
        return loss, recon_x


    def get_freq(self, input):
        input_copy = input.clone()
        long_freq = self.long_embed(input_copy)
        short_freq = self.short_embed(input_copy)
        long_freq = torch.fft.rfft(long_freq, dim=-1)
        long_freq = torch.cat((long_freq.real, long_freq.imag), dim=-1)
        short_freq = torch.fft.rfft(short_freq, dim=-1)
        short_freq = torch.cat((short_freq.real, short_freq.imag), dim=-1)
        return torch.cat((long_freq, short_freq), dim=-1)



    def tv_loss(self, trend_mu):
        diff = torch.abs(trend_mu[:,1:] - trend_mu[:,:-1])
        return torch.mean(torch.std(diff))