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
import logging
import os
from pytorch_lightning import LightningModule
from torch.utils.data import DataLoader
from .AnomalyTrans import AnomalyTransformer
import argparse
from torch import optim
from collections import OrderedDict
import torch
import numpy as np
import pandas as pd
from torch.utils.data.distributed import DistributedSampler


class MyATF(LightningModule):
    """Context and Seasonal LSTMs"""

    def __init__(self, hparams):
        super(MyATF, self).__init__()
        self.save_hyperparameters()
        self.hp = hparams
        self.__build_model()

    def __build_model(self):
        self.atf = AnomalyTransformer(self.hp)

    def forward(self, x, mode):
        return self.atf.forward(x, mode)

    def loss(self, x, y_all, z_all, mode="train"):
        loss = self.forward(
            x,
            "train",
        )
        return loss

    def training_step(self, data_batch, batch_idx):
        x, y_all, z_all = data_batch
        loss_val = self.loss(x, y_all, z_all)
        if self.trainer.strategy == "dp":
            loss_val = loss_val.unsqueeze(0)
        self.log("val_loss_train", loss_val, on_step=True, on_epoch=False, logger=True)
        output = OrderedDict(
            {
                "loss": loss_val,
            }
        )
        return output

    def validation_step(self, data_batch, batch_idx):
        x, y_all, z_all = data_batch
        loss_val = self.loss(x, y_all, z_all)
        if self.trainer.strategy == "dp":
            loss_val = loss_val.unsqueeze(0)
        self.log("val_loss_valid", loss_val, on_step=True, on_epoch=True, logger=True)
        output = OrderedDict(
            {
                "loss": loss_val,
            }
        )
        return output

    def test_step(self, data_batch, batch_idx):
        x, y_all, z_all = data_batch
        y = (y_all[:, -self.hp.horizon - 20:-self.hp.horizon]).unsqueeze(1)
        mask = torch.logical_not(torch.logical_or(y_all, z_all))
        with torch.no_grad():
            recon_prob, recon_x = self.forward(x, "test")
        recon_prob = recon_prob[:, :, -20:]
        recon_x = recon_x[:, :, -20:]
        x = x[:, :, -self.hp.horizon - 20:-self.hp.horizon]
        output = OrderedDict(
            {
                "y": y,
                "x": x,
                "recon_prob": recon_prob,
                "recon_x": recon_x,
            }
        )
        return output

    def test_epoch_end(self, outputs):
        y = torch.cat(([x["y"] for x in outputs]), 0)
        x = torch.cat(([x["x"] for x in outputs]), 0)
        recon_prob = torch.cat(([x["recon_prob"] for x in outputs]), 0)
        recon_x = torch.cat(([x["recon_x"] for x in outputs]), 0)
        score = recon_prob.squeeze(1).cpu().numpy().reshape(-1)
        label = y.squeeze(1).cpu().numpy().reshape(-1)
        df = pd.DataFrame()
        df["y"] = y.cpu().numpy().reshape(-1)
        df["x"] = x.cpu().numpy().reshape(-1)
        df["recon_x"] = recon_x.cpu().numpy().reshape(-1)
        np.save("./npy/score.npy", score)
        np.save("./npy/label.npy", label)
        # Assume df is a DataFrame that has already been defined.
        base_dir = os.path.join("csv", "W+A")
        file_path = os.path.join(base_dir, "result.csv")

        if os.path.exists(file_path):
            mode = 'a'
            header = False
        else:
            mode = 'w'
            header = True

        df.to_csv(
            file_path,
            mode=mode,
            index=False,
            header=header
        )

    def configure_optimizers(self):
        optimizer = optim.Adam(self.parameters(), lr=self.hp.learning_rate)
        scheduler = optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=10)
        return [optimizer], [scheduler]

    @staticmethod
    def add_model_specific_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("--tmp_model", default="AIOPS", type=str)
        parser.add_argument("--data_dir", default="./data/WSD_test/", type=str)
        parser.add_argument("--window_size", required=True, type=int)
        parser.add_argument("--horizon", default=1, type=int)
        parser.add_argument("--period", default=1440, type=int)
        parser.add_argument("--model_dim", default=256, type=int)
        parser.add_argument("--dropout_rate", default=0.2, type=float)
        parser.add_argument("--gpu", default=2, type=int)
        parser.add_argument("--use_label", default=1, type=int)
        parser.add_argument("--input_dim", default=20, type=int)
        parser.add_argument("--stride", default=8, type=int)
        parser.add_argument("--sliding_window_size", default=1, type=int)
        parser.add_argument("--batch_size", default=16, type=int)
        parser.add_argument("--learning_rate", default=0.001, type=float)
        parser.add_argument("--max_epoch", default=1, type=int)
        parser.add_argument("--num_workers", default=8, type=int)
        parser.add_argument("--data_pre_mode", default=0, type=int)
        parser.add_argument("--missing_data_rate", default=0.01, type=float)
        parser.add_argument(
            "--ckpt_path", default="./ckpt/WSD_300.ckpt", type=str
        )
        return parser
