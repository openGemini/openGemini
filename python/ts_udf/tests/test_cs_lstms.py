"""
Copyright 2026 Huawei Cloud Computing Technologies Co., Ltd.

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
"""Regression tests for CS-LSTM with Lightning 2.x and weights-only loading.

Run: PYTHONPATH=python python -m unittest discover -s python/ts_udf/tests -p test_cs_lstms.py
"""

import os
import pickle
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import torch
from pytorch_lightning import Trainer
from torch.utils.data import DataLoader, TensorDataset

from ts_udf.server.udf.forecast.lib.CS_LSTMs.interface import (
    model_infer, model_load, model_save,
)
from ts_udf.server.udf.forecast.lib.CS_LSTMs.model import MyATF


class CSLSTMCompatibilityTest(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        torch.manual_seed(981)
        self.hp = MyATF.add_model_specific_args().parse_args([
            "--window_size", "64", "--horizon", "20", "--period", "64",
            "--model_dim", "32",
        ])
        self.model = MyATF(self.hp)
        self.data = pd.DataFrame({"value": np.sin(np.arange(128) / 10)})
        x = torch.randn(4, 1, 104)
        labels = torch.zeros(4, 104)
        self.loader = DataLoader(TensorDataset(x, labels, labels), batch_size=2)

    def trainer(self):
        return Trainer(
            accelerator="cpu", devices=1, logger=False,
            enable_checkpointing=False, enable_progress_bar=False,
            enable_model_summary=False, max_epochs=1,
            limit_train_batches=1, limit_val_batches=1, num_sanity_val_steps=0,
        )

    def test_legacy_checkpoint_load_and_infer(self):
        # Mirror Lightning 1.7's checkpoint format and argparse hyperparameters.
        checkpoint = {
            "pytorch-lightning_version": "1.7.7",
            "state_dict": self.model.state_dict(),
            "hyper_parameters": {"hparams": self.hp},
            "global_step": 0,
            "epoch": 0,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.ckpt"
            torch.save(checkpoint, path)
            loaded = model_load(path)
        expected = model_infer(self.data, self.model, self.hp)
        actual = model_infer(self.data, loaded, loaded.hp)
        np.testing.assert_allclose(actual, expected)
        self.assertEqual(actual.shape, (20, 1))
        self.assertTrue(np.isfinite(actual.to_numpy()).all())

    def test_checkpoint_rejects_unrelated_pickle_globals(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unsupported.ckpt"
            torch.save({"unexpected": SimpleNamespace(value=1)}, path)
            with self.assertRaises(pickle.UnpicklingError):
                model_load(path)

    def test_train_save_load_and_infer(self):
        trainer = self.trainer()
        trainer.fit(self.model, self.loader, self.loader)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trained.ckpt"
            model_save(trainer, path)
            loaded = model_load(path)
        expected = model_infer(self.data, self.model, self.hp)
        actual = model_infer(self.data, loaded, loaded.hp)
        np.testing.assert_allclose(actual, expected)

    def test_test_epoch_outputs_and_repeated_runs(self):
        trainer = self.trainer()
        original = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                Path("npy").mkdir()
                Path("csv/W+A").mkdir(parents=True)
                self.model.eval()
                with torch.no_grad():
                    outputs = [self.model(batch[0], "test") for batch in self.loader]
                    scores = torch.cat([output[0] for output in outputs])
                    predictions = torch.cat([output[1] for output in outputs])
                expected = pd.DataFrame({
                    "y": np.zeros(80),
                    "x": self.loader.dataset.tensors[0][:, :, -40:-20].numpy().reshape(-1),
                    "recon_x": predictions.numpy().reshape(-1),
                })
                for run in range(2):
                    trainer.test(self.model, self.loader)
                    np.testing.assert_allclose(
                        np.load("npy/score.npy"), scores.numpy().reshape(-1), atol=1e-6,
                    )
                    np.testing.assert_array_equal(np.load("npy/label.npy"), expected["y"])
                    actual = pd.read_csv("csv/W+A/result.csv")
                    pd.testing.assert_frame_equal(
                        actual, pd.concat([expected] * (run + 1), ignore_index=True),
                        check_dtype=False, atol=1e-6,
                    )
            finally:
                os.chdir(original)


if __name__ == "__main__":
    unittest.main()
