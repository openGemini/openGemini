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
"""Run with PYTHONPATH=python python -m unittest discover -s python/ts_udf/tests -p test_sundial.py.

Set SUNDIAL_MODEL_DIR to the pinned upstream model source directory to also
exercise real attention, flow sampling, and checkpoint loading.
Use thuml/sundial-base-128m revision 3212e42564493f520593e5414af4367fc4b49226
with huggingface_hub.snapshot_download, allow_patterns=["*.py", "config.json"].
Only small random weights are used; no pretrained checkpoint is downloaded.
"""

import importlib
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import torch
from transformers import AutoConfig, AutoModelForCausalLM

from ts_udf.server.udf.forecast.sundial import _SundialCache, generate_sundial, restore_sundial_buffers


class RecordingModel:
    device = torch.device("cpu")

    def __init__(self):
        self.calls = []

    def __call__(self, input_ids, past_key_values, num_samples, **kwargs):
        self.calls.append((input_ids.clone(), past_key_values, past_key_values.get_usable_length(0)))
        patch_count = (input_ids.shape[1] + 1) // 2
        keys = torch.zeros(input_ids.shape[0], 1, patch_count, 2)
        past_key_values.update(keys, keys, 0)
        logits = torch.arange(num_samples, dtype=input_ids.dtype).view(1, num_samples, 1)
        return SimpleNamespace(
            logits=logits.expand(input_ids.shape[0], num_samples, 4),
            past_key_values=past_key_values,
        )


class SundialGenerationTest(unittest.TestCase):
    def test_cache_tracks_each_layer(self):
        cache = _SundialCache()
        keys = torch.zeros(1, 2, 3, 4)
        self.assertEqual(cache.get_usable_length(7), 0)
        cache.update(keys, keys, 0)
        self.assertEqual(cache.get_usable_length(7, 0), 3)
        self.assertEqual(cache.get_usable_length(7, 1), 0)
        cache.update(keys, keys, 1)
        cache.update(keys, keys, 0)
        self.assertEqual(cache.get_usable_length(7, 0), 6)
        self.assertEqual(cache.get_usable_length(7, 1), 3)

    def test_normalization_sampling_and_cache_reuse(self):
        model = RecordingModel()
        inputs = torch.tensor([[1., 2., 3., 4., 5.], [10., 12., 14., 16., 18.]], requires_grad=True)
        original = inputs.detach().clone()
        output = generate_sundial(model, inputs, max_new_tokens=9, num_samples=3)
        self.assertEqual(tuple(output.shape), (2, 3, 9))
        self.assertFalse(output.requires_grad)
        means = inputs.mean(dim=-1, keepdim=True)
        stdev = inputs.std(dim=-1, keepdim=True, unbiased=False) + 1e-5
        expected = torch.arange(3).view(1, 3, 1) * stdev.unsqueeze(1) + means.unsqueeze(1)
        torch.testing.assert_close(output, expected.expand(2, 3, 9))
        torch.testing.assert_close(model.calls[0][0], (inputs - means) / stdev)
        torch.testing.assert_close(model.calls[1][0], torch.ones(2, 4))
        self.assertEqual([call[2] for call in model.calls], [0, 3, 5])
        self.assertEqual(len({id(call[1]) for call in model.calls}), 1)
        torch.testing.assert_close(inputs, original)

        previous_cache = model.calls[0][1]
        generate_sundial(model, inputs, max_new_tokens=1, num_samples=1)
        self.assertIsNot(model.calls[-1][1], previous_cache)
        self.assertEqual(model.calls[-1][2], 0)

    def test_constant_and_single_value_inputs(self):
        for length in (1, 5):
            with self.subTest(length=length):
                output = generate_sundial(RecordingModel(), torch.full((1, length), 7.), 3, 1)
                torch.testing.assert_close(output, torch.full((1, 1, 3), 7.))

    def test_invalid_inputs(self):
        cases = [
            (torch.ones(4), 3, 1),
            (torch.empty(1, 0), 3, 1),
            (torch.ones(1, 4), 0, 1),
            (torch.ones(1, 4), -1, 1),
            (torch.ones(1, 4), 1.5, 1),
            (torch.ones(1, 4), 3, 0),
            (torch.ones(1, 4), 3, 1.5),
        ]
        for inputs, horizon, samples in cases:
            with self.subTest(shape=inputs.shape, horizon=horizon, samples=samples):
                with self.assertRaises(ValueError):
                    generate_sundial(RecordingModel(), inputs, horizon, samples)


@unittest.skipUnless(os.environ.get("SUNDIAL_MODEL_DIR"), "set SUNDIAL_MODEL_DIR for real model tests")
class SundialModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)
        torch.manual_seed(980)
        config = AutoConfig.from_pretrained(os.environ["SUNDIAL_MODEL_DIR"], trust_remote_code=True)
        config.hidden_size = 32
        config.intermediate_size = 64
        config.num_hidden_layers = 2
        config.num_attention_heads = 4
        config.flow_loss_depth = 1
        config.num_sampling_steps = 2
        config.output_token_lens = [32]
        cls.model = AutoModelForCausalLM.from_config(config, trust_remote_code=True).eval()

    def test_forecast_shapes_and_finite_values(self):
        for lookback, horizon, samples in ((32, 1, 1), (33, 16, 3), (33, 32, 2), (33, 33, 2), (48, 80, 3)):
            with self.subTest(lookback=lookback, horizon=horizon, samples=samples):
                inputs = torch.arange(lookback).float().expand(2, -1)
                output = generate_sundial(self.model, inputs, horizon, samples)
                self.assertEqual(tuple(output.shape), (2, samples, horizon))
                self.assertTrue(torch.isfinite(output).all())
                self.assertFalse(output.requires_grad)

    def test_cached_forecast_matches_uncached_forward(self):
        # Recompute the full context on each step as an independent cache oracle.
        inputs = torch.arange(66).view(2, 33).float()
        means = inputs.mean(-1, keepdim=True)
        stdev = inputs.std(-1, keepdim=True, unbiased=False) + 1e-5
        context = (inputs - means) / stdev
        blocks = []
        remaining = 80
        torch.manual_seed(42)
        with torch.no_grad():
            while remaining:
                predictions = self.model(
                    input_ids=context, use_cache=False, return_dict=True,
                    num_samples=3, max_output_length=remaining, revin=False,
                ).logits[..., :remaining]
                blocks.append(predictions)
                remaining -= predictions.shape[-1]
                context = torch.cat((context, predictions.median(dim=1).values), dim=-1)
        expected = torch.cat(blocks, dim=-1) * stdev.unsqueeze(1) + means.unsqueeze(1)
        torch.manual_seed(42)
        actual = generate_sundial(self.model, inputs, 80, 3)
        torch.testing.assert_close(actual, expected, rtol=1e-5, atol=1e-5)

    def test_local_checkpoint_loading_and_repeat_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            self.model.save_pretrained(directory)
            loaded = AutoModelForCausalLM.from_pretrained(directory, trust_remote_code=True).eval()
            restore_sundial_buffers(loaded)
            inputs = torch.linspace(-1, 1, 33).unsqueeze(0)
            torch.manual_seed(123)
            expected = generate_sundial(self.model, inputs, 65, 2)
            for _ in range(2):
                torch.manual_seed(123)
                actual = generate_sundial(loaded, inputs, 65, 2)
                torch.testing.assert_close(actual, expected)

    def test_ltsm_infer_returns_forecast_dataframe(self):
        # Isolate unrelated server startup and CS-LSTM dependencies; exercise
        # the real LTSM wrapper, Sundial model, and DataFrame conversion.
        dependencies = {
            "ts_udf.server.server": SimpleNamespace(get_config=Mock()),
            "ts_udf.server.udf.forecast.lib.CS_LSTMs.interface": SimpleNamespace(
                model_load=Mock(), udf_model_infer=Mock(),
            ),
        }
        with patch.dict(sys.modules, dependencies):
            factory = importlib.import_module("ts_udf.server.udf.forecast.model_factory")
        data = pd.DataFrame({"value": torch.linspace(-1, 1, 33).numpy()})
        with tempfile.TemporaryDirectory() as directory:
            self.model.save_pretrained(directory)
            wrapper = factory.LTSMModel("sundial", directory)
            result = wrapper.infer(data, {"window_size": 33, "horizon": 65, "samples_num": 3})
            with patch.object(wrapper._model, "generate", side_effect=AssertionError("legacy generation was called")):
                result = wrapper.infer(data, {"window_size": 33, "horizon": 65, "samples_num": 3})
        self.assertEqual(list(result.columns), ["value"])
        self.assertEqual(len(result), 65)
        self.assertTrue(torch.isfinite(torch.from_numpy(result["value"].to_numpy())).all())

        # Other causal models keep their own generate implementation.
        wrapper._model = SimpleNamespace(
            config=SimpleNamespace(model_type="other"),
            generate=Mock(return_value=torch.tensor([[[1., 3.], [3., 5.]]])),
        )
        result = wrapper.infer(data, {"window_size": 33, "horizon": 2})
        self.assertEqual(result["value"].tolist(), [2., 4.])
        self.assertEqual(wrapper._model.generate.call_args.kwargs, {"max_new_tokens": 2, "num_samples": 10})


if __name__ == "__main__":
    unittest.main()
