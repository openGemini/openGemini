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
"""Continuous-valued Sundial generation independent of HF text generation."""

from numbers import Integral

import torch
from transformers import DynamicCache


def restore_sundial_buffers(model):
    """Rebuild rotary buffers after Transformers 5's meta-device loading.

    Sundial's rotary buffers are nonpersistent, so they are absent from the
    checkpoint. Its legacy _init_weights does not initialize these buffers
    when the current loader materializes them from the meta device.
    """
    for layer in model.model.layers:
        rotary = layer.self_attn.rotary_emb
        layer.self_attn.rotary_emb = type(rotary)(
            rotary.dim,
            max_position_embeddings=rotary.max_position_embeddings,
            base=rotary.base,
            device=layer.self_attn.q_proj.weight.device,
        )


class _SundialCache(DynamicCache):
    """Bridge Sundial's attention layers to the current unbounded KV cache."""

    def get_usable_length(self, new_seq_length, layer_idx=0):
        # Sundial calls this before appending keys/values to each layer. Unlike
        # a sliding-window cache, DynamicCache retains every previous patch.
        return self.get_seq_length(layer_idx)


@torch.no_grad()
def generate_sundial(model, inputs, max_new_tokens, num_samples=10):
    """Return [batch, samples, horizon] forecasts using Sundial's sampling rule.

    The published model overrides Transformers 4's private _greedy_search,
    which Transformers 5 no longer calls. Drive its forward method directly:
    normalize once, sample each forecast block, feed back the sample median,
    and restore the original scale. Cache lengths count patches, not values.
    """
    if inputs.ndim != 2 or inputs.shape[1] == 0:
        raise ValueError("Sundial inputs must have shape [batch, nonempty lookback]")
    if not isinstance(max_new_tokens, Integral) or max_new_tokens <= 0:
        raise ValueError("Sundial horizon must be a positive integer")
    if not isinstance(num_samples, Integral) or num_samples <= 0:
        raise ValueError("Sundial sample count must be a positive integer")

    inputs = inputs.to(model.device)
    means = inputs.mean(dim=-1, keepdim=True)
    stdev = inputs.std(dim=-1, keepdim=True, unbiased=False) + 1e-5
    current = (inputs - means) / stdev
    cache = _SundialCache()
    remaining = max_new_tokens
    blocks = []
    while remaining > 0:
        outputs = model(
            input_ids=current,
            past_key_values=cache,
            use_cache=True,
            return_dict=True,
            output_attentions=False,
            output_hidden_states=False,
            revin=False,
            num_samples=num_samples,
            max_output_length=remaining,
        )
        predictions = outputs.logits[..., :remaining]
        if predictions.shape[-1] == 0:
            raise RuntimeError("Sundial returned an empty forecast block")
        blocks.append(predictions)
        remaining -= predictions.shape[-1]
        cache = outputs.past_key_values
        current = predictions.median(dim=1).values

    forecasts = torch.cat(blocks, dim=-1)
    return forecasts * stdev.unsqueeze(1) + means.unsqueeze(1)
