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

import time
from threading import Thread, Lock
from typing import Tuple, Optional, Callable
import hashlib


class MetaDataObj:
    def __init__(self, data, obj_id, prefix, postfix):
        self._last_visit_time = time.time()
        self._data = data
        self._id = obj_id
        self._prefix = prefix
        self._postfix = postfix

    @property
    def data_id(self):
        self._last_visit_time = time.time()
        return self._id

    @property
    def data(self):
        self._last_visit_time = time.time()
        return self._data

    @property
    def prefix(self):
        self._last_visit_time = time.time()
        return self._prefix

    @property
    def postfix(self):
        self._last_visit_time = time.time()
        return self._postfix

    @property
    def last_visit_time(self):
        return self._last_visit_time


class MetaData:
    def __init__(self, size: int, timeout_sec: float):
        if size <= 0:
            raise ValueError("size should > 0")
        self.size = size
        self.prefix_len = MetaData._set_prefix_len(size)
        self.prefix_mask = 10**self.prefix_len
        self.filler_plus_prefix_mask = self.prefix_mask * 10
        self.bucket_lock = [Lock() for _ in range(size)]
        self.bucket = [list() for _ in range(size)]
        self.timeout_sec = timeout_sec
        self.auto_clean_thread = Thread(
            target=self._ticker, args=(self._clean_timeout_obj,)
        )
        self.auto_clean_thread.setDaemon(True)
        self.auto_clean_thread.start()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["bucket_lock"]
        del state["auto_clean_thread"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.bucket_lock = [Lock() for _ in range(self.size)]
        self.auto_clean_thread = Thread(
            target=self._ticker, args=(self._clean_timeout_obj,)
        )
        self.auto_clean_thread.setDaemon(True)
        self.auto_clean_thread.start()

    @staticmethod
    def _set_prefix_len(i: int) -> int:
        i -= 1  # because prefix num start from 0
        bit_len = 1
        while i // 10:
            bit_len += 1
            i //= 10
        return bit_len

    def _ticker(self, func: Callable):
        if self.timeout_sec == 0:
            return
        now = time.time()
        while True:
            if time.time() - now > self.timeout_sec:
                func()
                now = time.time()
            time.sleep(1)

    def _clean_timeout_obj(self):
        now = time.time()
        for bucket, lock in zip(self.bucket, self.bucket_lock):
            lock.acquire()
            for i, item in enumerate(bucket):
                if item is None:
                    continue
                if now - item.last_visit_time > self.timeout_sec:
                    bucket[i] = None
            lock.release()

    def _get_obj_id(self, prefix: int, postfix: int) -> int:
        # fill 1 bit in front of prefix code in case bit length not equal to bucket size
        ret = self.prefix_mask + prefix
        ret *= 10
        tmp = postfix
        while tmp // 10:
            ret *= 10
            tmp //= 10
        ret += postfix
        return ret

    def _get_prefix_and_postfix(self, i: int) -> (int, int):
        tmp = i
        length = -1  # minus 1 for filler in front of prefix
        while tmp:
            tmp //= 10
            length += 1
        if length <= self.prefix_len:
            return None, None

        postfix_reverse = []
        while i // self.filler_plus_prefix_mask:
            postfix_reverse.append(i % 10)
            i //= 10
        postfix = 0
        for n in reversed(postfix_reverse):
            postfix = postfix * 10 + n

        prefix = i % self.prefix_mask
        return prefix, postfix

    def get_meta_data_by_id(
        self, i: int
    ) -> Optional[Tuple[str, Tuple[Tuple[str, str], ...], str]]:
        prefix, postfix = self._get_prefix_and_postfix(i)
        if prefix is None:
            return None
        if prefix >= len(self.bucket):
            return None
        self.bucket_lock[prefix].acquire()
        bucket = self.bucket[prefix]
        if postfix >= len(bucket):
            return None
        ret = bucket[postfix]
        self.bucket_lock[prefix].release()
        if ret is not None:
            return ret.data
        return ret

    def get_id_by_meta_data(
        self, data: Tuple[bytes, Tuple[Tuple[bytes, bytes], ...], bytes]
    ) -> Optional[int]:
        prefix = md5_hash(data, len(self.bucket))
        ret = None
        self.bucket_lock[prefix].acquire()
        for item in self.bucket[prefix]:
            if item.data == data:
                ret = item.data_id
                break
        self.bucket_lock[prefix].release()
        return ret

    def register_meta_data(
        self, data: Tuple[bytes, Tuple[Tuple[bytes, bytes], ...], bytes]
    ) -> int:
        prefix = md5_hash(data, len(self.bucket))
        self.bucket_lock[prefix].acquire()
        bucket = self.bucket[prefix]
        obj_id = None
        is_registered = False
        for i, item in enumerate(bucket):
            if item is not None and item.data == data:
                self.bucket_lock[prefix].release()
                return item.data_id
        for i, item in enumerate(bucket):
            if item is None:
                obj_id = self._get_obj_id(prefix, i)
                bucket[i] = MetaDataObj(data, obj_id, prefix, i)
                is_registered = True
                break
        if not is_registered:
            postfix = len(bucket)
            obj_id = self._get_obj_id(prefix, postfix)
            bucket.append(MetaDataObj(data, obj_id, prefix, postfix))
        self.bucket_lock[prefix].release()
        return obj_id

    def delete_meta_data_by_id(self, i: int):
        prefix, postfix = self._get_prefix_and_postfix(i)
        if prefix >= len(self.bucket):
            raise ValueError("id out of range")
        self.bucket_lock[prefix].acquire()
        bucket = self.bucket[prefix]
        if postfix >= len(bucket):
            raise ValueError("id out of range")
        bucket[postfix] = None
        self.bucket_lock[prefix].release()

    def delete_meta_data_by_data(
        self, data: Tuple[bytes, Tuple[Tuple[bytes, bytes], ...], bytes]
    ):
        prefix = md5_hash(data, len(self.bucket))
        self.bucket_lock[prefix].acquire()
        bucket = self.bucket[prefix]
        for i, item in enumerate(bucket):
            if item.data == data:
                bucket[i] = None
                break
        self.bucket_lock[prefix].release()


def md5_hash(
    data: Tuple[bytes, Tuple[Tuple[bytes, bytes], ...], bytes], modulo: int
) -> int:
    m = hashlib.md5()
    m.update(data[0])
    for t in data[1]:
        m.update(t[0])
        m.update(t[1])
    m.update(data[2])
    return int(m.hexdigest(), 16) % modulo
