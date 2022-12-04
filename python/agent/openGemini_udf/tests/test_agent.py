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

from threading import Thread
import unittest
from multiprocessing import get_context, Process
from multiprocessing.queues import Queue
from openGemini_udf.agent import Flusher, worker, Agent, fnv1a_hash, Handler
import pyarrow as pa
import time
import os
import socket
import contextlib
import string
import random


class MirrorHandler(Handler):
    def __init__(self, data_dir, model_config_dir, version, meta_data):
        self.ip = "127.0.0.1"
        self.port = 0

    def batch_data(self, data_req, cache, hook):
        return data_req


def generate_record_batch(num_points):
    data = [
        pa.array([i + 1 for i in range(num_points)]),
        pa.array([0.1 * (i + 1) for i in range(num_points)]),
    ]
    batch = pa.record_batch(data, names=["f1", "f2"])
    batch = batch.replace_schema_metadata(
        metadata={"t1": "tv1", "t2": "tv2", "_msgType": "data"}
    )
    return batch


class PyWorkers:
    def __init__(self, num_workers, handler):
        self.results = Queue(ctx=get_context())
        self.workers = []
        self.tasks = []
        for i in range(num_workers):
            task_queue = Queue(ctx=get_context(), maxsize=os.cpu_count())
            p = Process(
                target=worker,
                args=(300000, 3600, num_workers, handler, None, None, "1.0.0", task_queue, self.results),
            )
            self.tasks.append(task_queue)
            self.workers.append(p)

    def start(self):
        for worker in self.workers:
            worker.start()

    def stop(self):
        for worker in self.workers:
            worker.kill()
            worker.join()


class TestFlusher(unittest.TestCase):
    def setUp(self):
        self.tasks = Queue(ctx=get_context())
        self.worker = Process(target=self.work)
        self.worker.daemon = True
        self.worker.start()
        self.flusher = Flusher(self.tasks, None, 3, 0.01)
        self.flusher.run()

    def tearDown(self):
        self.worker.kill()
        self.worker.join()
        self.flusher.stop()

    def testPutBatch(self):
        for i in range(10):
            batch = generate_record_batch(num_points=100)
            self.flusher.put_batch(batch)
        time.sleep(0.1)

    def work(self):
        while True:
            try:
                data, _ = self.tasks.get(block=True, timeout=0.1)
                self.assertEqual(len(data), 1)
            except:
                break


class TestWorker(unittest.TestCase):
    def setUp(self):
        self.workers = PyWorkers(1, MirrorHandler)
        self.workers.start()

    def tearDown(self):
        self.workers.stop()

    def testBatchMirrorWorker(self):
        for i in range(10):
            batch = generate_record_batch(num_points=100)
            self.workers.tasks[0].put((batch, None))
            results = self.workers.results.get()
            self.assertEqual(results, batch)


class TestAgent(unittest.TestCase):
    def setUp(self):
        self._socket_path = "test.sock"
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.connector = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        self.client_thread = Thread(target=self.client)
        self.client_thread.setDaemon(True)
        self.client_thread.start()

        self.tasks = Queue(ctx=get_context())
        self.results = Queue(ctx=get_context())
        self.flusher = Flusher(self.tasks, None, 3, 0.01)
        self.flusher.run()

        self.listener.bind(self._socket_path)
        self.listener.listen()
        self.conn, _ = self.listener.accept()

        self.accept_thread = Thread(target=self.accept)
        self.accept_thread.setDaemon(True)

    def tearDown(self):
        self.flusher.stop()
        self.connector.close()
        self.listener.close()
        self.conn.close()
        if isinstance(self._socket_path, str):
            with contextlib.suppress(FileNotFoundError):
                os.remove(self._socket_path)

    def testAgentStartWait(self):
        self.accept_thread.start()

    def testAgentReadWrite(self):
        for i in range(10):
            batch = generate_record_batch(num_points=100)
            self.flusher.put_batch(batch)
        while True:
            try:
                data, _ = self.tasks.get(block=True, timeout=0.1)
                self.assertEqual(len(data), 1)
            except:
                break

    def accept(self):
        agent = Agent(self.conn, self.conn, None, 0, self.results, [self.flusher])
        agent.start()
        agent.wait()

    def client(self):
        while True:
            try:
                self.connector.connect(self._socket_path)
            except:
                time.sleep(0.1)
                continue


class TestHash(unittest.TestCase):
    def setUp(self):
        base = string.ascii_letters + string.digits
        self.strings = [
            random.choice(base).encode(encoding="utf-8") for _ in range(100000)
        ]
        self.length = 8
        self.results = {i: 0 for i in range(self.length)}
        self.random_str = random.choice(base).encode(encoding="utf-8")

    def tearDown(self):
        pass

    def testHashUniform(self):
        for s in self.strings:
            self.results[fnv1a_hash(s, self.length)] += 1
        print("hash results:", self.results)

    def testHashUnique(self):
        index = fnv1a_hash(self.random_str, self.length)
        for _ in range(100000):
            self.assertEqual(index, fnv1a_hash(self.random_str, self.length))


if __name__ == "__main__":
    unittest.main()
