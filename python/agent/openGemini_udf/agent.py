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

# Python UDF Agent for openGemini
from __future__ import absolute_import
import collections
import contextlib
import sys
import signal
from abc import ABC, abstractmethod
from threading import Lock, Thread
import threading
import traceback
import socket
import os
import stat
import pyarrow as pa
import logging
from .metadata import MetaData
from multiprocessing import Process, get_context, Lock, Value
from multiprocessing.queues import Queue

# Setup default in/out io
defaultIn = sys.stdin.buffer
defaultOut = sys.stdout.buffer

logger = logging.getLogger()


class Handler(ABC):
    @abstractmethod
    def batch_data(self, data_req_list, cache, hook):
        pass


class Flusher:
    def __init__(self, tasks, _dict, batch_size, timeout):
        self._tasks = tasks
        self._dict = _dict
        self._batch_size = batch_size
        self._timeout = timeout
        self._queue = Queue(ctx=get_context())
        self._buffer = []
        self._stopped = False
        self._get_thread = None

    def run(self):
        self._start()
        self._get_thread = Thread(target=self._get)
        self._get_thread.setDaemon(True)
        self._get_thread.start()

    def _get(self):
        while True:
            try:
                data = self._queue.get(block=True, timeout=self._timeout)
                if data == "stop":
                    self._flush_batch()
                    return
                else:
                    self._buffer.append(data)
                if self._check_flush():
                    self._flush_batch()
            except Exception:
                if self._queue.empty():
                    if len(self._buffer) > 0:
                        self._flush_batch()

    def _flush_batch(self):
        if len(self._buffer) != 0:
            self._tasks.put((self._buffer.copy(), self._dict))
            self._buffer.clear()

    def _check_flush(self):
        return len(self._buffer) >= self._batch_size

    def put_batch(self, table):
        self._queue.put(table)

    def _start(self):
        self._buffer.clear()
        self._stopped = False

    def stop(self):
        self._stopped = True
        self._queue.put("stop")
        self._buffer.clear()


class Agent(object):
    def __init__(
        self,
        in_socket=defaultIn,
        out_socket=defaultOut,
        cache=None,
        index=None,
        result=None,
        flusher=None,
    ):
        self._in_socket_file = in_socket.makefile(mode="rwb")
        self._out_socket_file = out_socket.makefile(mode="rwb")
        self._read_thread = None
        self._write_thread = None
        self._write_lock = Lock()
        self._dict = cache
        self._id = index  # connection id
        self._result = result
        self._flusher = flusher
        self.num_workers = len(flusher)

    def start(self):
        self._read_thread = Thread(target=self._read_batch_loop)
        self._read_thread.start()
        self._write_thread = Thread(target=self._write_loop)
        self._write_thread.start()

    def wait(self):
        self._read_thread.join()
        self._write_thread.join()
        self._in_socket_file.close()
        self._out_socket_file.close()

    def write_response(self, table_or_batch, flush=False):
        if table_or_batch is None:
            raise Exception("cannot write None response")

        # Serialize message
        self._write_lock.acquire()
        try:
            with pa.RecordBatchStreamWriter(
                self._out_socket_file, table_or_batch.schema
            ) as writer:
                writer.write(table_or_batch)

            if flush:
                self._out_socket_file.flush()
        finally:
            self._write_lock.release()

    def _write_loop(self):
        while True:
            try:
                result = self._result.get()
                if result == "stop":
                    return
                if result is not None:
                    self.write_response(result, True)

            except Exception as e:
                traceback.print_exc()
                error = "error flush batch: %s" % e
                logger.error(error)
                break

    def _read_batch_loop(self):
        while True:
            msg = "unknown"
            taskID = "unknown"
            metadata = None
            try:
                with pa.ipc.open_stream(self._in_socket_file) as reader:
                    schema = reader.schema
                    metadata = schema.metadata
                    if metadata is None or b"_msgType" not in metadata:
                        continue
                    else:
                        msg = metadata[b"_msgType"]
                    taskID = metadata[b"_taskID"]
                table = reader.read_all()
                hashID = fnv1a_hash(taskID, self.num_workers)
                if msg == b"data":
                    logger.info(
                        "data of task: %s is hashed to worker: %d" % (taskID, hashID)
                    )
                    metadata[b"_connID"] = str(self._id).encode()
                    table = table.replace_schema_metadata(metadata=metadata)
                    self._flusher[hashID].put_batch(table)
                else:
                    logger.error("received unhandled request %s", msg)
            except EOF:
                break
            except pa.lib.ArrowInvalid:
                break
            except TIMEOUT:
                error = "error write_response time out"
                logger.error(error)
                break
            except Exception as e:
                traceback.print_exc()
                error = "error processing request: %s" % e
                logger.error(error)
                break
        self._result.put("stop")
        logger.info("agent read over, id %d", self._id)


# Implemention of fnv-1a (64bit) hash function
# See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function for more details
def fnv1a_hash(task_id: bytes, length: int):
    offset = 0xCBF29CE484222325
    prime = 0x00000100000001B3
    hash_ = offset
    for b in task_id:
        hash_ ^= b
        hash_ *= prime
    return hash_ % length


# Indicates the end of a file/stream has been reached.
class EOF(Exception):
    pass


class TIMEOUT(Exception):
    pass


def worker(
    bucket_size,
    data_lifetime,
    num_workers,
    handler,
    data_dir,
    model_config_dir,
    version,
    data_in,
    data_out,
):
    signal.signal(signal.SIGHUP, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    worker_bucket_size = bucket_size // num_workers
    meta_data = MetaData(worker_bucket_size, data_lifetime)
    cache = dict()
    h = handler(data_dir, model_config_dir, version, meta_data)
    hook = collections.deque()
    for data, _ in iter(data_in.get, "stop"):
        results = None
        try:
            results = h.batch_data(data, cache, hook)
        except Exception as e:
            traceback.print_exc()
            error = "pyworker processes data got exception: %s" % e
            logger.error(error)
        if results is not None:
            data_out.put(results)
    while True:
        try:
            func, d = hook.pop()
            func(d)
        except IndexError:
            break


def worker_daemon(
    worker_id,
    bucket_size,
    data_lifetime,
    process,
    alive,
    handler,
    data_dir,
    model_config_dir,
    version,
    task_queue,
    results,
):
    while True:
        process[worker_id].join()
        logger.debug("worker %d exit" % worker_id)
        try:
            logger.debug("releasing read lock of input queue")
            task_queue._rlock.release()
        except ValueError:
            logger.debug("read lock of input queue has already released")
        if alive.value is False:
            return
        else:
            p = Process(
                target=worker,
                args=(
                    bucket_size,
                    data_lifetime,
                    len(process),
                    handler,
                    data_dir,
                    model_config_dir,
                    version,
                    task_queue,
                    results,
                ),
            )
            p.daemon = True
            process[worker_id] = p
            p.start()
            logger.debug("worker %d restart succeeded" % worker_id)


def start_split(results, qs):
    while True:
        records = results.get()
        for rec in records:
            try:
                index = int(rec.schema.metadata[b"_connID"].decode())
                q = qs.get(index)
                if q is not None:
                    q.put(rec)
                else:
                    logger.error("missing rec queue, id %d", index)
                    continue
            except Exception as e:
                traceback.print_exc()
                error = "split result got exception: %s" % e
                logger.error(error)


class accepter(object):
    def __init__(self, data_dir, model_config_dir, version):
        self._count = 0
        self.data_dir = data_dir
        self.model_config_dir = model_config_dir
        self.version = version
        self._resource_lock = threading.Lock()

    def accept(self, conn, addr, dict, id, result, flushers):
        self._resource_lock.acquire()
        self._count += 1
        count = self._count
        self._resource_lock.release()

        a = Agent(conn, conn, dict, id, result, flushers)

        logger.info("Starting Agent for connection %d", count)
        a.start()
        a.wait()
        logger.info("Agent finished connection %d", count)


def write_pid(file: str, pid: str):
    flags = os.O_WRONLY | os.O_CREAT
    modes = stat.S_IWUSR | stat.S_IRUSR
    with os.fdopen(os.open(file, flags, modes), 'w', encoding="utf-8") as fout:
        fout.write(pid)

class Server(object):
    def __init__(self, handler, params):
        os.setpgid(0, 0)
        self._socket_path = params["path"]
        if isinstance(self._socket_path, str):
            self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(self._socket_path)
        self._accepter = accepter(
            params["data_dir"], params["model_config_dir"], params["version"]
        )
        self._max_connections = params["max_connections"]
        self._count = 0
        self._thread_list = []
        self.dict = None
        self.tasks = []
        self.flushers = []
        self.results = Queue(ctx=get_context())
        self.process = []
        self.qs = dict()

        # pid
        write_pid(params["pidfile"], str(os.getpid()))

        # sharing variable for workers to determine whether server is alive
        self.alive = Value("b", True)
        # terminate handler
        def handle_sigterm(signum, frame):
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(0, signal.SIGHUP)
            self.alive.value = False
            sys.exit(1)

        # interrupt handler
        def handle_sigint(signum, frame):
            traceback.print_exc()
            signal.signal(signal.SIGINT, signal.default_int_handler)
            os.kill(0, signal.SIGHUP)
            self.alive.value = False
            sys.exit(1)

        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigint)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)  # ignore hang up signal
        signal.signal(
            signal.SIGCHLD, signal.SIG_IGN
        )  # ignore children terminate signal

        for i in range(params["num_workers"]):
            task_queue = Queue(ctx=get_context())
            self.tasks.append(task_queue)
            p = Process(
                target=worker,
                args=(
                    params["bucket_size"],
                    params["data_lifetime"],
                    params["num_workers"],
                    handler,
                    params["data_dir"],
                    params["model_config_dir"],
                    params["version"],
                    task_queue,
                    self.results,
                ),
            )
            p.daemon = True
            self.process.append(p)
            p.start()

            t = Thread(
                target=worker_daemon,
                args=(
                    i,
                    params["bucket_size"],
                    params["data_lifetime"],
                    self.process,
                    self.alive,
                    handler,
                    params["data_dir"],
                    params["model_config_dir"],
                    params["version"],
                    task_queue,
                    self.results,
                ),
            )
            t.setDaemon(True)
            t.start()

            flusher = Flusher(
                task_queue, self.dict, params["batch_size"], params["timeout"]
            )
            self.flushers.append(flusher)
            flusher.run()

    def listenConn(self):
        conn, addr = self._listener.accept()
        q = Queue(ctx=get_context())
        self.qs[self._count] = q
        thread = Thread(
            target=self._accepter.accept,
            args=(conn, addr, self.dict, self._count, q, self.flushers),
        )
        thread.setDaemon(True)
        thread.start()
        self._thread_list.append(thread)

    def serve(self):
        thread = Thread(target=start_split, args=(self.results, self.qs))
        thread.setDaemon(True)
        thread.start()

        self._listener.listen(self._max_connections)
        try:
            while True:
                self._count += 1
                self.listenConn()
        except Exception as e:
            traceback.print_exc()
            error = "error serve: %s" % e
            logger.error(error)
            self.stop()

    def stop(self):
        self._listener.close()
        for flusher in self.flushers:
            flusher.stop()
        for task in self.tasks:
            task.put("stop")
        if isinstance(self._socket_path, str):
            with contextlib.suppress(FileNotFoundError):
                os.remove(self._socket_path)
