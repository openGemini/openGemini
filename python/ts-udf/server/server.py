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

from __future__ import absolute_import
import signal
import threading
import stat
import os
import contextlib
import sys
import configparser
import getopt
import traceback

from openGemini_udf.agent import Server
import pkg_resources
from castor.utils.logger import logger, set_log_filename
from castor.utils import logger as llogger
from castor.utils.periodic import SymbolUpdateThread
from castor.utils.const import CLEAR_CACHE_INTERVAL, CLEAR_MODEL_INTERVAL
from openGemini_udf.telemetry import init_telemetry


def parse_argv(argv):
    _config_file = ""
    _path = ""
    _ip = ""
    _port = ""
    _pidfile = ""
    try:
        opts, _ = getopt.getopt(
            argv[1:], "h", ["config=", "path=", "ip=", "port=", "pidfile="]
        )
        if len(opts) == 0:
            sys.exit(2)
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            sys.exit()
        elif opt == "--config":
            _config_file = arg
        elif opt == "--path":
            _path = arg
        elif opt == "--ip":
            _ip = arg
        elif opt == "--port":
            _port = arg
        elif opt == "--pidfile":
            _pidfile = arg
    if _path != "":
        path = _path
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
    if _ip != "" and _port != "":
        ip = _ip
        port = _port
        _path = (ip, int(port))

    return _config_file, _path, _pidfile


def load_config(config_file: str):
    if not os.path.exists(config_file):
        logger.warning("config file %s not exist", config_file)
        sys.exit(2)

    try:
        _config = configparser.ConfigParser()
        _config.read(config_file)
    except Exception as e:
        logger.warning("load config file %s catch exception: %s", config_file, e)
        sys.exit(2)

    return _config


def print_stack(signum, frame):
    flags = os.O_WRONLY | os.O_CREAT
    modes = stat.S_IWUSR | stat.S_IRUSR
    stack_file = os.fdopen(os.open("stack.log", flags, modes), "w")
    print("\n*** STACKTRACE - START ***\n", file=stack_file)
    for th in threading.enumerate():
        print(th, file=stack_file)
        traceback.print_stack(sys._current_frames()[th.ident], file=stack_file)
        print("\n", file=stack_file)
    print("\n*** STACKTRACE - END ***\n", file=stack_file)
    stack_file.close()


def run(handler):
    signal.signal(signal.SIGUSR1, print_stack)
    # parse command line arguments
    config_file, path, pidfile = parse_argv(sys.argv)

    # load config file
    config = load_config(config_file)
    data_dir = config["System"]["data_dir"]
    model_config_dir = config["System"]["model_config_dir"]
    batch_size = int(config["System"]["batch_size"])
    timeout = float(config["System"]["timeout"])
    max_connections = int(config["System"]["max_connections"])
    num_workers = int(config["System"]["num_workers"])
    bucket_size = int(config["System"]["bucket_size"])
    data_lifetime = int(config["System"]["data_lifetime"])
    monitor_addr = config["Monitor"]["monitor_addr"]
    monitor_database = config["Monitor"]["monitor_database"]
    monitor_https_enabled = config["Monitor"]["monitor_http_enabled"] == "true"
    sampling_interval = int(config["Monitor"]["sampling_interval"])

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # version of this package
    version = pkg_resources.require("openGemini-castor")[0].version

    # init logger
    set_log_filename(filename=config["System"]["logger_file"])
    llogger.basic_config(level=config["System"]["logger_level"])
    # init OpenTelemetry
    init_telemetry(monitor_addr, monitor_database, monitor_https_enabled, sampling_interval)

    thread_cache = SymbolUpdateThread(
        key="del_cache", sleep_interval=CLEAR_CACHE_INTERVAL
    )
    thread_model = SymbolUpdateThread(
        key="del_model", sleep_interval=CLEAR_MODEL_INTERVAL
    )
    thread_model.setDaemon(True)
    thread_cache.setDaemon(True)
    thread_cache.start()
    thread_model.start()

    params = {
        "path": path,
        "pidfile": pidfile,
        "data_dir": data_dir,
        "model_config_dir": model_config_dir,
        "version": version,
        "batch_size": batch_size,
        "timeout": timeout,
        "max_connections": max_connections,
        "num_workers": num_workers,
        "bucket_size": bucket_size,
        "data_lifetime": data_lifetime,
    }
    server = Server(handler, params)
    logger.info("Started server")
    server.serve()
    logger.info("Stopped server")
