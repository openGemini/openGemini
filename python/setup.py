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
from setuptools import setup, find_packages
from os import path

import sys

setup_dir = path.dirname(__file__)
sys.path.insert(0, setup_dir)
from ts_udf import VERSION

this_directory = path.abspath(path.dirname(__file__))

# read the contents of requirements.txt
with open(path.join(this_directory, "requirements.txt"), encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="ts_udf",
    version=VERSION,
    author="Cloud Database Innovation Lab, Huawei Cloud Computing Technologies Co., Ltd.",
    author_email="community.ts@opengemini.org",
    url="http://opengemini.org/",
    description="openGemini ts_udf library",
    packages=find_packages(include=["agent", "agent.openGemini_udf", "ts_udf", "ts_udf.*"]),
    license="Apache-2.0",
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "ts-udf = ts_udf.server.handler:main",
        ]
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
