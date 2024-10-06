from setuptools import setup
from os import path

import sys

setup_dir = path.dirname(__file__)
sys.path.insert(0, setup_dir)

from openGemini_udf import VERSION


this_directory = path.abspath(path.dirname(__file__))

# read the contents of requirements.txt
with open(path.join(this_directory, "requirements.txt"), encoding="utf-8") as f:
    requirements = f.read().splitlines()


setup(
    name="openGemini_udf",
    version=VERSION,
    author="Cloud Database Innovation Lab, Huawei Cloud Computing Technologies Co., Ltd.",
    author_email="community.ts@opengemini.org",
    url="http://opengemini.org/",
    description="openGemini agent library",
    packages=["openGemini_udf"],
    license="Apache-2.0",
    include_package_data=True,
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
