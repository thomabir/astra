from __future__ import unicode_literals

import io
import json
import os
from distutils import dir_util

import pandas as pd
import pytest
import yaml
from pytest import fixture

from astra import schedule


@fixture
def datadir(tmpdir, request):
    """
    # https://stackoverflow.com/questions/29627341/pytest-where-to-store-expected-data
    Fixture responsible for searching a folder with the same name of test
    module and, if available, moving all contents to a temporary directory so
    tests can use them freely.
    """
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)

    if os.path.isdir(test_dir):
        dir_util.copy_tree(test_dir, str(tmpdir))

    return tmpdir


def test_yaml_to_csv(datadir):
    csv_df = schedule.read_schedule(datadir.join("full_schedule.csv"))
    yaml_df = schedule.read_schedule(
        datadir.join("full_schedule.yaml"), observatory_name="Callisto"
    )

    for i in range(len(yaml_df)):
        assert csv_df.iloc[i].to_dict() == yaml_df.iloc[i].to_dict()


@pytest.mark.parametrize(
    "csv_str, yaml_str",
    [
        (
            "Camera,camera_Callisto,open,{},2024-01-11 23:31:40.915,2024-01-12 10:07:40.253",
            """
open:
  start_time: '2024-01-11 23:31:40.915'
  end_time: '2024-01-12 10:07:40.253'
            """,
        ),
        (
            "Camera,camera_Callisto,close,{},2024-01-12 10:07:40.253,2024-01-12 10:07:40.253",
            """
close:
    start_time: '2024-01-12 10:07:40.253'
    end_time: '2024-01-12 10:07:40.253'
            """,
        ),
        (
            "Camera,camera_Callisto,object,\"{'object': 'Sp0711-3824', 'filter': 'I+z', 'ra': 107.7545375, 'dec': -38.41298694444444, 'exptime': 13, 'guiding': True, 'pointing': False}\",2024-01-12 00:16:20.020,2024-01-12 04:49:20.020",
            """
object:
  object: Sp0711-3824
  filter: I+z
  ra: 107.7545375
  dec: -38.41298694444444
  exptime: 13
  guiding: true
  pointing: false
  start_time: '2024-01-12 00:16:20.020'
  end_time: '2024-01-12 04:49:20.020'
            """,
        ),
        # single flats as list
        (
            "Camera,camera_Callisto,flats,\"{'filter': ['I+z'], 'n': [10]}\",2024-01-12 09:23:00.030,2024-01-12 10:07:40.253",
            """
flats:
  filter:
    - 'I+z'
  n:
    - 10
  start_time: '2024-01-12 09:23:00.030'
  end_time: '2024-01-12 10:07:40.253'
            """,
        ),
        # single flats as single string (filter: 'I+z' instead of filter: ['I+z'])
        (
            "Camera,camera_Callisto,flats,\"{'filter': ['I+z'], 'n': [10]}\",2024-01-12 09:23:00.030,2024-01-12 10:07:40.253",
            """
flats:
  filter: 'I+z'
  n: 10
  start_time: '2024-01-12 09:23:00.030'
  end_time: '2024-01-12 10:07:40.253'
            """,
        ),
        (
            "Camera,camera_Callisto,calibration,\"{'exptime': [0, 10, 13, 15, 21, 30, 60, 120], 'n': [10, 10, 10, 10, 10, 10, 10, 10]}\",2024-01-12 10:12:40.253,2024-01-12 10:37:40.253",
            """
calibration:
  exptime: [0,10,13,15,21,30,60,120]
  n: [10,10,10,10,10,10,10,10]
  start_time: '2024-01-12 10:12:40.253'
  end_time: '2024-01-12 10:37:40.253'
            """,
        ),
    ],
    ids=["open", "close", "object", "flats", "flats_filter_str", "calibration"],
)
def test_action_yaml_csv(csv_str, yaml_str):
    headers = [
        "device_type",
        "device_name",
        "action_type",
        "action_value",
        "start_time",
        "end_time",
    ]

    csv_df = pd.read_csv(io.StringIO(csv_str), names=headers)
    csv_dict = schedule.action_series_to_dict(csv_df.iloc[0])

    # just for comparison, because the current astra csv_df["action_value"]
    # is not well formatted

    assert csv_dict == schedule.unflatten_action(yaml.safe_load(yaml_str))
