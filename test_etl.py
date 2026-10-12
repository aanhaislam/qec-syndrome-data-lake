"""Tests the pipeline end to end with S3 mocked by moto."""

import json

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

import etl


def test_decoder_beats_no_decoding():
    circuit, dets, obs = etl.generate(3, 0.004, 5000, seed=1)
    cols = etl.decode(circuit, dets, obs)
    assert cols["logical_error"].mean() < cols["actual_flip"].mean()


def test_larger_distance_helps_below_threshold():
    rates = {}
    for d in (3, 5):
        c, dets, obs = etl.generate(d, 0.003, 20000, seed=d)
        rates[d] = etl.decode(c, dets, obs)["logical_error"].mean()
    assert rates[5] < rates[3]


def test_quality_check_catches_duplicates():
    df = pd.DataFrame({"run_id": ["a", "a"], "shot_index": [0, 0], "syndrome_weight": [1, 2]})
    with pytest.raises(ValueError, match="duplicate"):
        etl.quality_checks(df, 2)


@mock_aws
def test_end_to_end_with_s3(tmp_path):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="my-bucket")
    report = etl.run([3, 5], [0.004, 0.008], 2000, tmp_path, bucket="my-bucket", s3=s3)
    assert report["runs"] == 4 and report["rows_loaded"] == 8000
    assert report["parquet_bytes"] < report["csv_bytes"]

    keys = {o["Key"] for o in s3.list_objects_v2(Bucket="my-bucket")["Contents"]}
    assert any(k.startswith("qec/shots_parquet/distance=3/") for k in keys)
    assert any(k.startswith("qec/shots_parquet/distance=5/") for k in keys)
    assert {"qec/shots_csv/shots.csv", "qec/runs/runs.parquet"} <= keys

    runs = pq.read_table(tmp_path / "output/runs/runs.parquet").to_pandas()
    assert len(runs) == 4 and runs["run_id"].is_unique
    assert json.loads((tmp_path / "quality_report.json").read_text())["rows_loaded"] == 8000
