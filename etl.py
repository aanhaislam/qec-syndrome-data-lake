"""ETL pipeline for surface code syndrome data.

Extract   generates rotated surface code memory experiments with Stim across
          code distances and physical error rates, sampling detection events
          and true logical outcomes shot by shot.
Transform decodes every shot with PyMatching (minimum weight perfect matching),
          computes syndrome weight and whether the decoder failed, and runs
          data quality checks on the result.
Load      writes a shot level fact table and a run level dimension table as
          Parquet partitioned by distance, plus an unpartitioned CSV copy for
          comparison, and uploads everything to your S3 bucket for Athena.

Usage
    python etl.py --bucket YOUR_BUCKET
    python etl.py --shots 2000 --local-only
"""

import argparse
import json
import shutil
import time
import uuid
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pymatching
import stim

DEFAULT_DISTANCES = [3, 5, 7]
DEFAULT_RATES = [0.002, 0.004, 0.006, 0.008, 0.010, 0.012]


# ---------------------------------------------------------------- extract

def generate(distance, p, shots, seed):
    """Sample one memory experiment. Returns circuit, detection events, observable flips."""
    circuit = stim.Circuit.generated(
        "surface_code:rotated_memory_z",
        distance=distance, rounds=distance,
        after_clifford_depolarization=p,
        before_round_data_depolarization=p,
        before_measure_flip_probability=p,
        after_reset_flip_probability=p)
    sampler = circuit.compile_detector_sampler(seed=seed)
    dets, obs = sampler.sample(shots, separate_observables=True)
    return circuit, dets, obs


# -------------------------------------------------------------- transform

def decode(circuit, dets, obs):
    """Decode every shot and return per shot columns as a dict of arrays."""
    dem = circuit.detector_error_model(decompose_errors=True)
    matcher = pymatching.Matching.from_detector_error_model(dem)
    predicted = matcher.decode_batch(dets)
    actual = obs[:, 0].astype(bool)
    pred = predicted[:, 0].astype(bool)
    return {"syndrome_weight": dets.sum(axis=1).astype("int32"),
            "actual_flip": actual,
            "predicted_flip": pred,
            "logical_error": actual != pred}


def quality_checks(shots_df, expected_rows):
    """Fail loudly if the batch is malformed before anything is loaded."""
    problems = []
    if len(shots_df) != expected_rows:
        problems.append(f"expected {expected_rows} rows, got {len(shots_df)}")
    if shots_df.isna().any().any():
        problems.append("null values present")
    if (shots_df["syndrome_weight"] < 0).any():
        problems.append("negative syndrome weight")
    dupes = shots_df.duplicated(subset=["run_id", "shot_index"]).sum()
    if dupes:
        problems.append(f"{dupes} duplicate shot keys")
    if problems:
        raise ValueError("data quality check failed  " + "; ".join(problems))


# ------------------------------------------------------------------- load

def write_outputs(shots_df, runs_df, out_dir):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    pq.write_to_dataset(pa.Table.from_pandas(shots_df, preserve_index=False),
                        root_path=str(out_dir / "shots_parquet"),
                        partition_cols=["distance"])
    (out_dir / "shots_csv").mkdir(parents=True)
    csv = shots_df[["run_id", "shot_index", "physical_error_rate", "syndrome_weight",
                    "actual_flip", "predicted_flip", "logical_error", "distance"]].copy()
    for c in ("actual_flip", "predicted_flip", "logical_error"):
        csv[c] = csv[c].map({True: "true", False: "false"})
    csv.to_csv(out_dir / "shots_csv" / "shots.csv", index=False)
    (out_dir / "runs").mkdir(parents=True)
    pq.write_table(pa.Table.from_pandas(runs_df, preserve_index=False),
                   out_dir / "runs" / "runs.parquet")


def upload(out_dir, bucket, prefix, s3=None):
    s3 = s3 or boto3.client("s3")
    count = 0
    for path in sorted(out_dir.rglob("*")):
        if path.is_file():
            s3.upload_file(str(path), bucket, f"{prefix}/{path.relative_to(out_dir).as_posix()}")
            count += 1
    return count


# -------------------------------------------------------------------- run

def run(distances, rates, shots, work_dir, bucket=None, prefix="qec", seed=7, s3=None):
    start = time.time()
    work_dir = Path(work_dir)
    shot_frames, runs = [], []
    for d in distances:
        for i, p in enumerate(rates):
            t0 = time.time()
            circuit, dets, obs = generate(d, p, shots, seed=seed + 1000 * d + i)
            cols = decode(circuit, dets, obs)
            run_id = uuid.uuid4().hex[:12]
            df = pd.DataFrame({"run_id": run_id,
                               "shot_index": np.arange(shots, dtype="int32"),
                               "physical_error_rate": p,
                               **cols})
            df["distance"] = d
            shot_frames.append(df)
            ler = float(cols["logical_error"].mean())
            runs.append({"run_id": run_id, "distance": d, "rounds": d,
                         "physical_error_rate": p, "shots": shots,
                         "num_detectors": circuit.num_detectors,
                         "logical_errors": int(cols["logical_error"].sum()),
                         "logical_error_rate": ler,
                         "seconds": round(time.time() - t0, 3)})
            print(f"d={d}  p={p:.3f}  logical error rate {ler:.5f}")

    shots_df = pd.concat(shot_frames, ignore_index=True)
    runs_df = pd.DataFrame(runs)
    quality_checks(shots_df, len(distances) * len(rates) * shots)

    out_dir = work_dir / "output"
    write_outputs(shots_df, runs_df, out_dir)
    report = {"runs": len(runs_df),
              "rows_loaded": len(shots_df),
              "total_detection_events": int(shots_df["syndrome_weight"].sum()),
              "parquet_bytes": sum(f.stat().st_size for f in (out_dir / "shots_parquet").rglob("*.parquet")),
              "csv_bytes": (out_dir / "shots_csv" / "shots.csv").stat().st_size,
              "seconds": round(time.time() - start, 1)}
    if bucket:
        report["files_uploaded"] = upload(out_dir, bucket, prefix, s3=s3)
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "quality_report.json").write_text(json.dumps(report, indent=2))
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--distances", type=int, nargs="+", default=DEFAULT_DISTANCES)
    ap.add_argument("--rates", type=float, nargs="+", default=DEFAULT_RATES)
    ap.add_argument("--shots", type=int, default=100_000, help="shots per (distance, rate) run")
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="qec")
    ap.add_argument("--work-dir", default="data")
    ap.add_argument("--local-only", action="store_true")
    args = ap.parse_args()
    if not args.local_only and not args.bucket:
        ap.error("--bucket is required unless --local-only is set")
    report = run(args.distances, args.rates, args.shots, args.work_dir,
                 bucket=None if args.local_only else args.bucket, prefix=args.prefix)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
