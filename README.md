# QEC Syndrome Data Lake

An ETL pipeline that generates rotated surface code memory experiments with Stim, decodes every shot with PyMatching, and loads the results into Amazon S3 as Parquet partitioned by code distance, queried with SQL in Amazon Athena to find the error correction threshold.

## Pipeline

1. **Extract** samples detection events and true logical outcomes for every combination of code distance (3, 5, 7) and physical error rate (0.2% to 1.2%), 100,000 shots each by default.
2. **Transform** decodes each shot with minimum weight perfect matching, records syndrome weight and whether the decoder failed, and runs data quality checks (row counts, nulls, duplicate keys) before anything is loaded.
3. **Load** writes a shot level fact table (Parquet, partitioned by distance), a run level dimension table, and an unpartitioned CSV copy for comparison, then uploads them to your bucket.
4. **Query** with `athena.sql`, which covers logical error rates computed from raw shots, the threshold crossing, decoder failures by syndrome weight, and a partitioned versus unpartitioned scan test.

Every run writes `data/quality_report.json` with rows loaded, detection events, file sizes, and run time.

## Setup

1. Sign up at aws.amazon.com and choose the **free plan**. Set a billing alert anyway.
2. In IAM, create a user named `qec-pipeline`, attach `AmazonS3FullAccess` and `AmazonAthenaFullAccess`, and create an access key for the CLI.
3. Install the AWS CLI, then run `aws configure` and enter that key, with region `us-east-1`.
4. Install the Python packages.

```
pip install -r requirements.txt
```

5. Make a bucket. Bucket names are global, so add something unique.

```
aws s3 mb s3://aanha-qec-lake-2026 --region us-east-1
```

## Run

```
python -m pytest -q
python etl.py --bucket aanha-qec-lake-2026
```

The default run is 18 experiments and 1.8 million shots, and takes under a minute on a laptop.

## Athena

1. Open Athena, go to Settings, and set the query result location to `s3://aanha-qec-lake-2026/athena-results/`.
2. Paste the statements from `athena.sql` one at a time, with `YOUR_BUCKET` replaced by your bucket name.
3. For query 5, note the **Data scanned** value under each of the two results. That's your partitioning saving.
