"""Upload a local sales CSV to the raw zone of the S3 data lake.

The object lands at:
    s3://<bucket>/raw/sales/ingestion_date=YYYY-MM-DD/<file name>

Credentials come from your local AWS CLI profile (never hard-code keys).

Usage:
    python src/upload_to_s3.py --bucket my-sales-bucket
    python src/upload_to_s3.py --bucket my-sales-bucket --profile sales-dev
    python src/upload_to_s3.py --bucket my-sales-bucket --ingestion-date 2026-10-12
    # or set SALES_BUCKET / AWS_PROFILE in your environment (see .env.example)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path


def build_key(file_path: Path, ingestion_date: date) -> str:
    return f"raw/sales/ingestion_date={ingestion_date.isoformat()}/{file_path.name}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", default=os.getenv("SALES_BUCKET"))
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    parser.add_argument("--file", type=Path, default=Path("data/sample_sales.csv"))
    parser.add_argument("--ingestion-date", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()

    if not args.bucket:
        sys.exit("Set --bucket or the SALES_BUCKET environment variable.")
    if not args.file.exists():
        sys.exit(f"{args.file} not found. Run: python src/generate_data.py")

    import boto3  # imported here so the rest of the module works without boto3

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = session.client("s3")

    key = build_key(args.file, args.ingestion_date)
    s3.upload_file(str(args.file), args.bucket, key)
    print(f"Uploaded {args.file} -> s3://{args.bucket}/{key}")


if __name__ == "__main__":
    main()
