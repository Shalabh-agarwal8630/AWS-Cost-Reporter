#!/usr/bin/env python3
"""
aws_cost_daily.py

Fetch AWS costs (by service) for yesterday, a specific date, or a date range.
Save JSON/CSV to ./output/ and upload to S3.

Config is read from env or .env file:
    AWS_PROFILE    (optional, e.g. "my-prod-account")
    COST_S3_BUCKET (required, S3 bucket name)
    COST_S3_PREFIX (optional, default "aws-costs/")
    COST_DATE      (optional, single date YYYY-MM-DD)
    COST_START     (optional, start date YYYY-MM-DD)
    COST_END       (optional, end date YYYY-MM-DD)
"""

import boto3
import datetime
import json
import os
from decimal import Decimal
from dotenv import load_dotenv

try:
    import pandas as pd
except ImportError:
    pd = None

# Load .env if present
load_dotenv(dotenv_path=os.path.join("config", ".env"))

# --- Config ---
PROFILE = os.environ.get("AWS_PROFILE")
BUCKET = os.environ.get("COST_S3_BUCKET")
PREFIX = os.environ.get("COST_S3_PREFIX", "aws-costs/")
REGION = "us-east-1"

DATE = os.environ.get("COST_DATE")      # single date
START = os.environ.get("COST_START")    # range start
END = os.environ.get("COST_END")        # range end

if not BUCKET:
    raise RuntimeError("COST_S3_BUCKET env var is required")


def boto3_ce_client():
    session = boto3.Session(profile_name=PROFILE) if PROFILE else boto3.Session()
    return session.client("ce", region_name=REGION)


def boto3_s3_client():
    session = boto3.Session(profile_name=PROFILE) if PROFILE else boto3.Session()
    return session.client("s3")


def get_dates():
    """Return (start, end, label) depending on env vars."""
    if DATE:
        d = datetime.datetime.strptime(DATE, "%Y-%m-%d").date()
        start = d.strftime("%Y-%m-%d")
        end = (d + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        return start, end, f"{DATE}"
    elif START and END:
        s = datetime.datetime.strptime(START, "%Y-%m-%d").date()
        e = datetime.datetime.strptime(END, "%Y-%m-%d").date()
        start = s.strftime("%Y-%m-%d")
        end = (e + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        return start, end, f"{START}_to_{END}"
    else:
        # default → yesterday
        today = datetime.date.today()
        yest = today - datetime.timedelta(days=1)
        start = yest.strftime("%Y-%m-%d")
        end = (yest + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        return start, end, yest.strftime("%Y-%m-%d")


def get_cost_by_service(client, start, end):
    return client.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}]
    )


def normalize(resp):
    rows = []
    for period in resp.get("ResultsByTime", []):
        for g in period.get("Groups", []):
            service = g["Keys"][0]
            metric = g["Metrics"]["UnblendedCost"]
            amount = float(Decimal(metric["Amount"]))
            unit = metric["Unit"]
            rows.append({
                "date": period["TimePeriod"]["Start"],
                "service": service,
                "amount": amount,
                "unit": unit
            })
    return rows


def save_json(rows, path):
    with open(path, "w") as f:
        json.dump(rows, f, indent=2)


def save_csv(rows, path):
    if pd is None:
        return
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


def upload(local, bucket, key):
    boto3_s3_client().upload_file(local, bucket, key)


def main():
    client = boto3_ce_client()
    start, end, label = get_dates()
    resp = get_cost_by_service(client, start, end)
    rows = normalize(resp)

    os.makedirs("output", exist_ok=True)
    json_file = os.path.join("output", f"aws_costs_{label}.json")
    csv_file = os.path.join("output", f"aws_costs_{label}.csv")

    save_json(rows, json_file)
    save_csv(rows, csv_file)

    key_json = f"{PREFIX.rstrip('/')}/{os.path.basename(json_file)}"
    upload(json_file, BUCKET, key_json)
    if pd:
        key_csv = f"{PREFIX.rstrip('/')}/{os.path.basename(csv_file)}"
        upload(csv_file, BUCKET, key_csv)

    total = sum(r["amount"] for r in rows)
    unit = rows[0]["unit"] if rows else "USD"
    print(f"✅ Uploaded billing for {label} → s3://{BUCKET}/{PREFIX}")
    print(f"💰 Total = {total:.2f} {unit}")


if __name__ == "__main__":
    main()
