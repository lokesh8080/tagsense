"""Apply Lambda — applies approved tags. Dry-run by default."""

import json
import csv
import io
import boto3
from datetime import datetime, timezone
from config import RESULTS_BUCKET, RESULTS_PREFIX, REGION


def handler(event, context):
    run_id = event["run_id"]
    dry_run = event.get("dry_run", True)
    region = event.get("region", REGION)
    s3 = boto3.client("s3")
    tagging = boto3.client("resourcegroupstaggingapi", region_name=region)

    obj = s3.get_object(Bucket=RESULTS_BUCKET, Key=f"{RESULTS_PREFIX}/{run_id}/review.csv")
    reader = csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8")))

    applied, skipped, errors = [], [], []
    for row in reader:
        if row.get("Approve (Y/N)", "").strip().upper() != "Y":
            skipped.append(row["ARN"])
            continue
        try:
            tags = json.loads(row["Suggested Tags"])
        except (json.JSONDecodeError, KeyError):
            skipped.append(row["ARN"])
            continue
        if not tags:
            skipped.append(row["ARN"])
            continue
        tags = {k: v for k, v in tags.items() if not k.startswith("aws:")}
        if dry_run:
            applied.append({"arn": row["ARN"], "tags": tags, "action": "DRY_RUN"})
        else:
            try:
                tagging.tag_resources(ResourceARNList=[row["ARN"]], Tags=tags)
                applied.append({"arn": row["ARN"], "tags": tags, "action": "APPLIED"})
            except Exception as e:
                errors.append({"arn": row["ARN"], "error": str(e)})

    audit = {"run_id": run_id, "timestamp": datetime.now(timezone.utc).isoformat(),
             "dry_run": dry_run, "applied": len(applied), "skipped": len(skipped), "errors": len(errors),
             "details": {"applied": applied, "skipped": skipped, "errors": errors}}
    key = f"{RESULTS_PREFIX}/{run_id}/apply_audit.json"
    s3.put_object(Bucket=RESULTS_BUCKET, Key=key, Body=json.dumps(audit, default=str), ContentType="application/json")

    return {"run_id": run_id, "dry_run": dry_run, "applied": len(applied), "skipped": len(skipped), "errors": len(errors), "audit_key": key}
