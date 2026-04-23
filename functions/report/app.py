"""Report Lambda v2 — generates CSV + summary from inference results."""

import json
import csv
import io
import boto3
from datetime import datetime, timezone
from config import RESULTS_BUCKET, RESULTS_PREFIX, REGION, SNS_TOPIC_ARN


def handler(event, context):
    run_id = event["run_id"]
    s3 = boto3.client("s3")

    disc = json.loads(s3.get_object(Bucket=RESULTS_BUCKET, Key=f"{RESULTS_PREFIX}/{run_id}/discovery.json")["Body"].read())
    inf = json.loads(s3.get_object(Bucket=RESULTS_BUCKET, Key=f"{RESULTS_PREFIX}/{run_id}/inference.json")["Body"].read())

    # CSV
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ARN", "Resource Type", "Existing Tags", "Missing Tags", "Suggested Tags",
                "Confidence %", "Tier", "Method", "Evidence", "Likely Orphan", "Approve (Y/N)"])
    for rec in inf.get("recommendations", []):
        i = rec.get("inference", {})
        w.writerow([rec["arn"], rec["resource_type"], json.dumps(rec.get("tags", {})),
                    ", ".join(rec.get("missing_tags", [])), json.dumps(i.get("suggested_tags", {})),
                    i.get("confidence", 0), i.get("tier", "N/A"), i.get("method", "N/A"),
                    i.get("evidence", "N/A"), "YES" if i.get("is_likely_orphan") else "", ""])
    csv_key = f"{RESULTS_PREFIX}/{run_id}/review.csv"
    s3.put_object(Bucket=RESULTS_BUCKET, Key=csv_key, Body=buf.getvalue(), ContentType="text/csv")

    # Recount tiers from actual data
    ds = disc["summary"]
    tier = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for rec in inf.get("recommendations", []):
        t = rec.get("inference", {}).get("tier", 5)
        tier[t] = tier.get(t, 0) + 1
    orphan_count = sum(1 for r in inf.get("recommendations", []) if r.get("inference", {}).get("is_likely_orphan"))
    resolvable = sum(tier.get(t, 0) for t in [1, 2, 3, 4])
    est = round((ds["compliant"] + resolvable) / ds["total_resources"] * 100, 1) if ds["total_resources"] else 0

    summary = f"""=== TagSense Report ===
Run: {run_id} | Region: {event.get('region', REGION)} | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

DISCOVERY
  Total resources: {ds['total_resources']}
  Compliant: {ds['compliant']} ({ds['compliance_pct']}%)
  Non-compliant: {ds['non_compliant']}

INFERENCE (Distributed Map + Bedrock Batch)
  Tier 1 (CloudFormation):  {tier.get(1,0)} resources (~99% confidence)
  Tier 2 (CloudTrail):      {tier.get(2,0)} resources (~95% confidence)
  Tier 3 (Neighbor):        {tier.get(3,0)} resources (~80% confidence)
  Tier 4 (Bedrock AI):      {tier.get(4,0)} resources (~60% confidence)
  Tier 5 (Manual):          {tier.get(5,0)} resources (needs human)
  Orphan candidates:        {orphan_count}

PROJECTED COMPLIANCE: {ds['compliance_pct']}% → ~{est}%

FILES
  Review CSV: s3://{RESULTS_BUCKET}/{csv_key}
  Full data:  s3://{RESULTS_BUCKET}/{RESULTS_PREFIX}/{run_id}/inference.json
"""
    s3.put_object(Bucket=RESULTS_BUCKET, Key=f"{RESULTS_PREFIX}/{run_id}/summary.txt",
                  Body=summary, ContentType="text/plain")

    if SNS_TOPIC_ARN:
        boto3.client("sns", region_name=REGION).publish(
            TopicArn=SNS_TOPIC_ARN, Subject=f"TagSense Report — {run_id}", Message=summary)

    return {"run_id": run_id, "csv_key": csv_key, "summary": summary}
