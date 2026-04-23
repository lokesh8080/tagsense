"""Discovery Lambda — scans account, scores compliance, writes items to S3 for Distributed Map."""

import json
import boto3
from datetime import datetime, timezone
from config import load_tag_policy, REGION, RESULTS_BUCKET, RESULTS_PREFIX


def handler(event, context):
    region = event.get("region", REGION)
    run_id = event.get("run_id", datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))
    tag_policy = load_tag_policy()
    required_keys = [k for k, v in tag_policy.items() if v.get("required")]

    tagging = boto3.client("resourcegroupstaggingapi", region_name=region)
    s3 = boto3.client("s3")

    resources = []
    paginator = tagging.get_paginator("get_resources")
    for page in paginator.paginate():
        for r in page.get("ResourceTagMappingList", []):
            arn = r["ResourceARN"]
            tags = {t["Key"]: t["Value"] for t in r.get("Tags", []) if not t["Key"].startswith("aws:")}
            parts = arn.split(":")
            svc = parts[2] if len(parts) > 2 else "unknown"
            rtype = parts[5].split("/")[0] if len(parts) > 5 and "/" in parts[5] else (parts[5] if len(parts) > 5 else "unknown")

            missing = [k for k in required_keys if k not in tags]
            invalid = {}
            for k, policy in tag_policy.items():
                if k in tags and "allowed_values" in policy and tags[k] not in policy["allowed_values"]:
                    invalid[k] = {"current": tags[k], "allowed": policy["allowed_values"]}

            resources.append({
                "arn": arn, "resource_type": f"{svc}:{rtype}", "tags": tags,
                "missing_tags": missing, "invalid_tags": invalid,
                "compliant": len(missing) == 0 and len(invalid) == 0,
            })

    total = len(resources)
    compliant = sum(1 for r in resources if r["compliant"])
    non_compliant = [r for r in resources if not r["compliant"]]

    by_type = {}
    for r in resources:
        rt = r["resource_type"]
        by_type.setdefault(rt, {"total": 0, "compliant": 0, "non_compliant": 0})
        by_type[rt]["total"] += 1
        by_type[rt]["compliant" if r["compliant"] else "non_compliant"] += 1

    summary = {
        "total_resources": total, "compliant": compliant,
        "non_compliant": total - compliant,
        "compliance_pct": round(compliant / total * 100, 1) if total else 0,
        "by_resource_type": by_type,
    }

    # Write non-compliant resources as JSONL for Distributed Map
    items_key = f"{RESULTS_PREFIX}/{run_id}/items.jsonl"
    lines = "\n".join(json.dumps(r, default=str) for r in non_compliant)
    s3.put_object(Bucket=RESULTS_BUCKET, Key=items_key, Body=lines, ContentType="application/jsonlines")

    # Write discovery summary
    disc_output = {
        "run_id": run_id, "region": region,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tag_policy": tag_policy, "summary": summary,
    }
    s3.put_object(Bucket=RESULTS_BUCKET, Key=f"{RESULTS_PREFIX}/{run_id}/discovery.json",
                  Body=json.dumps(disc_output, default=str), ContentType="application/json")

    return {
        "run_id": run_id, "region": region, "summary": summary,
        "non_compliant_count": len(non_compliant),
        "items_s3": {"Bucket": RESULTS_BUCKET, "Key": items_key},
    }
