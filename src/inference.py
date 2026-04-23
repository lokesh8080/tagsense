"""Inference Lambda — tiered tag inference engine."""

import json
import re
import boto3
from datetime import datetime, timezone, timedelta
from collections import Counter
from config import (
    load_tag_policy, REGION, RESULTS_BUCKET, RESULTS_PREFIX,
    NEIGHBOR_CONSENSUS_THRESHOLD, BEDROCK_MODEL_ID,
    BEDROCK_CONFIDENCE_THRESHOLD, CREATE_EVENT_MAP,
    CLOUDTRAIL_LOOKBACK_DAYS, ORPHAN_INACTIVITY_DAYS,
)


def tier1_stack(arn, cfn):
    try:
        resp = cfn.describe_stack_resources(PhysicalResourceId=arn)
        sr = resp.get("StackResources", [])
        if not sr:
            return None
        stack = cfn.describe_stacks(StackName=sr[0]["StackName"])["Stacks"][0]
        tags = {t["Key"]: t["Value"] for t in stack.get("Tags", []) if not t["Key"].startswith("aws:")}
        if tags:
            return {"suggested_tags": tags, "confidence": 99, "tier": 1,
                    "method": "CloudFormation stack", "evidence": f"Stack: {sr[0]['StackName']}"}
    except Exception:
        pass
    return None


def tier2_cloudtrail(resource, trail):
    rtype = resource["resource_type"]
    arn = resource["arn"]
    rid = arn.split("/")[-1] if "/" in arn else arn.split(":")[-1]
    event_name = CREATE_EVENT_MAP.get(rtype)
    if not event_name:
        return None
    try:
        start = datetime.now(timezone.utc) - timedelta(days=CLOUDTRAIL_LOOKBACK_DAYS)
        resp = trail.lookup_events(
            LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": event_name}],
            StartTime=start, MaxResults=50)
        for ev in resp.get("Events", []):
            res_names = [r.get("ResourceName", "") for r in ev.get("Resources", [])]
            if rid in res_names or rid in ev.get("CloudTrailEvent", ""):
                user = ev.get("Username", "unknown")
                return {"suggested_tags": {"Owner": user, "CreatedBy": user, "CreatedDate": ev["EventTime"].strftime("%Y-%m-%d")},
                        "confidence": 95, "tier": 2, "method": "CloudTrail creator",
                        "evidence": f"Created by {user} on {ev['EventTime']} via {event_name}"}
    except Exception:
        pass
    return None


def tier3_neighbor(resource, ec2, tagging, tag_policy):
    if resource["resource_type"] != "ec2:instance":
        return None
    rid = resource["arn"].split("/")[-1] if "/" in resource["arn"] else resource["arn"].split(":")[-1]
    try:
        resp = ec2.describe_instances(InstanceIds=[rid])
        vpc_id = resp["Reservations"][0]["Instances"][0].get("VpcId")
        if not vpc_id:
            return None
    except Exception:
        return None

    required_keys = [k for k, v in tag_policy.items() if v.get("required")]
    try:
        instances = ec2.describe_instances(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        peer_tags = []
        for res in instances.get("Reservations", []):
            for inst in res.get("Instances", []):
                tags = {t["Key"]: t["Value"] for t in inst.get("Tags", []) if not t["Key"].startswith("aws:")}
                if any(k in tags for k in required_keys):
                    peer_tags.append(tags)
        if len(peer_tags) < 3:
            return None
        consensus = {}
        for key in required_keys:
            vals = [t[key] for t in peer_tags if key in t]
            if not vals:
                continue
            top, count = Counter(vals).most_common(1)[0]
            if count / len(peer_tags) >= NEIGHBOR_CONSENSUS_THRESHOLD:
                consensus[key] = top
        if consensus:
            return {"suggested_tags": consensus, "confidence": int(len(consensus) / len(required_keys) * 80),
                    "tier": 3, "method": "Neighbor consensus",
                    "evidence": f"{len(peer_tags)} peers in VPC {vpc_id}"}
    except Exception:
        pass
    return None


def tier4_bedrock(resource, tag_policy, bedrock):
    # Build richer context from existing tags
    existing = resource.get('tags', {})
    name_hint = existing.get('Name', existing.get('name', ''))
    creator_hint = existing.get('creator', existing.get('creatorUserId', existing.get('lambda:createdBy', '')))
    dz_project = existing.get('AmazonDataZoneProject', '')

    prompt = f"""You are an AWS resource tagging assistant. Suggest tags for this resource.

Resource: ARN={resource['arn']}, Type={resource['resource_type']}
Existing tags: {json.dumps(existing)}
Name/hint: {name_hint}
Creator hint: {creator_hint}
DataZone project: {dz_project}

Required tags (suggest values for THESE missing ones only): {json.dumps({k:v for k,v in tag_policy.items() if k in resource.get('missing_tags',[])})}

Rules:
- Use the existing tags, resource name, and ARN to infer the required tags.
- For Owner: look at creator, creatorUserId, lambda:createdBy, or sqlworkbench-resource-owner tags.
- For Environment: look at Name tag patterns (prod, dev, staging), or account context.
- For Application: look at Name, AmazonDataZoneProject, Application tags, or ARN patterns.
- For CostCenter: only suggest if there's clear evidence. Otherwise say "unknown".
- For each tag, state confidence: high/medium/low.
- If you cannot determine a value, say "unknown". Do not guess.
- Respond ONLY with JSON: {{"tags": {{"Key": {{"value":"...","confidence":"high|medium|low","reasoning":"..."}}}}}}"""

    try:
        resp = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps({"anthropic_version": "bedrock-2023-05-31", "max_tokens": 500,
                             "messages": [{"role": "user", "content": prompt}]}))
        text = json.loads(resp["body"].read())["content"][0]["text"]
        print(f"TIER4 RAW [{resource['arn'][-40:]}]: {text[:300]}")
        # Strip markdown code fences
        clean = text.strip()
        if clean.startswith("```"):
            clean = re.sub(r'^```(?:json)?\s*', '', clean)
            clean = re.sub(r'\s*```$', '', clean)
        m = re.search(r'\{.*\}', clean, re.DOTALL)
        if not m:
            return None
        parsed = json.loads(m.group())
        tags_data = parsed.get("tags", {})
        conf_map = {"high": 80, "medium": 60, "low": 30}
        suggested, reasons = {}, []
        for k, info in tags_data.items():
            v = info.get("value", "unknown")
            if v and v != "unknown":
                suggested[k] = v
                reasons.append(f"{k}={v} ({info.get('confidence','low')}: {info.get('reasoning','N/A')})")
        if suggested:
            avg = sum(conf_map.get(tags_data[k].get("confidence", "low"), 30) for k in suggested) // len(suggested)
            return {"suggested_tags": suggested, "confidence": avg, "tier": 4,
                    "method": "Bedrock AI", "evidence": "; ".join(reasons)}
    except Exception as e:
        return {"suggested_tags": {}, "confidence": 0, "tier": 4, "method": "Bedrock AI",
                "evidence": f"Error: {type(e).__name__}: {str(e)[:200]}"}
    return None


def tier5_manual(resource, cw):
    is_orphan = False
    rtype = resource["resource_type"]
    rid = resource["arn"].split("/")[-1] if "/" in resource["arn"] else resource["arn"].split(":")[-1]
    checks = {"ec2:instance": ("AWS/EC2", "CPUUtilization", "InstanceId"),
              "lambda:function": ("AWS/Lambda", "Invocations", "FunctionName"),
              "rds:db": ("AWS/RDS", "DatabaseConnections", "DBInstanceIdentifier")}
    if rtype in checks:
        ns, metric, dim = checks[rtype]
        try:
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=ORPHAN_INACTIVITY_DAYS)
            resp = cw.get_metric_data(
                MetricDataQueries=[{"Id": "m1", "MetricStat": {"Metric": {"Namespace": ns, "MetricName": metric,
                    "Dimensions": [{"Name": dim, "Value": rid}]}, "Period": 86400, "Stat": "Sum"}}],
                StartTime=start, EndTime=end)
            vals = resp["MetricDataResults"][0].get("Values", [])
            is_orphan = all(v == 0 for v in vals) if vals else True
        except Exception:
            pass
    return {"suggested_tags": {}, "confidence": 0, "tier": 5, "method": "Manual review",
            "evidence": "No signal from Tiers 1-4", "is_likely_orphan": is_orphan,
            "orphan_note": "No usage in 30 days — consider terminating" if is_orphan else ""}


def run_inference(resource, tag_policy, clients, bedrock_allowed=True):
    if not resource.get("missing_tags"):
        return None
    missing = resource["missing_tags"]

    r = tier1_stack(resource["arn"], clients["cfn"])
    if r and any(k in r.get("suggested_tags", {}) for k in missing):
        return r

    r = tier2_cloudtrail(resource, clients["trail"])
    if r:
        return r

    r = tier3_neighbor(resource, clients["ec2"], clients["tagging"], tag_policy)
    if r and r["confidence"] >= 60:
        return r

    # Tier 4: Only call Bedrock for resources with 2+ existing tags and within budget
    if bedrock_allowed and len(resource.get("tags", {})) >= 2:
        r = tier4_bedrock(resource, tag_policy, clients["bedrock"])
        if r and r.get("suggested_tags"):
            return r
        bedrock_note = r.get("evidence", "") if r else ""
    else:
        bedrock_note = "Skipped — insufficient existing tags for AI inference"

    t5 = tier5_manual(resource, clients["cw"])
    if bedrock_note:
        t5["evidence"] += f" | Bedrock: {bedrock_note}"
    return t5


def handler(event, context):
    region = event.get("region", REGION)
    run_id = event["run_id"]
    s3 = boto3.client("s3")
    clients = {
        "cfn": boto3.client("cloudformation", region_name=region),
        "trail": boto3.client("cloudtrail", region_name=region),
        "ec2": boto3.client("ec2", region_name=region),
        "tagging": boto3.client("resourcegroupstaggingapi", region_name=region),
        "bedrock": boto3.client("bedrock-runtime", region_name=region),
        "cw": boto3.client("cloudwatch", region_name=region),
    }

    obj = s3.get_object(Bucket=RESULTS_BUCKET, Key=event["s3_key"])
    discovery = json.loads(obj["Body"].read())
    tag_policy = discovery["tag_policy"]
    non_compliant = discovery["non_compliant_resources"]

    # Prioritize high-value resource types, then limit for Lambda timeout
    priority_types = {"ec2:instance", "lambda:function", "s3:", "rds:db", "dynamodb:table",
                      "ecs:cluster", "ecs:service", "cloudformation:stack", "elasticloadbalancing:"}
    high = [r for r in non_compliant if any(r["resource_type"].startswith(p) for p in priority_types)]
    low = [r for r in non_compliant if r not in high]
    non_compliant = (high + low)[:int(event.get("max_resources", 300))]

    recommendations = []
    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    orphans = []
    bedrock_call_count = 0
    max_bedrock_calls = int(event.get("max_bedrock_calls", 50))  # Cap Bedrock calls

    for resource in non_compliant:
        result = run_inference(resource, tag_policy, clients, bedrock_call_count < max_bedrock_calls)
        if result:
            recommendations.append({**resource, "inference": result})
            tier_counts[result.get("tier", 5)] += 1
            if result.get("tier") == 4:
                bedrock_call_count += 1
            if result.get("is_likely_orphan"):
                orphans.append(resource["arn"])

    output = {"run_id": run_id, "region": region, "timestamp": datetime.now(timezone.utc).isoformat(),
              "total_inferred": len(recommendations), "tier_breakdown": tier_counts,
              "orphan_candidates": orphans, "recommendations": recommendations}
    s3_key = f"{RESULTS_PREFIX}/{run_id}/inference.json"
    s3.put_object(Bucket=RESULTS_BUCKET, Key=s3_key, Body=json.dumps(output, default=str), ContentType="application/json")

    return {"run_id": run_id, "s3_key": s3_key, "total_inferred": len(recommendations),
            "tier_breakdown": tier_counts, "orphan_count": len(orphans)}
