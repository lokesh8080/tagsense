"""Inference Worker Lambda — processes a batch of resources through Tiers 1-3 + orphan detection.
Invoked by Step Functions Distributed Map. One invocation per batch (~10-20 resources)."""

import json
import boto3
from datetime import datetime, timezone, timedelta
from collections import Counter
from config import (
    load_tag_policy, REGION, RESULTS_BUCKET, RESULTS_PREFIX,
    NEIGHBOR_CONSENSUS_THRESHOLD, CREATE_EVENT_MAP,
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


def tier3_neighbor(resource, ec2, tag_policy):
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


def check_orphan(resource, cw):
    rtype = resource["resource_type"]
    rid = resource["arn"].split("/")[-1] if "/" in resource["arn"] else resource["arn"].split(":")[-1]
    checks = {"ec2:instance": ("AWS/EC2", "CPUUtilization", "InstanceId"),
              "lambda:function": ("AWS/Lambda", "Invocations", "FunctionName"),
              "rds:db": ("AWS/RDS", "DatabaseConnections", "DBInstanceIdentifier")}
    if rtype not in checks:
        return False
    ns, metric, dim = checks[rtype]
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=ORPHAN_INACTIVITY_DAYS)
        resp = cw.get_metric_data(
            MetricDataQueries=[{"Id": "m1", "MetricStat": {"Metric": {"Namespace": ns, "MetricName": metric,
                "Dimensions": [{"Name": dim, "Value": rid}]}, "Period": 86400, "Stat": "Sum"}}],
            StartTime=start, EndTime=end)
        vals = resp["MetricDataResults"][0].get("Values", [])
        return all(v == 0 for v in vals) if vals else True
    except Exception:
        return False


def process_resource(resource, tag_policy, clients):
    """Run Tiers 1-3 + orphan check. Resources needing Tier 4 are flagged for Bedrock Batch."""
    missing = resource.get("missing_tags", [])
    if not missing:
        return {**resource, "inference": {"tier": 0, "method": "Already compliant", "suggested_tags": {}, "confidence": 100}}

    # Tier 1
    r = tier1_stack(resource["arn"], clients["cfn"])
    if r and any(k in r.get("suggested_tags", {}) for k in missing):
        return {**resource, "inference": r}

    # Tier 2
    r = tier2_cloudtrail(resource, clients["trail"])
    if r:
        return {**resource, "inference": r}

    # Tier 3
    r = tier3_neighbor(resource, clients["ec2"], tag_policy)
    if r and r["confidence"] >= 60:
        return {**resource, "inference": r}

    # Check orphan
    is_orphan = check_orphan(resource, clients["cw"])

    # Flag for Tier 4 (Bedrock Batch) if resource has enough context
    needs_bedrock = len(resource.get("tags", {})) >= 2

    return {
        **resource,
        "inference": {
            "tier": 4 if needs_bedrock else 5,
            "method": "Pending Bedrock Batch" if needs_bedrock else "Manual review",
            "suggested_tags": {},
            "confidence": 0,
            "evidence": "Queued for AI inference" if needs_bedrock else "No signal from Tiers 1-3",
            "is_likely_orphan": is_orphan,
            "orphan_note": "No usage in 30 days — consider terminating" if is_orphan else "",
        }
    }


def handler(event, context):
    """Distributed Map worker — processes a batch of items from S3."""
    region = event.get("region", REGION)
    tag_policy = load_tag_policy()

    clients = {
        "cfn": boto3.client("cloudformation", region_name=region),
        "trail": boto3.client("cloudtrail", region_name=region),
        "ec2": boto3.client("ec2", region_name=region),
        "cw": boto3.client("cloudwatch", region_name=region),
    }

    # Distributed Map passes items in event["Items"]
    items = event.get("Items", [])
    if isinstance(items, str):
        items = [json.loads(line) for line in items.strip().split("\n") if line.strip()]

    results = []
    for item in items:
        if isinstance(item, str):
            item = json.loads(item)
        results.append(process_resource(item, tag_policy, clients))

    return results
