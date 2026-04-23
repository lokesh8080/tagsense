"""Enforce Lambda — generates Tag Policy, SCP, EventBridge templates."""

import json
import boto3
from datetime import datetime, timezone
from config import load_tag_policy, RESULTS_BUCKET, RESULTS_PREFIX, REGION


def handler(event, context):
    run_id = event["run_id"]
    tag_policy = load_tag_policy()
    s3 = boto3.client("s3")

    # Tag Policy
    tp = {"tags": {}}
    for k, cfg in tag_policy.items():
        entry = {"tag_key": {"@@assign": k}}
        if "allowed_values" in cfg:
            entry["tag_value"] = {"@@assign": cfg["allowed_values"]}
        tp["tags"][k] = entry

    # SCP
    required = [k for k, v in tag_policy.items() if v.get("required")]
    scp = {"Version": "2012-10-17", "Statement": [{"Sid": "DenyUntaggedResources", "Effect": "Deny",
        "Action": ["ec2:RunInstances", "rds:CreateDBInstance", "s3:CreateBucket",
                    "lambda:CreateFunction", "dynamodb:CreateTable"],
        "Resource": "*", "Condition": {"Null": {f"aws:RequestTag/{k}": "true" for k in required}}}]}

    # EventBridge
    eb = {"Description": "TagSense auto-tag on resource creation",
          "EventPattern": {"source": ["aws.ec2", "aws.rds", "aws.s3", "aws.lambda"],
                           "detail-type": ["AWS API Call via CloudTrail"],
                           "detail": {"eventName": ["RunInstances", "CreateDBInstance", "CreateBucket", "CreateFunction20150331"]}},
          "Note": "Attach a Lambda target that reads creator from event and applies Owner tag"}

    output = {"run_id": run_id, "timestamp": datetime.now(timezone.utc).isoformat(),
              "artifacts": {"tag_policy": tp, "scp": scp, "eventbridge_rule": eb},
              "instructions": {"tag_policy": "Deploy via Organizations > Tag policies",
                               "scp": "Deploy via Organizations > SCPs — TEST IN SANDBOX OU FIRST",
                               "eventbridge_rule": "Deploy via EventBridge + Lambda target"}}
    key = f"{RESULTS_PREFIX}/{run_id}/enforcement.json"
    s3.put_object(Bucket=RESULTS_BUCKET, Key=key, Body=json.dumps(output, indent=2, default=str), ContentType="application/json")

    return {"run_id": run_id, "s3_key": key, "artifacts_generated": ["tag_policy", "scp", "eventbridge_rule"]}
