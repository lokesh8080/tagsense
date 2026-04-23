"""TagSense shared config — loaded by all Lambdas."""

import os
import json

DEFAULT_TAG_POLICY = {
    "Owner": {"required": True, "description": "Team or individual owning the resource"},
    "Environment": {"required": True, "allowed_values": ["prod", "staging", "dev", "sandbox"]},
    "CostCenter": {"required": True, "description": "Budget code for cost allocation"},
    "Application": {"required": True, "description": "Application or workload name"},
}

RESULTS_BUCKET = os.environ.get("RESULTS_BUCKET", "")
RESULTS_PREFIX = os.environ.get("RESULTS_PREFIX", "tagsense")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
REGION = os.environ.get("AWS_REGION", "us-east-1")
NEIGHBOR_CONSENSUS_THRESHOLD = 0.7
BEDROCK_CONFIDENCE_THRESHOLD = 50
CLOUDTRAIL_LOOKBACK_DAYS = 90
ORPHAN_INACTIVITY_DAYS = 30

CREATE_EVENT_MAP = {
    "ec2:instance": "RunInstances",
    "s3:bucket": "CreateBucket",
    "rds:db": "CreateDBInstance",
    "lambda:function": "CreateFunction20150331",
    "dynamodb:table": "CreateTable",
    "sqs:queue": "CreateQueue",
    "sns:topic": "CreateTopic",
    "ecs:cluster": "CreateCluster",
    "elasticloadbalancing:loadbalancer": "CreateLoadBalancer",
}


def load_tag_policy():
    policy_json = os.environ.get("TAG_POLICY")
    if policy_json:
        return json.loads(policy_json)
    return DEFAULT_TAG_POLICY
