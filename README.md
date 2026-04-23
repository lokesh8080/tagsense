# TagSense — AI-Powered Retroactive Resource Tagging

Deploy with two commands. Get a compliance report with AI-inferred tag recommendations. Review. Apply. Enforce.

## Quick Start

```bash
git clone https://github.com/lokesh8080/tagsense.git
cd tagsense
sam build
sam deploy --guided
```

SAM handles everything — packages Lambda code, uploads to S3, deploys CloudFormation. No manual zipping or shell scripts.

## Architecture

```
EventBridge / Manual Trigger
        │
  Step Functions Pipeline
        │
  Discovery Lambda ──► S3 (resource list as JSONL)
        │
  Distributed Map (Tiers 1-3, parallel, 40 concurrent)
    ├── Worker Lambda (batch of 20 resources)
    ├── Worker Lambda ...
    └── Worker Lambda ...
        │
  Aggregator Lambda ──► S3 (merged results)
        │
  Bedrock Batch Inference (Tier 4, async, 50% cheaper)
        │
  Poller (wait loop until batch completes)
        │
  Report Lambda ──► S3 (CSV + Summary) ──► SNS notification
        │
  Enforce Lambda ──► S3 (Tag Policy + SCP templates)
```

## How It Works

### 5 Inference Tiers — Accuracy Over Hype

| Tier | Method | Confidence | Cost |
|------|--------|------------|------|
| 1 | CloudFormation stack tags | ~99% | Free |
| 2 | CloudTrail creator lookup | ~95% | Free |
| 3 | Neighbor consensus (same VPC) | ~80% | Free |
| 4 | Amazon Bedrock Batch AI | ~60-71% | ~$0.003/resource |
| 5 | Manual flag + orphan detection | N/A | Free |

Tiers run in order. Deterministic first, AI last. Stops at first high-confidence match.

### Report & Review

Generates a CSV with an `Approve (Y/N)` column for human review. No tags applied without explicit approval.

### Apply (manual trigger, dry-run default)

```bash
# Dry run
aws lambda invoke --function-name tagsense-apply \
  --payload '{"run_id": "<run_id>", "dry_run": true}' response.json

# Apply approved tags
aws lambda invoke --function-name tagsense-apply \
  --payload '{"run_id": "<run_id>", "dry_run": false}' response.json
```

### Enforce

Generates ready-to-deploy templates:
- **Tag Policy** — enforce allowed values across the Organization
- **SCP** — deny resource creation without required tags
- **EventBridge rule** — auto-tag new resources at creation

## Customization

Set your required tags via the `TAG_POLICY` environment variable on the Lambda functions:

```json
{
  "Owner": {"required": true},
  "Environment": {"required": true, "allowed_values": ["prod", "staging", "dev", "sandbox"]},
  "CostCenter": {"required": true},
  "Application": {"required": true}
}
```

## Cost

~$1-5 per account scan. Bedrock Batch is 50% cheaper than real-time invocation.

## Limitations

- CloudTrail Tier 2 limited to 90-day default retention
- Business context tags (Compliance, SLA) require human knowledge
- Shared resources (NAT GW, TGW) flagged as ambiguous
- Max 50 user tags per resource — checked before recommending
