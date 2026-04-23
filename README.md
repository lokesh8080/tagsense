# TagSense — AI-Powered Retroactive Resource Tagging

Deploy a single CloudFormation stack. Get a compliance report with AI-inferred tag recommendations. Review. Apply. Enforce.

## Architecture

```
EventBridge (weekly) ──► Step Functions Pipeline
                              │
                    ┌─────────┼─────────────┐
                    ▼         ▼             ▼
               Discovery → Inference → Report + Enforce
                    │         │             │
                    └─────────┴──────┬──────┘
                                     ▼
                              S3 (results)
                                     │
                              SNS (notification)
                                     │
                              Human reviews CSV
                                     │
                              Apply Lambda (manual trigger)
```

## Quick Start

```bash
# Deploy
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name tagsense \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    NotificationEmail=your-email@example.com \
    ScheduleExpression="rate(7 days)"

# Run on-demand
aws stepfunctions start-execution \
  --state-machine-arn <StateMachineArn from outputs> \
  --input '{"region": "us-east-1"}'

# After reviewing the CSV in S3, apply approved tags
aws lambda invoke \
  --function-name tagsense-apply \
  --payload '{"run_id": "<run_id>", "dry_run": false}' \
  response.json
```

## How It Works

### 1. Discovery
Scans all resources via Resource Groups Tagging API. Scores compliance against your tag policy. Outputs gap list.

### 2. Inference (5 tiers — accuracy over hype)

| Tier | Method | Accuracy | Cost |
|------|--------|----------|------|
| 1 | CloudFormation stack tags | ~99% | Free |
| 2 | CloudTrail creator lookup | ~95% | Free |
| 3 | Neighbor consensus (same VPC/subnet) | ~80% | Free |
| 4 | Amazon Bedrock AI | ~60% | ~$0.01/resource |
| 5 | Manual flag + orphan detection | N/A | Free |

Tiers run in order. Stops at first high-confidence match. AI is the last resort, not the first.

### 3. Report
Generates:
- **CSV** for human review (with Approve Y/N column)
- **Summary** with compliance score and tier breakdown
- **SNS notification** when ready

### 4. Apply (manual trigger only)
Reads the reviewed CSV. Only applies tags where `Approve = Y`. Full audit trail in S3.

**Dry-run by default.** Must explicitly pass `dry_run: false`.

### 5. Enforce
Generates ready-to-deploy templates:
- **Tag Policy** — enforce allowed values across the Organization
- **SCP** — deny resource creation without required tags
- **EventBridge rule** — auto-tag new resources at creation

## Customization

Edit the tag policy in the Lambda environment variable `TAG_POLICY`:

```json
{
  "Owner": {"required": true},
  "Environment": {"required": true, "allowed_values": ["prod", "staging", "dev"]},
  "CostCenter": {"required": true},
  "Application": {"required": true}
}
```

## Cost

~$1-5 per account scan. Bedrock is only invoked for resources that fail deterministic tiers.

## Limitations

- CloudTrail Tier 2 limited to 90-day default retention
- Business context tags (Compliance, SLA) require human knowledge — AI won't guess
- Shared resources (NAT GW, TGW) flagged as ambiguous, not force-tagged
- Max 50 user tags per resource — checked before recommending
- Not all resource types support the Tagging API

## Multi-Account Deployment

Deploy as a CloudFormation StackSet across your Organization. Each account gets its own scan, results go to a central S3 bucket.
