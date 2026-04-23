"""Bedrock Batch Lambda — submits unresolved resources to Bedrock Batch Inference."""

import json
import boto3
from datetime import datetime, timezone
from config import load_tag_policy, REGION, RESULTS_BUCKET, RESULTS_PREFIX, BEDROCK_MODEL_ID


def build_prompt(resource, tag_policy):
    existing = resource.get("tags", {})
    missing = resource.get("missing_tags", [])
    return f"""You are an AWS resource tagging assistant. Suggest tags for this resource.

Resource: ARN={resource['arn']}, Type={resource['resource_type']}
Existing tags: {json.dumps(existing)}

Required tags (suggest values for THESE missing ones only): {json.dumps({k:v for k,v in tag_policy.items() if k in missing})}

Rules:
- Use existing tags, resource name, and ARN to infer required tags.
- For Owner: look at creator, creatorUserId, lambda:createdBy tags.
- For Environment: look at Name patterns (prod, dev, staging).
- For Application: look at Name, AmazonDataZoneProject, stack name patterns.
- For CostCenter: only suggest if clear evidence. Otherwise say "unknown".
- State confidence: high/medium/low per tag.
- If you cannot determine a value, say "unknown".
- Respond ONLY with JSON: {{"tags": {{"Key": {{"value":"...","confidence":"high|medium|low","reasoning":"..."}}}}}}"""


def handler(event, context):
    """Reads Tier 1-3 results, submits unresolved resources to Bedrock Batch."""
    run_id = event["run_id"]
    region = event.get("region", REGION)
    tag_policy = load_tag_policy()

    s3 = boto3.client("s3")
    bedrock = boto3.client("bedrock", region_name=region)

    # Read aggregated results from Distributed Map output
    results_key = f"{RESULTS_PREFIX}/{run_id}/tier123_results.json"
    obj = s3.get_object(Bucket=RESULTS_BUCKET, Key=results_key)
    all_results = json.loads(obj["Body"].read())

    # Separate resolved vs needs-bedrock
    resolved = [r for r in all_results if r.get("inference", {}).get("tier") not in (4,)]
    needs_bedrock = [r for r in all_results if r.get("inference", {}).get("tier") == 4]

    if not needs_bedrock:
        # No Bedrock needed — write final results
        final_key = f"{RESULTS_PREFIX}/{run_id}/inference.json"
        output = _build_output(run_id, region, all_results)
        s3.put_object(Bucket=RESULTS_BUCKET, Key=final_key,
                      Body=json.dumps(output, default=str), ContentType="application/json")
        return {"run_id": run_id, "s3_key": final_key, "batch_job_id": None,
                "bedrock_count": 0, **_tier_summary(all_results)}

    # Build JSONL input for Bedrock Batch
    batch_lines = []
    for i, resource in enumerate(needs_bedrock):
        record = {
            "recordId": str(i),
            "modelInput": {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 500,
                "messages": [{"role": "user", "content": build_prompt(resource, tag_policy)}]
            }
        }
        batch_lines.append(json.dumps(record))

    input_key = f"{RESULTS_PREFIX}/{run_id}/bedrock_batch_input.jsonl"
    s3.put_object(Bucket=RESULTS_BUCKET, Key=input_key,
                  Body="\n".join(batch_lines), ContentType="application/jsonlines")

    output_prefix = f"s3://{RESULTS_BUCKET}/{RESULTS_PREFIX}/{run_id}/bedrock_batch_output/"

    # Submit batch job
    job = bedrock.create_model_invocation_job(
        jobName=f"tagsense-{run_id}",
        modelId=BEDROCK_MODEL_ID,
        roleArn=event.get("bedrock_role_arn", ""),
        inputDataConfig={
            "s3InputDataConfig": {
                "s3InputFormat": "JSONL",
                "s3Uri": f"s3://{RESULTS_BUCKET}/{input_key}"
            }
        },
        outputDataConfig={
            "s3OutputDataConfig": {"s3Uri": output_prefix}
        }
    )

    # Save state for the poller
    state = {
        "run_id": run_id, "region": region,
        "batch_job_arn": job["jobArn"],
        "resolved": resolved,
        "needs_bedrock": needs_bedrock,
        "output_prefix": output_prefix,
    }
    s3.put_object(Bucket=RESULTS_BUCKET, Key=f"{RESULTS_PREFIX}/{run_id}/batch_state.json",
                  Body=json.dumps(state, default=str), ContentType="application/json")

    return {
        "run_id": run_id,
        "batch_job_arn": job["jobArn"],
        "bedrock_count": len(needs_bedrock),
        "resolved_count": len(resolved),
    }


def _build_output(run_id, region, results):
    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    orphans = []
    for r in results:
        t = r.get("inference", {}).get("tier", 5)
        tier_counts[t] = tier_counts.get(t, 0) + 1
        if r.get("inference", {}).get("is_likely_orphan"):
            orphans.append(r["arn"])
    return {
        "run_id": run_id, "region": region,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_inferred": len(results),
        "tier_breakdown": tier_counts,
        "orphan_candidates": orphans,
        "recommendations": results,
    }


def _tier_summary(results):
    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    orphans = []
    for r in results:
        t = r.get("inference", {}).get("tier", 5)
        tier_counts[t] = tier_counts.get(t, 0) + 1
        if r.get("inference", {}).get("is_likely_orphan"):
            orphans.append(r["arn"])
    return {"tier_breakdown": tier_counts, "orphan_count": len(orphans), "total_inferred": len(results)}
