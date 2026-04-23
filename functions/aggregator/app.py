"""Aggregator Lambda v2 — unwraps Distributed Map output and aggregates results."""

import json
import boto3
from config import RESULTS_BUCKET, RESULTS_PREFIX, REGION


def handler(event, context):
    run_id = event["run_id"]
    region = event.get("region", REGION)
    s3 = boto3.client("s3")

    output_prefix = f"{RESULTS_PREFIX}/{run_id}/map_output/"

    all_results = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RESULTS_BUCKET, Prefix=output_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json") or "manifest" in obj["Key"]:
                continue
            try:
                body = s3.get_object(Bucket=RESULTS_BUCKET, Key=obj["Key"])["Body"].read()
                data = json.loads(body)

                # Distributed Map ResultWriter wraps results in execution metadata
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "Output" in item:
                            # Unwrap Step Functions execution envelope
                            output = item["Output"]
                            if isinstance(output, str):
                                output = json.loads(output)
                            # Worker returns a list of resource results
                            if isinstance(output, list):
                                all_results.extend(output)
                            elif isinstance(output, dict):
                                all_results.append(output)
                        elif isinstance(item, dict) and "arn" in item:
                            # Already unwrapped
                            all_results.append(item)
                elif isinstance(data, dict) and "arn" in data:
                    all_results.append(data)
            except Exception as e:
                print(f"Error processing {obj['Key']}: {e}")
                continue

    # Write aggregated results
    results_key = f"{RESULTS_PREFIX}/{run_id}/tier123_results.json"
    s3.put_object(Bucket=RESULTS_BUCKET, Key=results_key,
                  Body=json.dumps(all_results, default=str), ContentType="application/json")

    needs_bedrock = sum(1 for r in all_results if r.get("inference", {}).get("tier") == 4)
    resolved = len(all_results) - needs_bedrock

    print(f"Aggregated {len(all_results)} results: {resolved} resolved, {needs_bedrock} need Bedrock")

    return {
        "run_id": run_id, "region": region,
        "total_processed": len(all_results),
        "resolved_tiers_123": resolved,
        "needs_bedrock": needs_bedrock,
        "results_key": results_key,
    }
