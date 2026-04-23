#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/src"
PKG_DIR="$SCRIPT_DIR/packages"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=${AWS_REGION:-us-east-1}
BUCKET="tagsense-deploy-${ACCOUNT_ID}"
STACK_NAME="tagsense"

echo "=== TagSense v2 Deployment ==="
echo "Account: $ACCOUNT_ID | Region: $REGION"

if ! aws s3 ls "s3://$BUCKET" 2>/dev/null; then
    echo "Creating deployment bucket: $BUCKET"
    aws s3 mb "s3://$BUCKET" --region "$REGION"
fi

# Package each Lambda (each gets config.py + its handler)
LAMBDAS="discovery inference_worker aggregator bedrock_batch bedrock_poller report apply enforce"
for fn in $LAMBDAS; do
    echo "Packaging $fn..."
    rm -rf "$PKG_DIR/$fn"
    mkdir -p "$PKG_DIR/$fn"
    cp "$SRC_DIR/config.py" "$PKG_DIR/$fn/"
    cp "$SRC_DIR/$fn.py" "$PKG_DIR/$fn/"
    cd "$PKG_DIR/$fn"
    zip -q "../${fn}.zip" *.py
    cd "$SCRIPT_DIR"
    aws s3 cp "$PKG_DIR/${fn}.zip" "s3://$BUCKET/lambdas/${fn}.zip" --quiet
done
echo "All Lambdas packaged and uploaded."

# Delete old stack if it exists (v1 → v2 has breaking changes)
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null; then
    echo "Deleting old stack..."
    aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
    aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
    echo "Old stack deleted."
fi

echo "Deploying CloudFormation stack: $STACK_NAME"
aws cloudformation deploy \
    --template-file "$SCRIPT_DIR/template.yaml" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides DeployBucket="$BUCKET" \
    --region "$REGION" \
    --no-fail-on-empty-changeset

echo ""
echo "=== Deployment Complete ==="
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs" --output table

SM_ARN=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" --output text)
echo ""
echo "To run: aws stepfunctions start-execution --state-machine-arn $SM_ARN --input '{\"region\": \"$REGION\"}'"
