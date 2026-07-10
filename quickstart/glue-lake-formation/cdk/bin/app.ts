#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { GlueLakeFormationStack } from "../lib/glue-lf-quickstart-stack";

const app = new cdk.App();

// Account + region come from the AWS provider chain (AWS_PROFILE / AWS_REGION)
// that run.sh exports, so the stack targets the same account as SQE.
new GlueLakeFormationStack(app, "SqeGlueLfQuickstart", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
