#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { GlueQuickstartStack } from "../lib/glue-quickstart-stack";

const app = new cdk.App();

// Account + region come from the AWS provider chain (AWS_PROFILE / AWS_REGION)
// that run.sh exports, so the stack targets the same account as SQE.
new GlueQuickstartStack(app, "SqeGlueQuickstart", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
