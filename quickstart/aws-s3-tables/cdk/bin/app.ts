#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { S3TablesQuickstartStack } from "../lib/s3tables-quickstart-stack";

const app = new cdk.App();

new S3TablesQuickstartStack(app, "SqeS3TablesQuickstart", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
