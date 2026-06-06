import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3tables from "aws-cdk-lib/aws-s3tables";

/**
 * Throwaway test resource for the SQE AWS S3 Tables quickstart: a single S3
 * Tables *table bucket* (AWS's managed-Iceberg storage). SQE creates the
 * namespace + table inside it via the S3 Tables API.
 *
 * We create only the bucket (not a namespace/table) for the same reason as the
 * Glue quickstart: a SQE-created namespace makes the caller its owner, which is
 * what works cleanly under Lake Formation, whereas resources created out-of-band
 * are governed with no grants.
 *
 * RemovalPolicy.DESTROY so `cdk destroy` removes the bucket -- but S3 Tables
 * refuses to delete a non-empty table bucket, so run.sh deletes the SQE-created
 * table + namespace first (see run.sh `destroy`).
 */
export class S3TablesQuickstartStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const bucket = new s3tables.CfnTableBucket(this, "TableBucket", {
      // Lowercase, 3-63 chars, S3-Tables naming rules.
      tableBucketName: "sqe-s3tables-quickstart",
    });
    bucket.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    new cdk.CfnOutput(this, "TableBucketArn", {
      value: bucket.attrTableBucketArn,
      description: "S3 Tables table-bucket ARN for SQE's s3tables backend.",
    });
    new cdk.CfnOutput(this, "Region", { value: this.region });
  }
}
