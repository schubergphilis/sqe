import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";

/**
 * Throwaway test resource for the SQE AWS Glue quickstart: an S3 bucket to hold
 * the Iceberg warehouse (table data + metadata).
 *
 * We deliberately do NOT create the Glue *database* here. SQE creates it via
 * `CREATE SCHEMA`, which makes the calling principal its owner. That matters in
 * Lake-Formation-enabled accounts (like this one): a database created out-of-band
 * by CloudFormation is LF-governed with no grants, so even an LF admin is denied
 * `Create Table` on it; a database SQE creates itself carries owner permissions.
 * Regular S3 is not an LF-registered data location, so writes use IAM access
 * control and the caller's bucket permissions suffice. Net: this works the same
 * with or without Lake Formation. (The dedicated glue-lake-formation quickstart
 * keeps the database LF-governed and grants explicit table/database-level LF
 * permissions; SQE does not enforce LF column/row masking.)
 *
 * RemovalPolicy.DESTROY + autoDeleteObjects so `cdk destroy` (run.sh teardown)
 * removes the bucket and its contents; run.sh drops the SQE-created database.
 */
export class GlueQuickstartStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const warehouse = new s3.Bucket(this, "Warehouse", {
      bucketName: `sqe-glue-quickstart-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
    });

    new cdk.CfnOutput(this, "WarehouseUri", {
      value: `s3://${warehouse.bucketName}/`,
      description: "S3 warehouse URI for SQE's glue backend.",
    });
    new cdk.CfnOutput(this, "Region", { value: this.region });
  }
}
