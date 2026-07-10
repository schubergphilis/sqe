import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";

/**
 * Throwaway test resources for the SQE glue-lake-formation quickstart:
 *   - an S3 bucket for the Iceberg warehouse (table data + metadata), and
 *   - a Glue *database* created here by CloudFormation.
 *
 * Creating the database out-of-band (CloudFormation, not SQE) is the whole
 * point. In a Lake-Formation-enabled account a CFN-created database is
 * LF-governed with NO grants, so the calling principal -- even a Lake Formation
 * admin -- is denied `CreateTable` on it until an explicit LF grant is made.
 * run.sh demonstrates that denial, then grants the principal LF permissions and
 * shows the same statement succeed.
 *
 * Contrast with the `aws-glue` quickstart, which lets SQE create the database
 * (making the caller its owner) to side-step LF. Here we keep LF in the loop.
 *
 * NOTE: this gates the Glue *catalog* operations (CreateTable/GetTable), which
 * is what SQE actually calls. SQE reads S3 data files directly with the caller's
 * IAM credentials, so it does NOT enforce LF column-masking or row-filtering;
 * that is a different mechanism (SQE's own OPA/Cedar policy engine).
 *
 * RemovalPolicy.DESTROY + autoDeleteObjects so `cdk destroy` removes the bucket
 * and its contents. run.sh deletes any SQE-created tables before destroy so the
 * database can be removed.
 */
export class GlueLakeFormationStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const databaseName = "sqe_lf_quickstart";

    const warehouse = new s3.Bucket(this, "Warehouse", {
      bucketName: `sqe-glue-lf-quickstart-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
    });

    new glue.CfnDatabase(this, "Database", {
      catalogId: this.account,
      databaseInput: {
        name: databaseName,
        description:
          "SQE glue-lake-formation quickstart. LF-governed; granted to the SQE principal by run.sh.",
      },
    });

    new cdk.CfnOutput(this, "WarehouseUri", {
      value: `s3://${warehouse.bucketName}/`,
      description: "S3 warehouse URI for SQE's glue backend.",
    });
    new cdk.CfnOutput(this, "DatabaseName", { value: databaseName });
    new cdk.CfnOutput(this, "Region", { value: this.region });
  }
}
