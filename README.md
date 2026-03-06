# Write Database tables to S3 Tables in Iceberg format using AWS Glue

This project demonstrate how to extract data from databases like Amazon Aurora and write to S3 Tables in Apache Iceberg format using AWS Glue.

## Challenge
Currently, there are no direct way to unload or extract tables directly to [Apache Iceberg](https://iceberg.apache.org/) format, let alone creating S3 tables directly from there.

## Solution Overview

This solution demonstrates how to extract data from Aurora MySQL Serverless database and write to S3 Tables in Apache Iceberg format using AWS Glue.

## Architecture Overview

![Write DB tables to S3 Architecture](images/aurora-mysql-to-s3tables-architecture.png)

The architecture consists of the following components:

1. **Amazon Aurora MySQL Serverless v2** - Source relational database containing the TICKIT sample dataset (users, venue, category, date, event, listing, and sales tables).
2. **AWS Secrets Manager** - Stores the Aurora MySQL database credentials securely.
3. **Amazon S3 (Staging Bucket)** - Stages the TICKIT sample data files downloaded from the public `redshift-downloads` S3 bucket.
4. **AWS Lambda (PyMySQL)** - Loads the staged TICKIT data files into the Aurora MySQL database using PyMySQL with `LOAD DATA LOCAL INFILE`.
5. **AWS Glue Job (Glue 5.0)** - Reads tables from Aurora MySQL via a native MYSQL connection and writes them to S3 Tables in Apache Iceberg format using the S3 Tables REST catalog endpoint.
6. **Amazon S3 Tables** - Target storage that stores the migrated tables in Apache Iceberg format.
7. **VPC with Private Subnets** - All resources run within private subnets with VPC endpoints for S3, S3 Tables, Glue, Secrets Manager, STS, CloudWatch Logs, and CloudFormation.

## Sample Data
We will be using the [TICKIT, a sample database that Amazon Redshift documentation examples use](https://docs.aws.amazon.com/redshift/latest/dg/c_sampledb.html). The data is available in public S3 bucket `s3://redshift-downloads/tickit/` as mentioned [here](https://docs.aws.amazon.com/redshift/latest/gsg/new-user-serverless.html).

## Prerequisites
This solution requires an AWS account. Follow this [link](https://aws.amazon.com/resources/create-account/) to create an AWS account if you do not have one. Specific permissions required for both account which will be set up in subsequent steps.

## Implementation walkthrough
Following are the step by step implementation guide.

### CloudFormation Parameters

The following parameters may be configured before deploying the CloudFormation stack:

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `S3TableBucketName` | Name of the S3 Tables bucket to create (or use existing) | — | Yes |
| `DatabaseName` | Name of the initial Aurora MySQL database | `tickit` | No |
| `MasterUsername` | Master username for Aurora MySQL | `admin` | No |
| `VpcCidr` | CIDR block for the VPC | `10.1.0.0/16` | No |
| `S3TableNamespace` | Namespace for S3 Tables | `tickit` | No |

### Step 1: Deploy the CloudFormation Stack

Deploy the CloudFormation template `scripts/aurora-mysql-to-s3tables-stack.yaml` using the AWS Console or the AWS CLI. If you are not using the AWS CLI then provide a name for the S3 Tables bucket, the stack will create it automatically (or use an existing one if it already exists):

1. In order to deploy the CloudFormation template using AWS Console head to the [AWS CloudFormation Console](https://console.aws.amazon.com/cloudformation/) and follow as mentioned in the screenshots below.

![CF1](images/CF1.png)
![CF2](images/CF2.png)
![CF3](images/CF3.png)
![CF4](images/CF4.png)
![CF5](images/CF5.png)
![CF_Create_in_progress](images/CF_Create_in_progress.png)
![CF_Create_Complete](images/CF_Create_Complete.png)

If you prefer using the AWS CLI, first upload the template to an S3 bucket (the template exceeds the 51,200 byte limit for inline `--template-body`), then create the stack:

```bash
# Upload the template to S3
aws s3 cp scripts/aurora-mysql-to-s3tables-stack.yaml \
  s3://<your-s3-bucket>/aurora-mysql-to-s3tables-stack.yaml \
  --region <your-region>

# Create the stack using the S3 template URL
aws cloudformation create-stack \
  --stack-name aurora-mysql-tickit-stack \
  --template-url https://<your-s3-bucket>.s3.<your-region>.amazonaws.com/aurora-mysql-to-s3tables-stack.yaml \
  --parameters \
    ParameterKey=S3TableBucketName,ParameterValue=<your-s3-table-bucket-name> \
  --capabilities CAPABILITY_NAMED_IAM \
  --region <your-region>
```

The stack will automatically:
- Create the S3 Tables bucket (or use existing if it already exists)
- Create a VPC with private subnets and VPC endpoints
- Provision an Aurora MySQL Serverless v2 cluster
- Download TICKIT sample data from the public AWS S3 bucket
- Load the sample data into Aurora MySQL via a Lambda function using PyMySQL
- Create a Glue job configured to migrate data to S3 Tables in Iceberg format

> **Note:** The S3 Tables bucket is retained when the stack is deleted to preserve your data.

### Step 2: Verify the Aurora MySQL data
From the **Outputs** tab of the CloudFormation stack, make a note of the *AuroraClusterEndpoint*, *DatabaseName*, *SecretArn*. Navigate to the [Amazon Aurora Console](https://console.aws.amazon.com/rds/), click on the *Query editor*, enter the values from the CloudFormation stack to connect. Then run the SQL commands to verify the data load as mentioned in the screenshots.


```sql
# Verify if the tables are created.
select * from information_schema.tables where table_schema = 'tickit';

# Verify if the data is loaded
SELECT 'users' as table_name, COUNT(*) as record_count FROM tickit.users
UNION ALL SELECT 'venue', COUNT(*) FROM tickit.venue
UNION ALL SELECT 'category', COUNT(*) FROM tickit.category
UNION ALL SELECT 'date', COUNT(*) FROM tickit.date
UNION ALL SELECT 'event', COUNT(*) FROM tickit.event
UNION ALL SELECT 'listing', COUNT(*) FROM tickit.listing
UNION ALL SELECT 'sales', COUNT(*) FROM tickit.sales;
```
![CF_Outputs](images/CF_Outputs.png)
![Aurora_Query_Editor](images/Aurora_Query_Editor.png)
![Connect_to_DB](images/Connect_to_DB.png)
![DB_Query1](images/DB_Query1.png)
![DB_Query2](images/DB_Query2.png)

### Step 3: Run the Glue Job

Go to the [AWS Glue Console](https://console.aws.amazon.com/gluestudio/), click **ETL jobs** under **Data Integration and ETL** from the left panel. Select the Glue job 'mysql-tickit-to-iceberg-job' and click *Run job* to start execution. Following are the steps as highlighted in the screen shots below or you can also kick off the ETL job using the command line below.

```bash
aws glue start-job-run --job-name mysql-tickit-to-iceberg-job --region <your-region>
```
![Glue1](images/Glue1.png)
![Glue2](images/Glue2.png)
![Glue3](images/Glue3.png)
![Glue4](images/Glue4.png)
![Glue5](images/Glue5.png)


### Step 4: Verify the Results

After the Glue job completes, verify that the tables have been created in your S3 Table bucket by clicking the *Table buckets* under **Buckets** from the [Amazon S3 Console](https://console.aws.amazon.com/s3/) or by using AWS CLI as mentioned below. The name of the s3 table bucket is `mysql-tickit-extract` in this demonstration.

```bash
aws s3tables list-tables \
  --table-bucket-arn arn:aws:s3tables:<your-region>:<your-account-id>:bucket/<your-s3-table-bucket-name> \
  --namespace tickit \
  --region <your-region>
```

Click on the `mysql-tickit-extract` S3 Table bucket, select a table from `tickit` namespace and then click **Preview**.

![S3_Table_Bucket1](images/S3_Table_Bucket1.png)
![S3_Table_Bucket2](images/S3_Table_Bucket2.png)
![S3_Table_Bucket3](images/S3_Table_Bucket3.png)

You can also query the migrated tables using Amazon Athena to validate the data.

### Step 5: Verify the Results using kiro-cli

1. Open an [AWS CloudShell](https://aws.amazon.com/cloudshell/), enter `kiro-cli` in the CloudShell.

![kiro-cli1](images/kiro-cli1.png)

2. Select the option to `Use with Builder ID`. Then open the URL to get authorized and *Allow* access to `kiro-cli`.

![kiro-cli2](images/kiro-cli2.png)
![kiro-cli3](images/kiro-cli3.png)
![kiro-cli4](images/kiro-cli4.png)
![kiro-cli5](images/kiro-cli5.png)
![kiro-cli6](images/kiro-cli6.png)

3. Set up the S3 Tables MCP Server.
After logging in to Kiro exit by entering `/quit` in the [Kiro CLI](https://kiro.dev/cli/) terminal.

![kiro-cli-mcp1](images/kiro-cli-mcp1.png)

Install UV package manager. Configure PATH and verify installation by running the following.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH" && which uvx
```

![kiro-cli-mcp2](images/kiro-cli-mcp2.png)

Create MCP configuration file. Locate and create the configuration directory using the following commands.

```bash
touch ~/.kiro/settings/mcp.json
ls -lrt ~/.kiro/settings/mcp.json
```

```bash
cat > ~/.kiro/settings/mcp.json << EOF
{
  "mcpServers": {
    "awslabs.s3-tables-mcp-server": {
      "command": "uvx",
      "args": [
        "awslabs.s3-tables-mcp-server@latest",
        "--allow-write"
      ],
      "env": {}
    }
  }
}
EOF
```

![kiro-cli-mcp3](images/kiro-cli-mcp3.png)
![kiro-cli-mcp4](images/kiro-cli-mcp4.png)

Verify the MCP by running the command `/mcp`
![kiro-cli-mcp5](images/kiro-cli-mcp5.png)

Run the following prompt. Change to appropriate s3 table bucket name 

`Count number of records in all tables in 'tickit' namespace of s3 table bucket 'mysql-tickit-extract' in us-west-2.`

The records counts should match the ones from Aurora Query Editor.

![kiro-cli-mcp6](images/kiro-cli-mcp6.png)


## Summary
This solution demonstrates how to extract data from Amazon Aurora MySQL and write to S3 Tables in Apache Iceberg format using AWS Glue. The CloudFormation stack automates the entire setup, including VPC networking, Aurora MySQL provisioning, sample data loading, and Glue job configuration. The Glue job uses the S3 Tables REST catalog endpoint with SigV4 authentication to write Iceberg tables directly to S3 Tables.

## Conclusion
This solution leverages AWS Glue's native Iceberg support and the S3 Tables REST catalog endpoint to bridge the gap between relational databases and modern lakehouse storage formats.

This architecture offers several advantages:
- **Fully automated setup** - A single CloudFormation stack provisions all required infrastructure, loads sample data, and configures the ETL pipeline.
- **Serverless and cost-efficient** - Aurora MySQL Serverless v2 and AWS Glue both scale based on demand, minimizing idle costs.
- **Iceberg table format** - Data is stored in Apache Iceberg format, enabling ACID transactions, schema evolution, and time travel queries.
- **Secure by design** - All resources run within private subnets with VPC endpoints, and database credentials are managed through AWS Secrets Manager.

## Code of Conduct
See [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md) for more information.

## Contributing Guidelines
See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License
This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file. 
