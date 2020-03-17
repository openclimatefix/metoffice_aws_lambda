Simple AWS Lambda function to extract specific parts of the UK Met Office's UKV and MOGREPS-UK numerical weather predictions, compress, and save to S3 as Zarr.

## Installation on Linux

Install conda environment.  Then find where Python is installed for your conda env (e.g. `~/
miniconda3/envs/metoffice_aws_lambda/bin/pip`) and run:

```shell
~/miniconda3/envs/metoffice_aws_lambda/bin/pip install aws-sam-cli
```

Add this line to `~/.bash_aliases`:

```
alias sam='python3 -m samcli'
```

And then load with `source ~/.bash_aliases`

Build and deploy with:

```
sam build
sam deploy --guided
```

SAM will then ask some questions.  These are reasonable answers:

```
Stack name                      : metoffice-aws-lambda
Region                          : eu-west-2
Confirm changeset               : N
Allow SAM CLI IAM role creation : Y
Save arguments to samconfig.toml: Y
```

### Configure AWS permissions

Go to the AWS Identity and Access Management (IAM) console, and attach policy `AWSLambdaSQSQueueExecutionRole` to the role for the metoffice-aws-lambda function.

### Create bucket for storing NWPs

Create a bucket called `metoffice-nwp` (or change the value of `target_bucket` in `app.py`).

## Configure AWS Simple Queue Service (SQS)

When the Met Office uploads new NWPs to S3, they also send a message to an AWS Simple Notification Service topic.  It is possible to trigger lambda functions directly from SNS notifications.  However, this results in the lambda function sometimes triggering too soon.  This often means the lambda function will take a long time (300 seconds) to download the NetCDF file from S3; and sometimes means the lambda function cannot find the NetCDF file at all.

A solution is to set up a Simple Queue Service, and the the SQS to delay messages a little, to ensure that the NetCDF files are ready and waiting on S3 by the time our lambda function triggers.

Set up SQS as per the [Met Office's instructions](https://github.com/MetOffice/aws-earth-examples/blob/master/examples/2.%20Subscribing%20to%20data.ipynb).

Additionally, set these config options on the queue:

* Default Visibility Timeout = 1000 seconds (must be larger than the timeout for the Lambda function, which is currently 900 seconds).
* Delivery Delay = 15 minutes (to allow time for each NetCDF file to replicate across S3)

## Configure SQS to trigger lambda function

Go to the AWS Lambda console. Click 'Add trigger'. Add the SQS queue you created above.  Set batch size to 1.

## Deleting deployment

* Manually delete:
  - S3 bucket with the code
  - the lambda function
  - any roles created by SAM
* Run `aws cloudformation delete-stack --stack-name metoffice-aws-lambda`