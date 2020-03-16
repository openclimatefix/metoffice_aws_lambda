Simple AWS Lambda function to extract specific parts of the UK Met Office's UKV and MOGREPS-UK numerical weather predictions, compress, and save to S3 as Zarr.

## Installation on Linux

Install the AWS SAM CLI:

```shell
pip3 install --user aws-sam-cli
```

Add this line to `~/.bash_aliases`:

```
alias sam='python3 -m samcli'
```

And then load with `source ~/.bash_aliases`

Initialise the SAM project with `sam init`