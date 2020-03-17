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
sam build && sam deploy
```


## Configure AWS permissions

