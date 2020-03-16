import xarray as xr
import numpy as np
import numcodecs
import pandas as pd
import os
import lzma
import s3fs
import json
from typing import Dict


PARAMS_TO_COPY = [
    'wind_speed',
    'wind_speed_of_gust',
    'wind_from_direction']

HEIGHT_METERS = [10, 50, 100, 150]

# Approximate boundaries of UKV data from JASMIN, projected into
# MOGREPS-UK's Lambert Azimuthal Equal Area projection.
NORTH =  668920.2182797253
SOUTH = -742783.9449856092
EAST  =  494613.07597373443
WEST  = -611744.985010537


def load_and_filter_nc_file(file_obj):
    dataset = xr.open_dataset(file_obj, engine='h5netcdf')
    dataset = dataset.sel(height=HEIGHT_METERS).loc[
        dict(
            projection_x_coordinate=slice(WEST, EAST),  
            projection_y_coordinate=slice(SOUTH, NORTH))]

    return dataset


def get_variable_name(dataset):
    return list(dataset.data_vars.keys())[0]


def get_zarr_path_and_filename(dataset):
    forecast_ref_time = dataset.forecast_reference_time.values
    forecast_ref_time = pd.Timestamp(forecast_ref_time)
    valid_time = dataset.time.values
    valid_time = pd.Timestamp(valid_time)
    var_name = get_variable_name(dataset)
    model_name = dataset.attrs['title'].split()[0]

    path = os.path.join(
        model_name,
        var_name,
        forecast_ref_time.strftime('%Y/m%m/d%d/h%H'))
    
    base_filename = '{model_name}__{var_name}__{ref_time}__{valid_time}.zarr'.format(
        model_name=model_name,
        var_name=var_name,
        ref_time=forecast_ref_time.strftime('%Y-%m-%dT%H'),
        valid_time=valid_time.strftime('%Y-%m-%dT%H'))
    
    return path, base_filename


class FileExistsError(Exception):
    pass


def write_zarr_to_s3(dataset, dest_bucket, s3):
    zarr_path, base_zarr_filename = get_zarr_path_and_filename(dataset)
    zarr_path = os.path.join(dest_bucket, zarr_path)
    full_zarr_filename = os.path.join(zarr_path, base_zarr_filename)
    if s3.exists(full_zarr_filename):
        raise FileExistsError('Destination already exists: {}'.format(full_zarr_filename))

    s3.makedirs(path=zarr_path)
    store = s3fs.S3Map(root=full_zarr_filename, s3=s3, check=False, create=True)
    
    lzma_filters = [
        dict(id=lzma.FILTER_DELTA, dist=4),
        dict(id=lzma.FILTER_LZMA2, preset=9)]
    compressor = numcodecs.LZMA(filters=lzma_filters, format=lzma.FORMAT_RAW)
    var_name = get_variable_name(dataset)
    encoding = {var_name: {'compressor': compressor}}
    
    dataset.to_zarr(store, mode='w', consolidated=True, encoding=encoding)
    return full_zarr_filename


def lambda_handler(event: Dict, context: object) -> Dict:
    """Sample pure Lambda function.

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    
    status_code = 200
    
    mo_message = event['Records'][0]['Sns']['Message']
    mo_message = json.loads(mo_message)
    source_bucket = mo_message['bucket']
    source_key = mo_message['key']
    source_url = os.path.join(source_bucket, source_key)
    var_name = mo_message['name']
    dest_bucket = context.function_name
    do_copy = var_name in PARAMS_TO_COPY and 'height' in mo_message and ' ' in mo_message['height']
    dest_exists = False
    status_body = {
        'source_url': source_url,
        'dest_bucket': dest_bucket,
        'var_name': var_name,
        'do_copy': do_copy}

    if do_copy:
        try:
            s3 = s3fs.S3FileSystem()
            source_store = s3.open(source_url)
            dataset = load_and_filter_nc_file(source_store)
            full_zarr_filename = write_zarr_to_s3(dataset, dest_bucket, s3)
        except FileExistsError as e:
            status_body['report'] = str(e)
        except Exception as e:
            status_code = 500
            status_body['report'] = str(e)
        else:
            status_body['dest_url'] = full_zarr_filename
            status_body['report'] = 'success'

    return {
        'statusCode': status_code,
        'body': status_body}
