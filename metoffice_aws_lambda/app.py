import xarray as xr
import numcodecs
import pandas as pd
import os
import lzma
import s3fs
import json
from typing import Dict
import time
import hashlib


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

    base_filename = (
        '{model_name}__{var_name}__{ref_time}__{valid_time}.zarr'.format(
            model_name=model_name,
            var_name=var_name,
            ref_time=forecast_ref_time.strftime('%Y-%m-%dT%H'),
            valid_time=valid_time.strftime('%Y-%m-%dT%H')))

    return path, base_filename


class FileExistsError(Exception):
    pass


def write_zarr_to_s3(dataset, dest_bucket, s3):
    zarr_path, base_zarr_filename = get_zarr_path_and_filename(dataset)
    zarr_path = os.path.join(dest_bucket, zarr_path)
    full_zarr_filename = os.path.join(zarr_path, base_zarr_filename)
    if s3.exists(full_zarr_filename):
        raise FileExistsError(
            'Destination already exists: {}'.format(full_zarr_filename))

    s3.makedirs(path=zarr_path)
    store = s3fs.S3Map(
        root=full_zarr_filename, s3=s3, check=False, create=True)

    lzma_filters = [
        dict(id=lzma.FILTER_DELTA, dist=4),
        dict(id=lzma.FILTER_LZMA2, preset=9)]
    compressor = numcodecs.LZMA(filters=lzma_filters, format=lzma.FORMAT_RAW)
    var_name = get_variable_name(dataset)
    encoding = {var_name: {'compressor': compressor}}

    dataset.to_zarr(store, mode='w', consolidated=True, encoding=encoding)
    return full_zarr_filename


class Timer:
    def __init__(self):
        self.t = time.time()
        self.times = []

    def tick(self, label=''):
        now = time.time()
        time_since_last_tick = now - self.t
        self.t = now
        self.times.append((label, '{:.2f}s'.format(time_since_last_tick)))

    def __str__(self):
        return str(self.times)


def lambda_handler(event: Dict, context: object) -> Dict:
    """Sample pure Lambda function.

    Parameters
    ----------
    event: dict, required

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """
    for sns_message in event['Records']:
        process_record(sns_message)


def extract_mo_message(sns_message):
    """Return Met Office (MO) message from SNS message."""
    body_json_string = sns_message['body']
    body_json_string = body_json_string.encode('utf-8')
    md5 = hashlib.md5(body_json_string)
    if md5.hexdigest() != sns_message['md5OfBody']:
        raise RuntimeError('MD5 checksum does not match!')

    body_dict = json.loads(body_json_string)
    mo_message = json.loads(body_dict['Message'])

    # Include SQS details in MO message.
    mo_message['sqs_message_id'] = sns_message['messageId']
    sns_attributes = sns_message['attributes']
    sent_timestamp = float(sns_attributes['SentTimestamp']) / 1000
    mo_message['message_sent_timestamp'] = pd.Timestamp.fromtimestamp(
        sent_timestamp)
    return mo_message


def process_record(sns_message):
    mo_message = extract_mo_message(sns_message)
    source_bucket = mo_message['bucket']
    source_key = mo_message['key']
    source_url = os.path.join(source_bucket, source_key)
    var_name = mo_message['name']
    dest_bucket = 'metoffice-nwp'
    is_multi_level = 'height' in mo_message and ' ' in mo_message['height']

    # do_copy is True if this message is about an NWP we want to process.
    do_copy = var_name in PARAMS_TO_COPY and is_multi_level

    print('do_copy=', do_copy,
          '; var_name=', var_name,
          '; is_multi_level=', is_multi_level,
          '; object_size={:,.1f} MB'.format(mo_message['object_size'] / 1E6),
          '; model=', mo_message['model'],
          '; message_sent_timestamp=', mo_message['message_sent_timestamp'],
          '; forecast_reference_time=', mo_message['forecast_reference_time'],
          '; created_time=', mo_message['created_time'],
          '; time=', mo_message['time'],
          '; source_url=', source_url,
          '; SQS_message_ID=', mo_message['sqs_message_id'],
          sep='')

    if do_copy:
        timer = Timer()
        s3 = s3fs.S3FileSystem()
        source_store = s3.open(source_url)
        timer.tick('open s3 file')
        dataset = load_and_filter_nc_file(source_store)
        timer.tick('load_and_filter_nc_file')
        try:
            full_zarr_filename = write_zarr_to_s3(dataset, dest_bucket, s3)
        except FileExistsError as e:
            print(e)
        else:
            timer.tick('write zarr to s3')
            print(timer)
            print('SUCCESS! dest_url=', full_zarr_filename, sep='')
