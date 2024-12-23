import os
import requests
import socket
import json
from aws_s3_manager import S3Manager
from render_file_manager import RenderFileManager


def get_host():
    """
    Determines the host on which the script is deployed.
    Returns:
        str: The name of the running environment.
    """

    # check if running on aws lambda
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
        return "Amazon"

    # check if running on render
    if os.getenv('RENDER') == 'true':
        return "Render"

    # otherwise, return the host name of the computer
    return socket.gethostname()


def _initialize_amazon():
    """
    Initializes the AWS S3 Manager and returns the manager and bucket name.
    Returns:
        S3Manager: The AWS S3 Manager object.
        str: The name of the bucket.
    """
    access_key = os.getenv('AWS_WATCHDOG_ACCESS_KEY')
    secret_key = os.getenv('AWS_WATCHDOG_SECRET_KEY')
    bucket = os.getenv('AWS_WATCHDOG_BUCKET')
    if (access_key is None) or (secret_key is None) or (bucket is None):
        raise EnvironmentError('One or more Environment Variables not available')
    s3_manager = S3Manager(access_key, secret_key)
    return s3_manager, bucket


def _initialize_render():
    """
    Initializes the Render File Manager and returns the manager.
    Returns:
        RenderFileManager: The Render File Manager object.
    """
    API_KEY = os.getenv('RENDER_FILE_API_KEY')
    BASE_URL = 'https://file-manager-vist.onrender.com'
    manager = RenderFileManager(BASE_URL, API_KEY)
    return manager


def _write_to_amazon(file_name, data, data_format='text'):
    """
    Writes data to a file in AWS S3.

    Args:
        file_name (str): The name of the file to write to.
        data (str or dict): The data to write to the file.
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        error (str): An error message if the write operation failed.
    """

    # initialize aws s3 storage
    s3_manager, bucket = _initialize_amazon()

    # convert json to text if necessary
    if data_format == 'json':
        data = json.dumps(data)
    elif data_format != 'text':
        raise TypeError('Unsupported data_format. Only text and json are supported.')

    # write data to bucket
    error = s3_manager.put_text(bucket, file_name, data)
    return error


def _write_to_render(file_name, data, data_format='text'):
    """
    Write data to a file in Render's file manager.

    Args:
        file_name (str): The name of the file to write to.
        data (str or dict): The data to write to the file.
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        error (str): An error message if the write operation failed.
    """

    # initialize
    manager = _initialize_render()

    # convert json to text if necessary
    if data_format == 'json':
        data = json.dumps(data)
    elif data_format != 'text':
        raise TypeError('Unsupported data_format. Only text and json are supported.')

    # write data to file
    error = manager.put_object(file_name, data, data_format='text')
    return error


def _write_to_local(file_path, data, data_format='text'):
    """
    Writes data to a local file.

    Args:
        file_path (str): The path of the file to write to.
        data (str or dict): The data to write to the file.
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        error (str): An error message if the write operation failed.
    """

    # convert json to text if necessary
    if data_format == 'json':
        data = json.dumps(data)
    elif data_format != 'text':
        raise TypeError('Unsupported data_format. Only text and json are supported.')

    # write data to file
    try:
        with open(file_path, 'w') as f:
            f.write(data)
            error = ''
    except Exception as e:
        error = f'[Local Write Error] {str(e)}'
    return error


def _read_from_amazon(file_name, data_format='text'):
    """
    Reads data from a file in AWS S3.

    Args:
        file_name (str): The name of the file to read from.
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        data (str or dict): The data read from the file.
        error (str): An error message if the read operation failed.
    """

    # initialize aws s3 storage
    s3_manager, bucket = _initialize_amazon()

    # read data from bucket
    data, error = s3_manager.get_text(bucket, file_name)
    if error:
        return None, error

    # convert text to json if necessary
    if data_format == 'json':
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            return None, f'[Amazon Read Error] {str(e)}'
    elif data_format != 'text':
        return None, 'Unsupported data_format. Only text and json are supported.'

    return data, ''


def _read_from_render(file_name, data_format='text'):
    """
    Reads data from a file in Render's file manager.

    Args:
        file_name (str): The name of the file to read from.
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        data (str or dict): The data read from the file.
        error (str): An error message if the read operation failed.
    """

    # initialize
    manager = _initialize_render()

    # read data from file
    data, error = manager.get_object(file_name, data_format)
    if error:
        return None, error

    # convert text to json if necessary
    if data_format == 'json' and isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            return None, f'[Render Read Error] {str(e)}'
    elif data_format != 'text':
        return None, 'Unsupported data_format. Only text and json are supported.'

    return data, ''


def _read_from_local(file_path, data_format='text'):
    """
    Reads data from a local file.

    Args:
        file_path (str): The path of the file to read from.
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        data (str or dict): The data read from the file.
        error (str): An error message if the read operation failed.
    """

    # read data from file
    try:
        with open(file_path, 'r') as f:
            data = f.read()
    except Exception as e:
        return None, f'[Local Read Error] {str(e)}'

    # convert text to json if necessary
    if data_format == 'json':
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            return None, f'[Local Read Error] {str(e)}'
    elif data_format != 'text':
        return None, 'Unsupported data_format. Only text and json are supported.'

    return data, ''


def _upload_to_amazon(local_file_path, file_name):
    """
    Uploads a local file to AWS S3.

    Args:
        local_file_path (str): The path of the local file to upload.
        file_name (str): The name of the file to upload to.

    Returns:
        error (str): An error message if the upload operation failed.
    """
    s3_manager, bucket = _initialize_amazon()
    error = s3_manager.upload_file(local_file_path, bucket, file_name)
    return error


def _upload_to_render(local_file_path, file_name):
    """
    Uploads a local file to Render's file manager.

    Args:
        local_file_path (str): The path of the local file to upload.
        file_name (str): The name of the file to upload to.

    Returns:
        error (str): An error message if the upload operation failed.
    """
    manager = _initialize_render()
    error = manager.upload_file(local_file_path, file_name)
    return error


def _download_from_amazon(file_name, local_file_path):
    """
    Downloads a file from AWS S3 to a local file.

    Args:
        file_name (str): The name of the file to download.
        local_file_path (str): The path where to save the file locally.

    Returns:
        error (str): An error message if the download operation failed.
    """
    s3_manager, bucket = _initialize_amazon()
    error = s3_manager.download_file(bucket, file_name, local_file_path)
    return error


def _download_from_render(file_name, local_file_path):
    """
    Downloads a file from Render's file manager to a local file.

    Args:
        file_name (str): The name of the file to download.
        local_file_path (str): The path where to save the file locally.

    Returns:
        error (str): An error message if the download operation failed.
    """
    manager = _initialize_render()
    error = manager.download_file(file_name, local_file_path)
    return error


def put(file_name, data, host, data_format='text'):
    """
    Writes data to a file in the specified host.

    Args:
        file_name (str): The name of the file to write to.
        data (str or dict): The data to write to the file.
        host (str): The host to write the file to ('Amazon', 'Render', or local).
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        error (str): An error message if the write operation failed.
    """

    if host == 'Amazon':
        error = _write_to_amazon(file_name, data, data_format)
    elif host == 'Render':
        error = _write_to_render(file_name, data, data_format)
    else:
        error = _write_to_local(file_name, data, data_format)

    return error


def get(file_name, host, data_format='text'):
    """
    Reads data from a file in the specified host.

    Args:
        file_name (str): The name of the file to read from.
        host (str): The host to read the file from ('Amazon', 'Render', or local).
        data_format (str): The format of the data ('text' or 'json').

    Returns:
        data (str or dict): The data read from the file.
        error (str): An error message if the read operation failed.
    """

    if host == 'Amazon':
        data, error = _read_from_amazon(file_name, data_format)
    elif host == 'Render':
        data, error = _read_from_render(file_name, data_format)
    else:
        data, error = _read_from_local(file_name, data_format)

    return data, error


def upload(local_file_path, file_name, host):
    """
    Uploads a local file to the specified host.

    Args:
        local_file_path (str): The path of the local file to upload.
        file_name (str): The name of the file to upload to.
        host (str): The host to upload the file to ('Amazon', 'Render', or local).

    Returns:
        error (str): An error message if the upload operation failed.
    """
    if host == 'Amazon':
        error = _upload_to_amazon(local_file_path, file_name)
    elif host == 'Render':
        error = _upload_to_render(local_file_path, file_name)
    else:
        error = ''

    return error


def download(file_name, local_file_path, host):
    """
    Downloads a file from the specified host to a local file.

    Args:
        file_name (str): The name of the file to download.
        local_file_path (str): The path where to save the file locally.
        host (str): The host to download the file from ('Amazon', 'Render', or local).

    Returns:
        error (str): An error message if the download operation failed.
    """
    if host == 'Amazon':
        error = _download_from_amazon(file_name, local_file_path)
    elif host == 'Render':
        error = _download_from_render(file_name, local_file_path)
    else:
        error = ''

    return error
