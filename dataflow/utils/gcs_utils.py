import json


def get_gcs_bucket_path_tuple(url):
    """Get a tuple with (bucket_name, bucket_path) based on the gs url"""
    url_parts_list = url.split('/')
    return url_parts_list[2], '/'.join(url_parts_list[3:])


def get_gcs_json_as_dict(url, client):
    """Parses a json file in gcs into a python dict"""
    bucket_name, blob_path = get_gcs_bucket_path_tuple(url)
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return json.loads(blob.download_as_string().decode('utf-8'))


def get_gcs_blob_list(url, client):
    """Get list of blobs based on gs url prefix provided"""
    bucket_name, blob_path = get_gcs_bucket_path_tuple(url)
    blob_list = []
    blobs = client.list_blobs(bucket_name, prefix=blob_path)
    for blob in blobs:
        if not blob.name.endswith('/'):
            blob_list.append('gs://{}/{}'.format(bucket_name, blob.name))
    return blob_list
