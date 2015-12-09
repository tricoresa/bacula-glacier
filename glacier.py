__author__ = 'matt'

import boto3

g = boto3.resource('glacier')

def upload_glacier(filename, vault, description):
    with open(filename, 'rb') as f:
        g.upload_archive(vaultName=vault, archiveDescription=description, body=f)
    return