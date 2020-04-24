#!/usr/bin/python3


##
# Plasma Component for using Amazon S3 storage
# Author : vector@s20.ai
##


from boto3.session import Session
import os
import logging
import sys

# Setup Logger
logger = logging.getLogger('S3-Connector')
session = None


def get_session(S3_ACCESS_KEY, S3_SECRET_KEY):
    global session
    session = Session(
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY)
    return session


def get_s3_bucket(args):
    session = get_session()
    s3 = session.resource('s3')
    bucket = s3.Bucket(args['bucket_name'])
    return bucket


def list_files(args):
    try:
        bucket_name = args['bucket_name']
        bucket = get_s3_bucket(bucket_name)
        file_list = []
        for s3_file in bucket.objects.all():
            files.append(s3_file.key)
        return file_list
    except Exception as e:
        logger.error('Exception ' + str(e))
        return False


def download(args):
    try:
        bucket_name = args['bucket_name']
        file_name = args['file_name']
        file_path = args['file_path']
        bucket = get_s3_bucket(bucket_name)
        bucket.download_file(file_name, file_path)
        return True
    except Exception as e:
        logger.error('Exception ' + str(e))
        return False


def upload(args):
    try:
        bucket_name = args['bucket_name']
        file_name = args['file_name']
        file_path = args['file_path']
        bucket = get_s3_bucket(bucket_name)
        bucket.upload_file(file_path, file_name)
        return True
    except Exception as e:
        logger.error('Exception ' + str(e))
        return False


def delete(args):
    try:
        bucket_name = args['bucket_name']
        file_name = args['file_name']
        file_version = args['file_version']
        response = bucket.delete_objects(
            Delete={
                'Objects': [{'Key': file_name, 'VersionId': file_version}, ],
                'Quiet': True
            },
            MFA='string',
            RequestPayer='requester',
            BypassGovernanceRetention=True
        )
        return response
    except Exception as e:
        logger.error('Exception ' + str(e))
        return False


def main(args):
    try:
        session = get_session(args['S3_SECRET_KEY'], args['S3_ACCESS_KEY'])
        print(args)
        if args['operation'] == 'list':
            files = list_files(args)
            return files
        elif args['operation'] == 'download':
            success = Download(args)
            return success
        elif args['operation'] == 'upload':
            success = upload(args)
            return success
        elif args['operation'] == 'delete':
            success = delete(args)
            return success
    except Exception as e:
        logger.error('Exception '+str(e))
        return False
