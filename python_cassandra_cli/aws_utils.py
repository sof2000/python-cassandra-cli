
import boto3
from botocore.exceptions import ClientError
import logging

class AWSUtils(object):

    def __init__(self, id, key):

        self.id = id
        self.key = key

    def s3_client(self) :
         
        # try:
        client_s3 = boto3.client(
            's3',
            aws_access_key_id = self.id,
            aws_secret_access_key = self.key,
        )

        # except ClientError as e:
        #     logging.error(e)

        return client_s3

    @classmethod
    def ssm_client(cls, id, key, region) :

        cls.id = id
        cls.key = key
        cls.region = region

        client_ssm = boto3.client(
            'ssm',
            aws_access_key_id = cls.id,
            aws_secret_access_key = cls.key,
            region_name = cls.region
        )

        return client_ssm