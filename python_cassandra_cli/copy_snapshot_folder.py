import os
import sys
from python_cassandra_cli.aws_utils import AWSUtils

class CopySnapshotFolder(object):
    
    def __init__(self, s3, id, key, snapshot_folder):

        self.s3 = s3
        self.id = id
        self.key = key
        self.snapshot_folder = snapshot_folder

    def copy_snapshot_folder(self):
        try:
            copy_snapshot = AWSUtils(self.id, self.key)

            client_s3 = copy_snapshot.s3_client()

            list=client_s3.list_objects(Bucket=self.s3, Prefix=self.snapshot_folder)['Contents']
            for s3_key in list:
                s3_object = s3_key['Key']
                # print object
                # print(s3_object)
                # creating folder for object
                os.makedirs(os.path.dirname(s3_object), exist_ok=True)
                # copy object from s3 to local folder
                client_s3.download_file(self.s3, s3_object, s3_object)
        except Exception as e:
            print("Unable to copy-snapshot-folder due to", e)
            sys.exit(1)