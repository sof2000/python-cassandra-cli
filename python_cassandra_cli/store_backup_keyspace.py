from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
from cassandra.query import dict_factory
import boto3
import time
import os
import sys
import re
import shutil
import time
import datetime
import cassandra.util
from python_cassandra_cli.aws_utils import AWSUtils
from cassandra.query import SimpleStatement
import copy
from cassandra import ConsistencyLevel

import dotenv

dotenv.load_dotenv()


class StoreBackupKeyspace(object):
    
    def __init__(self, keyspace, s3, id, key, schema, host, user, password, no_clear_snapshot, ssm_secret, region,secret_name, 
        ssl_cqlsh, snapshot_folder_override):

        self.keyspace = keyspace
        self.s3 = s3
        self.id = id
        self.key = key
        self.schema = schema
        self.host = host
        self.user = user
        self.password = password
        self.no_clear_snapshot = no_clear_snapshot
        self.ssm_secret = ssm_secret
        self.region = region
        self.secret_name = secret_name
        self.ssl_cqlsh = ssl_cqlsh
        self.snapshot_folder_override = snapshot_folder_override


    def store_backup_keyspace(self):
        try:
            aws_utils = AWSUtils(self.id, self.key)

            client_s3 = aws_utils.s3_client()

            client_ssm = aws_utils.ssm_client(self.id, self.key, self.region)

            time_stamp = str(int(time.time()))

            if self.snapshot_folder_override is not None:
                main_folder = self.snapshot_folder_override
            else:
                main_folder = time_stamp + "_backup_"  + self.keyspace;
            print(main_folder)
            os.mkdir(main_folder)

            # Get user/password credentials
            if self.ssm_secret:
                print('Getting secrets from SSM')
                parameter = client_ssm.get_parameter(Name=self.secret_name, WithDecryption=True)
                value_array = parameter['Parameter']['Value'].split('\n')
                result_dict={}
                for item in value_array:
                    result_dict[item.partition('=')[0]]=item.partition('=')[2]
                user=result_dict['USER']
                password=result_dict['PASSWORD']
            else:
                user = self.user
                password = self.password

            host = self.host

            # connecting to cassandra
            auth_provider = PlainTextAuthProvider(username=user, password=password)
            cluster = Cluster([host], auth_provider=auth_provider, protocol_version=4)

            session = cluster.connect()

            session.row_factory = dict_factory

            select_tables_stmt = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name=?")

            tables = session.execute(select_tables_stmt, [self.keyspace])
 
            for table in tables:
                print(table["table_name"])
                table_name=table["table_name"]

                file_path = main_folder + '/' + table_name + '.csv'

                statement = SimpleStatement(f"SELECT * from {self.keyspace}.{table_name};", fetch_size=50, consistency_level=ConsistencyLevel.ONE)
                print('Creating csv file for table: ' + table_name)

                data_count_value = 0
                store_table_meta_flag = False

                with open( file_path, 'w', newline='') as csvfile:

                    for row in session.execute(statement,timeout=2.0):
                        if not store_table_meta_flag:
                            with open(main_folder + "/" + table_name + "_table_meta.txt", 'a') as table_meta_file:
                                for key in row.keys():
                                    table_meta_file.write(f'{key}\n')
                            table_meta_file.close()
                            store_table_meta_flag = True
                        writer = csv.DictWriter(csvfile, fieldnames=row.keys(), delimiter='|', quotechar='\"')

                        for k, v in row.items():
                            if isinstance(v, datetime.datetime):
                                new_v = v.replace(microsecond=0)
                                row.update([(k,new_v)])
                            elif isinstance(v, cassandra.util.SortedSet):
                                row.update([(k,set(v))])
                            elif v is None:
                                    row.update([(k,'null')])
                        data_count_value += 1            
                        writer.writerow(row)

                csvfile.close()

                if data_count_value == 0:
                    print(f"Table {table_name} doesn't have data")
                    with open(main_folder + "/" + table_name + "_table_meta.txt", 'a') as table_meta_file:
                        table_meta_file.write(f'NODATA')
                    table_meta_file.close()
                else:
                    print(f"Data count in table {table_name}: {data_count_value}")
                    with open(main_folder + "/" + table_name + "_table_meta.txt", 'a') as table_meta_file:
                        table_meta_file.write(f'{data_count_value}')
                    table_meta_file.close()
                
                

                

                client_s3.upload_file(file_path, self.s3, file_path)
                print('Backup is uploaded to ' + file_path)
                
                client_s3.upload_file(main_folder + "/" + table_name + "_table_meta.txt", self.s3, main_folder + "/" + table_name + "_table_meta.txt")
                print('Table info is uploaded to ' + main_folder + "/" + table_name + "_table_meta.txt")
                
            if self.schema:
                keyspace = session.cluster.metadata.keyspaces[self.keyspace]
                ks_str = keyspace.export_as_string()
                print(ks_str)

                keyspace_schema_file = open(main_folder + "/keyspace_schema.txt", "w")
                keyspace_schema_file.write(ks_str)
                keyspace_schema_file.close()
                client_s3.upload_file(main_folder + "/keyspace_schema.txt", self.s3, main_folder + "/keyspace_schema.txt")

            
        except Exception as e:
            print("Unable to store-backup-keyspace due to", e)
            sys.exit(1)
            
