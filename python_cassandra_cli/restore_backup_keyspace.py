from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
from cassandra.query import dict_factory
import boto3
import os
import sys
from datetime import datetime
import uuid
from python_cassandra_cli.aws_utils import AWSUtils
from cassandra import ReadTimeout
from cassandra.concurrent import execute_concurrent
import time

client_s3 = boto3.client('s3')

import dotenv

dotenv.load_dotenv()

class RestoreBackupKeyspace(object):

    def __init__(self, s3, id, key, keyspace, region, schema, snapshot_folder, host, user, password,
        ssm_secret, secret_name, drop_keyspace, data_insert_chunk_size, csv_field_size_limit, session_concurrency_level):

        self.s3 = s3
        self.id = id
        self.key = key
        self.keyspace = keyspace
        self.region = region
        self.schema = schema
        self.snapshot_folder = snapshot_folder
        self.host = host
        self.user = user
        self.password = password
        self.ssm_secret = ssm_secret
        self.secret_name = secret_name
        self.drop_keyspace = drop_keyspace
        self.data_insert_chunk_size = data_insert_chunk_size
        self.csv_field_size_limit = csv_field_size_limit
        self.session_concurrency_level = session_concurrency_level

    def restore_backup_keyspace(self) :

        try:

            aws_utils = AWSUtils(self.id, self.key)

            client_s3 = aws_utils.s3_client()

            client_ssm = aws_utils.ssm_client(self.id, self.key, self.region)

            list=client_s3.list_objects(Bucket=self.s3, Prefix=self.snapshot_folder)['Contents']
            
            print('Copying files from s3: ' + self.s3)
            for s3_key in list:
                s3_object = s3_key['Key']
                # creating folder for object
                os.makedirs(os.path.dirname(s3_object), exist_ok=True)
                # copy object from s3 to local folder
                client_s3.download_file(self.s3, s3_object, s3_object)
                print('Downloaded: ' + s3_object)

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

            concurrency_level =  self.session_concurrency_level

            # connecting to cassandra
            auth_provider = PlainTextAuthProvider(username=user, password=password)
            cluster = Cluster([host], auth_provider=auth_provider, protocol_version=4)

            session = cluster.connect()


            session.row_factory = dict_factory

            if self.schema:
                print('Creating keyspace schema');
                file_schema = open(self.snapshot_folder + '/keyspace_schema.txt', 'r')
                schema = file_schema.readlines()

                schema_dict = []
                for line in schema:
                    schema_dict.append(line)
                schema_str = ''.join(schema_dict)
                schema_stmts = schema_str.split(';')
                schema_stmts = schema_stmts[ : -1]

                if self.drop_keyspace:
                    try:
                        print(f"DROP keyspace {self.keyspace}")
                        session.execute(f"DROP keyspace {self.keyspace}")

                    except Exception as e: 
                        
                        print(e)

                for query in schema_stmts:
                    # create schema
                    if query.find('CREATE TABLE'):
                        query = query.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
                    if query.find('CREATE INDEX'):
                        query = query.replace('CREATE INDEX', 'CREATE INDEX IF NOT EXISTS')
                    if query.find('CREATE INDEX'):
                        query = query.replace('CREATE KEYSPACE', 'CREATE KEYSPACE IF NOT EXISTS')
                    print(query)
                    session.execute(query) 


                
            select_tables_stmt = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name=?")

            tables = session.execute(select_tables_stmt, [self.keyspace])
 
            for table in tables:
                table_name=table["table_name"]
                print('Restoring table: ' + table_name)
                file_meta = open(self.snapshot_folder + '/' + table_name + '_table_meta.txt', 'r')
                table_meta = file_meta.readlines()
                # print(table_meta)
                table_meta_dict = []
                values_dict = []
                for line in table_meta:
                    table_meta_dict.append(line.strip())
                    values_dict.append("?")
                if table_meta_dict[0] == "NODATA":

                    print(f"Table {table_name} doesn't have data to restore")

                else:
                    data_count_origin = int(table_meta_dict[-1])
                    # remove last object count
                    table_meta_dict = table_meta_dict[ : -1]
                    values_dict = values_dict[ : -1]
                    result_type_stmt = session.prepare(f"SELECT type from system_schema.columns where table_name = \'{table_name}\' and keyspace_name = \'{self.keyspace}\' and column_name=? allow filtering;")
                    result_kind_stmt = session.prepare(f"SELECT kind from system_schema.columns where table_name = \'{table_name}\' and keyspace_name = \'{self.keyspace}\' and column_name=? allow filtering;")
                    data_types = {}
                    data_types_list = []
                    data_kind_list = []

                    for value_column in  table_meta_dict:
                        result_type = session.execute(result_type_stmt, [value_column])
                        result_kind = session.execute(result_kind_stmt, [value_column])
                        data_types[value_column] = result_type[0]['type']
                        data_types_list.append(result_type[0]['type'])
                        data_kind_list.append(result_kind[0]['kind'])

                    table_meta_str = ','.join(table_meta_dict)
                    values_str = ','.join(values_dict)

                    insert_data_stmt = session.prepare(f"INSERT INTO {self.keyspace}.{table_name} ({table_meta_str}) VALUES ({values_str});")

                    chunk_size = self.data_insert_chunk_size
                    chunk_count = 0
                    #increase the limit for csv
                    csv.field_size_limit(self.csv_field_size_limit)
                    with open(self.snapshot_folder + '/' + table_name + '.csv', newline='') as csvfile:
                        start = time.time()
                        data = csv.reader(csvfile, delimiter='|')
                        rows_count = 0
                        statements_and_params = []
                        for row in data:
                            rows_count += 1
                            for i in range(len(row)):
                                if row[i]== 'null':
                                    row[i] = None
                                elif data_types_list[i] == 'timestamp':
                                    try:
                                        row[i] = datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S')
                                    except Exception as e: 
                                        row[i] = None
                                        print(e)
                                elif data_types_list[i] in ('bigint','int','varint'):
                                        row[i] = int(row[i])
                                elif data_types_list[i] == 'uuid':
                                    try:
                                        row[i] = uuid.UUID(str(row[i]))
                                    except ValueError as e:
                                        row[i] = None
                                        print(e)
                                elif data_types_list[i] == 'boolean':
                                    if row[i] == 'True':
                                        row[i] = True
                                    elif row[i] == 'False':
                                        row[i] = False
                                    else:
                                        row[i] = None  
                                elif data_types_list[i] == 'list<text>':
                                    row[i] = row[i].split(",")
                                elif data_types_list[i] == 'set<text>':
                                    if row[i].startswith("{"):
                                        row[i] = row[i][1:]
                                    if row[i].endswith("}"): 
                                        row[i] = row[i][1:]
                                    row[i] =row[i].replace("'","").split(",")
                                    row[i] = set([item.strip() for item in row[i]])
                            
                            params = (row)
                            statements_and_params.append((insert_data_stmt, params))
                            if rows_count % chunk_size == 0:
                                 chunk_count += 1
                                 print(f'Inserting {chunk_count} chunk of data')
                                 execute_concurrent(session, statements_and_params,concurrency=concurrency_level , raise_on_first_error=False)
                                 statements_and_params = []
                        if len(statements_and_params) > 0:
                            print("Inserting rest of data")
                            execute_concurrent(session, statements_and_params,concurrency=concurrency_level , raise_on_first_error=False)
                        end = time.time()
                        print(f"Data into table {table_name} uploaded: ",(end-start),"sec")

                    data_count_current = rows_count                  

                    print(f"Data count in table {table_name} origin equal {data_count_origin}")
                    print(f"Data count in table {table_name} current equal {data_count_current}")
        except Exception as e: 
            print(e)
            sys.exit(1)
