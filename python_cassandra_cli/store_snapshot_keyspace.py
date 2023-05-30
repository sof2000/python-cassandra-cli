import os
import sys
import re
import shutil
import time
from python_cassandra_cli.aws_utils import AWSUtils

class StoreSnapshotKeyspace(object):
    
    def __init__(self, keyspace, s3, id, key, tag, environment, schema, host, user, password, no_clear_snapshot, ssm_secret, region,secret_name, ssl_cqlsh, snapshot_folder_override):

        self.keyspace = keyspace
        self.s3 = s3
        self.id = id
        self.key = key
        self.tag = tag
        self.environment = environment
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

    def store_snapshot_keyspace(self):
        try:
            aws_utils = AWSUtils(self.id, self.key)

            client_s3 = aws_utils.s3_client()

            client_ssm = aws_utils.ssm_client(self.id, self.key, self.region)

            result_filtered_array = [];

            time_stamp = str(int(time.time()))

            if self.snapshot_folder_override is not None:
                main_folder = self.snapshot_folder_override
            else:
                main_folder = time_stamp + "_snapshots_" + self.tag + "_" + self.keyspace + "_" + self.environment;

            # nodetool take snapshot
            cmd = "nodetool snapshot --tag {0}  -- {1}".format(self.tag, self.keyspace)
            os.system(cmd);

            user_dir=os.getcwd()
            # change to root of system
            os.chdir('/')

            # search snapshots
            for root,dirs,files in os.walk('.'):
                if (re.search('/snapshots/'+self.tag+"$", root)) and ("_snapshots_" + self.tag + "_" + self.keyspace not in root ):
                        result_filtered_array.append(root);
            # change dir to user directory
            os.chdir(user_dir);

            for line in result_filtered_array:
                i = result_filtered_array.index(line);
                result_record_init = result_filtered_array[i];
                result_record = re.sub('^.','', result_record_init);
                folder_path=result_record.partition("/" +  self.keyspace)[2];
                # print(folder_path);
                folder_copy=re.sub("/" + self.tag, '',folder_path);
                # print(folder_copy);
                snapshotdir = main_folder + folder_copy + "/" + self.tag;
                shutil.copytree(result_record, snapshotdir);

            if self.schema:
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
                    user=self.user
                    password=self.password
                if user:
                    if self.ssl_cqlsh:
                        cmd_cql = "cqlsh {0} -u {1} -p {2} -e \"DESC KEYSPACE {3};\" --ssl > {4}/{5}_{6}_schema.cql".format(
                            self.host, user, password, self.keyspace, main_folder, self.keyspace, self.environment)
                    else:
                        cmd_cql = "cqlsh {0} -u {1} -p {2} -e \"DESC KEYSPACE {3};\" > {4}/{5}_{6}_schema.cql".format(
                            self.host, user, password, self.keyspace, main_folder, self.keyspace, self.environment)
                else:
                    if self.ssl_cqlsh:
                        cmd_cql = "cqlsh {0} -e \"DESC KEYSPACE {1};\" --ssl > {2}/{3}_{4}_schema.cql".format(
                            self.host, self.keyspace, main_folder, self.keyspace, self.environment)
                    else:
                        cmd_cql = "cqlsh {0} -e \"DESC KEYSPACE {1};\" > {2}/{3}_{4}_schema.cql".format(
                            self.host, self.keyspace, main_folder, self.keyspace, self.environment)
                # print(cmd_cql);
                os.system(cmd_cql);

            for root,dirs,files in os.walk(main_folder):
                for file in files:
                    client_s3.upload_file(os.path.join(root,file), self.s3, os.path.join(root,file))
                    # print(os.path.join(root,file));

            shutil.rmtree(main_folder)

            if self.no_clear_snapshot:
                print("You need to clear snapshot with tag '% s' using nodetool" % self.tag)
            else:
                print("Clearing snapshot '% s' from system" % self.tag);
                cmd_nodetool = "nodetool clearsnapshot -t {0}".format(self.tag)
                # print(cmd_nodetool);
                os.system(cmd_nodetool);
            print('Folder name: ', main_folder)
        except Exception as e:
            print("Unable to store-snapshot-keyspace due to", e)
            sys.exit(1)