# Python Cassandra CLI tool

## Descirptoin
  CLI has **7** main commands:
  1. copy-snapshot-folder       Copy folder with snapshots from s3 bucket
  2. restore-snapshot-keyspace  Restore Keyspace snapshot from s3 bucket
  3. restore-snapshot-table     Restore Table snapshot from s3 bucket
  4. store-snapshot-keyspace    Take and store Keyspace snapshot to s3 bucket
  5. store-snapshot-table       Take and store Table snapshot to s3 bucket
  6. store-backup-keyspace      Take and store Keyspace backup to s3 bucket without running nodetool, sstableloader, and cqlsh
  7. restore-backup-keyspace    Restore Keyspace backup from s3 bucket without running nodetool, sstableloader, and cqlsh
## Installation 

1. Navigate to https://pypi.org/project/python-cassandra-cli/ for details.
2. Run 
```
pip install python-cassandra-cli
```

## Release Notes

### 1.4.0
  1. Updated store-backup-keyspace and restore-backup-keyspace.
### 1.3.6
  1. Updated readme.
### 1.3.5
  1. cleanup.

### 1.3.4
  1. copy backup updates.

### 1.3.3
  1. boolean values updates.


### 1.3.2
  1. null values updates.

### 1.3.1
  1. Increased CSV file field size limit.
  2. Creation schema fix.  

### 1.3.0
  1. **store-backup-keyspace** and **restore-backup-keyspace** commands which doesn't run nodetool, sstableloader, and cqlsh. It uses only cassandra-driver.

### 1.2.1
  1. --truststore-password and --keystore-password options \
  which is used in **sstableloader** command in *"restore-snapshot-keyspace"* and *"restore-snapshot-table"*
  2. --version option

### 1.1.1
  Clean up code
### 1.1.0
  1. Added --ssl flag for cqlsh command if connection supports only SSL connection; --ssl-cqlsh option
  2. Logic -p flag for sstableloader not mandatory for SSL connection type
  3. Overriding folder main folder name; --snapshot-folder-override option
### 1.0.3
  Fix option description: create-keyspace-schema

### 1.0.2
  Updated Authentication logic

### 1.0.1
  Added boto3 libs in requirements

### 1.0.0
  Initial release

## AWS resources access

## AWS S3 bucket
  1. You need to set -s3(--s3-bucket), -id(--aws-access-key-id) and  -key(--aws-secret-access-key) options for the connection to AWS S3
    bucket via access key. 
  2. If your host has AWS profile setup in .aws/credentials and/or IAM role with s3 bucket permissons then CLI
    needs only -s3(--s3-bucket) option.
## AWS Systems Manager
  1. You need to set -sn(--secret-name), -id(--aws-access-key-id) and  -key(--aws-secret-access-key) for getting secrest from Paramenter Store.
  2. If your host has AWS profile setup in .aws/credentials and/or IAM role with SSM permissons then CLI
    needs only -sn(--secret-name) option.
  3. Optionally you can change -r(--aws-region) for SSM. Default value is 'us-east-1'


## Commands examples:

### Store keyspace snapshot on s3 
```
python-cassandra-cli store-snapshot-keyspace  
  -k my_keyspace 
  -t tag-01-keyspace
  -e dev 
  -s3 my.s3.bucket 
  -h "10.99.3.55"   
  --ssm-secret -sn /cassandra/dev 
```

### Store keyspace snapshot on s3 with keyspace schema
```
python-cassandra-cli store-snapshot-keyspace  
  -k my_keyspace 
  -t tag-01-keyspace
  -e dev -s3 my.s3.bucket -h "10.99.3.55"  
  --ssm-secret -sn /cassandra/dev 
  --create-keyspace-schema
``` 

### Store table snapshot on s3 with keyspace schema
```
python-cassandra-cli store-snapshot-table 
  -k my_keyspace 
  -tn my_table
  -t tag-01-table 
  -e dev 
  -s3 my.s3.bucket 
  -h "10.99.3.55"  
  --ssm-secret
  -sn /cassandra/dev 
  --create-keyspace-schema
```

### Resotore table

```
python-cassandra-cli restore-snapshot-table 
  -sf 1608700346_snapshots_tag-01-keyspace_my_keyspace_develop 
  -t tag-01-table 
  -k my_keyspace 
  -tn my_table -h "10.99.3.55" 
  --ssm-secret 
  -sn /cassandra/dev 
  -s3 my.s3.bucket
```
### Restore table ssl
```
python-cassandra-cli restore-snapshot-table  
  -sf 1608700346_snapshots_tag-01-table_my_table_develop 
  -t tag-01-table 
  -k my_keyspace 
  -tn my_table 
  -h "10.99.3.55" 
  --ssm-secret 
  -s3 my.s3.bucket 
  -sn /cassandra/dev 
  -cf /etc/cassandra/conf/cassandra.yaml  
  -ks /etc/cassandra/conf/keystore.node0 
  -ts /etc/cassandra/conf/truststore.node0 
  -ssl 
  -pt 9142 
```
### Restore keyspace ssl with keyspace creation
```
python-cassandra-cli restore-snapshot-keyspace  
  -sf 608700346_snapshots_tag-01-keyspace_my_keyspace_develop 
  -t tag-01-keyspace 
  -k my_keyspace  
  -h "10.99.3.55" 
  --ssm-secret 
  -s3 my.s3.bucket 
  -sn /cassandra/dev 
  --create-keyspace-schema 
  -e develop 
  -cf /etc/cassandra/conf/cassandra.yaml  
  -ks /etc/cassandra/conf/keystore.node0 
  -ts /etc/cassandra/conf/truststore.node0 
  -ssl 
  -pt 9142 
```
### Copy folder with sanpshot on local
```
python-cassandra-cli copy-snapshot-folder 
  -s3 my.s3.bucket 
  -sf 1608700346_snapshots_tag-01-keyspace_my_keyspace_develop
```
