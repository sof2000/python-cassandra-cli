# !/usr/bin/env python3
"""
Cassandra cli tool
"""
import click
import python_cassandra_cli.constants as constant
from python_cassandra_cli.restore_snapshot_table import RestoreSnapshotTable
from python_cassandra_cli.restore_snapshot_keyspace import RestoreSnapshotKeyspace
from python_cassandra_cli.copy_snapshot_folder import CopySnapshotFolder
from python_cassandra_cli.store_snapshot_keyspace import StoreSnapshotKeyspace
from python_cassandra_cli.store_snapshot_table import StoreSnapshotTable
from python_cassandra_cli.store_backup_keyspace import StoreBackupKeyspace
from python_cassandra_cli.restore_backup_keyspace import RestoreBackupKeyspace



@click.group()
@click.version_option()
def cli():
    pass

@cli.command(help='Take and store Keyspace snapshot to s3 bucket')
@click.option('-k' ,'--keyspace', 'keyspace', required = True, help = constant.KEYSPACE_OPTION)
@click.option('-t' ,'--snapshot-tag', 'tag', required = True, help = constant.SNAPSHOT_TAG_OPTION)
@click.option('-s3' ,'--s3-bucket', 's3', required = True, help = constant.S3_BUCKET_OPTION)
@click.option('-id' ,'--aws-access-key-id', 'id', help = constant.AWS_ACCESS_KEY_ID_OPTION)
@click.option('-key' ,'--aws-secret-access-key', 'key', help = constant.AWS_SECRET_ACCESS_KEY_OPTION)
@click.option('-r' ,'--aws-region', 'region', default = 'us-east-1',help = constant.AWS_REGION_OPTION)
@click.option('-e' ,'--environment', 'environment', required = True, help = constant.ENVIRONMENT_OPTION)
@click.option('-cks' ,'--create-keyspace-schema', 'schema', is_flag = True , help = constant.CREATE_KEYSPACE_SCHEMA_OPTION)
@click.option('-h' ,'--cassandra-host', 'host' , help = constant.CASSANDRA_HOST_OPTION)
@click.option('-u' ,'--cassandra-user', 'user' , help = constant.CASSANDRA_USER_OPTION)
@click.option('-p' ,'--cassandra-password', 'password', help = constant.CASSANDRA_PASSWORD_OPTION)
@click.option('--no-clear-snapshot', is_flag = True, help = constant.NO_CLEAR_SNAOSHOT_OPTION)
@click.option('--ssm-secret', is_flag = True, help = constant.SSM_SECRET_OPTION)
@click.option('-sn' ,'--secret-name', help = constant.SECRET_NAME_OPTION)
@click.option('-sc' ,'--ssl-cqlsh', is_flag = True, help = constant.SSL_CONNECTION_CQLSH_OPTION)
@click.option('-sfo' ,'--snapshot-folder-override', help = constant.SNAPSHOT_FOLDER_S3_OVERRIDE_OPTION)

def store_snapshot_keyspace(keyspace, s3, id, key, tag, environment, schema, host, user, password, no_clear_snapshot, ssm_secret, region,secret_name, ssl_cqlsh, snapshot_folder_override):

    print(constant.INITIAL_MESSAGE)
    
    store_snapshot = StoreSnapshotKeyspace(keyspace, s3, id, key, tag, environment, schema, host, user, password, no_clear_snapshot, ssm_secret, region,secret_name, ssl_cqlsh, snapshot_folder_override)

    store_snapshot.store_snapshot_keyspace()

@cli.command(help='Take and store Table snapshot to s3 bucket')
@click.option('-k' ,'--keyspace', 'keyspace', required = True, help = constant.KEYSPACE_OPTION)
@click.option('-tn' ,'--table-name', required = True,  help = constant.TABLE_NAME_OPTION)
@click.option('-t' ,'--snapshot-tag', 'tag', required = True, help = constant.SNAPSHOT_TAG_OPTION)
@click.option('-s3' ,'--s3-bucket', 's3', required = True, help = constant.S3_BUCKET_OPTION)
@click.option('-id' ,'--aws-access-key-id', 'id', help = constant.AWS_ACCESS_KEY_ID_OPTION)
@click.option('-key' ,'--aws-secret-access-key', 'key', help=constant.AWS_SECRET_ACCESS_KEY_OPTION)
@click.option('-r' ,'--aws-region', 'region', default = 'us-east-1', help= constant.AWS_REGION_OPTION )
@click.option('-e' ,'--environment', 'environment', required = True,  help = constant.ENVIRONMENT_OPTION)
@click.option('-cts' ,'--create-table-schema', 'schema', is_flag = True , help = constant.CREATE_TABLE_SCHEMA_OPTION)
@click.option('-h' ,'--cassandra-host', 'host', help = constant.CASSANDRA_HOST_OPTION)
@click.option('-u' ,'--cassandra-user', 'user' , help = constant.CASSANDRA_USER_OPTION)
@click.option('-p' ,'--cassandra-password', 'password', help = constant.CASSANDRA_PASSWORD_OPTION)
@click.option('--no-clear-snapshot', is_flag = True, help = constant.NO_CLEAR_SNAOSHOT_OPTION)
@click.option('--ssm-secret', is_flag = True, help = constant.SSM_SECRET_OPTION)
@click.option('-sn' ,'--secret-name', help = constant.SECRET_NAME_OPTION)
@click.option('-sc' ,'--ssl-cqlsh', is_flag = True, help = constant.SSL_CONNECTION_CQLSH_OPTION)
@click.option('-sfo' ,'--snapshot-folder-override', help = constant.SNAPSHOT_FOLDER_S3_OVERRIDE_OPTION)

def store_snapshot_table(keyspace, table_name, s3, id, key, tag, environment, schema, host, user, password, no_clear_snapshot, ssm_secret, region, secret_name, ssl_cqlsh, snapshot_folder_override):

    print(constant.INITIAL_MESSAGE)
    
    store_snapshot = StoreSnapshotTable(keyspace, table_name, s3, id, key, tag, environment, schema, host, user, password, no_clear_snapshot, ssm_secret, region,secret_name, ssl_cqlsh, snapshot_folder_override)

    store_snapshot.store_snapshot_table()


@cli.command(help='Restore Table snapshot from s3 bucket')
@click.option('-s3' ,'--s3-bucket', 's3', required = True, help = constant.S3_BUCKET_OPTION)
@click.option('-id' ,'--aws-access-key-id', 'id', help = constant.AWS_ACCESS_KEY_ID_OPTION)
@click.option('-key' ,'--aws-secret-access-key', 'key', help = constant.AWS_SECRET_ACCESS_KEY_OPTION)
@click.option('-tn' ,'--table-name', required = True,  help = constant.TABLE_NAME_OPTION)
@click.option('-t' ,'--snapshot-tag', 'tag', required = True, help = constant.SNAPSHOT_TAG_OPTION)
@click.option('-sf' ,'--snapshot-folder', required = True, help = constant.SNAPSHOT_FOLDER_OPTION)
@click.option('-k' ,'--keyspace', 'keyspace', required = True, help = constant.KEYSPACE_OPTION)
@click.option('-ssl' ,'--ssl-connection', is_flag = True, help = constant.SSL_CONNECTION_OPTION)
@click.option('-h' ,'--cassandra-host', 'host', required = True, help = constant.CASSANDRA_HOST_OPTION)
@click.option('-u' ,'--cassandra-user', 'user', help = constant.CASSANDRA_USER_OPTION)
@click.option('-p' ,'--cassandra-password', 'password', help = constant.CASSANDRA_PASSWORD_OPTION)
@click.option('-pt' ,'--cassandra-port' , help = constant.CASSANDRA_PORT_OPTION)
@click.option('-cf' ,'--cassandra-config-file' , help= constant.CASSANDRA_CONFIG_FILE_OPTION)
@click.option('-ks' ,'--cassandra-keystore' , help = constant.CASSANDRA_KEYSTORE_OPTION)
@click.option('-ts' ,'--cassandra-truststore' , help = constant.CASSANDRA_TRUSTSTORE_OPTION)
@click.option('--ssm-secret', is_flag = True, help = constant.SSM_SECRET_OPTION)
@click.option('-sn' ,'--secret-name', help = constant.SECRET_NAME_OPTION)
@click.option('-r' ,'--aws-region', 'region', default = 'us-east-1', help = constant.AWS_REGION_OPTION)
@click.option('-tspw' ,'--truststore-password', 'truststore_password', help = constant.TRUSTSTORE_PASSWORD_OPTION)
@click.option('-kspw' ,'--keystore-password', 'keystore_password', help = constant.KEYSTORE_PASSWORD_OPTION)

def restore_snapshot_table(s3, id, key, tag, keyspace, region, table_name, snapshot_folder, host, user, password, 
    cassandra_config_file, cassandra_keystore, cassandra_truststore, cassandra_port, ssl_connection, ssm_secret, secret_name,
    truststore_password, keystore_password):

    print(constant.INITIAL_MESSAGE)
    
    restore_snapshot = RestoreSnapshotTable(s3, id, key, tag, keyspace, region, table_name, snapshot_folder, host, user, password,
        cassandra_config_file, cassandra_keystore, cassandra_truststore, cassandra_port, ssl_connection, ssm_secret, secret_name,
        truststore_password, keystore_password)

    restore_snapshot.restore_snapshot_table()

@cli.command(help='Restore Keyspace snapshot from s3 bucket')
@click.option('-s3' ,'--s3-bucket', 's3', required = True, help = constant.S3_BUCKET_OPTION)
@click.option('-id' ,'--aws-access-key-id', 'id', help = constant.AWS_ACCESS_KEY_ID_OPTION)
@click.option('-key' ,'--aws-secret-access-key', 'key', help = constant.AWS_SECRET_ACCESS_KEY_OPTION)
@click.option('-cks' ,'--create-keyspace-schema', 'schema', is_flag = True , help = constant.CREATE_KEYSPACE_SCHEMA_OPTION)
@click.option('-t' ,'--snapshot-tag', 'tag', required = True, help = constant.SNAPSHOT_TAG_OPTION)
@click.option('-sf' ,'--snapshot-folder', required = True, help = constant.SNAPSHOT_FOLDER_OPTION)
@click.option('-k' ,'--keyspace', 'keyspace', required = True, help = constant.KEYSPACE_OPTION)
@click.option('-e' ,'--environment', 'environment', required = True, help = constant.ENVIRONMENT_OPTION)
@click.option('-ssl' ,'--ssl-connection', is_flag = True, help = constant.SSL_CONNECTION_OPTION)
@click.option('-h' ,'--cassandra-host', 'host', required = True, help = constant.CASSANDRA_HOST_OPTION)
@click.option('-u' ,'--cassandra-user', 'user', help = constant.CASSANDRA_USER_OPTION)
@click.option('-p' ,'--cassandra-password', 'password', help = constant.CASSANDRA_PASSWORD_OPTION)
@click.option('-pt' ,'--cassandra-port' , help= constant.CASSANDRA_PORT_OPTION)
@click.option('-cf' ,'--cassandra-config-file' ,help = constant.CASSANDRA_CONFIG_FILE_OPTION)
@click.option('-ks' ,'--cassandra-keystore' , help = constant.CASSANDRA_KEYSTORE_OPTION)
@click.option('-ts' ,'--cassandra-truststore' , help = constant.CASSANDRA_TRUSTSTORE_OPTION)
@click.option('--ssm-secret', is_flag = True, help = constant.SSM_SECRET_OPTION)
@click.option('-sn' ,'--secret-name', help = constant.SECRET_NAME_OPTION)
@click.option('-r' ,'--aws-region', 'region', default = 'us-east-1', help = constant.AWS_REGION_OPTION)
@click.option('-sc' ,'--ssl-cqlsh', is_flag = True, help = constant.SSL_CONNECTION_CQLSH_OPTION)
@click.option('-tspw' ,'--truststore-password', 'truststore_password', help = constant.TRUSTSTORE_PASSWORD_OPTION)
@click.option('-kspw' ,'--keystore-password', 'keystore_password', help = constant.KEYSTORE_PASSWORD_OPTION)


def restore_snapshot_keyspace(s3, id, key, tag, keyspace, region, schema, snapshot_folder, host, user, password, 
    cassandra_config_file, cassandra_keystore, cassandra_truststore, cassandra_port, ssl_connection, ssm_secret, secret_name, environment, ssl_cqlsh, truststore_password, keystore_password):

    print(constant.INITIAL_MESSAGE)
    
    restore_snapshot = RestoreSnapshotKeyspace(s3, id, key, tag, keyspace, region, schema, snapshot_folder, host, user, password,
        cassandra_config_file, cassandra_keystore, cassandra_truststore, cassandra_port, ssl_connection, ssm_secret, secret_name, environment, ssl_cqlsh, truststore_password, keystore_password)

    restore_snapshot.restore_snapshot_keyspace()

@cli.command(help='Copy folder with snapshots from s3 bucket')
@click.option('-s3' ,'--s3-bucket', 's3', required = True, help = constant.S3_BUCKET_OPTION)
@click.option('-id' ,'--aws-access-key-id', 'id', help = constant.AWS_ACCESS_KEY_ID_OPTION)
@click.option('-key' ,'--aws-secret-access-key', 'key', help = constant.AWS_SECRET_ACCESS_KEY_OPTION)
@click.option('-sf' ,'--snapshot-folder', required = True, help = constant.SNAPSHOT_FOLDER_OPTION)

def copy_snapshot_folder(s3, id, key, snapshot_folder):

    print(constant.INITIAL_MESSAGE)
    
    copy_snapshot = CopySnapshotFolder(s3, id, key, snapshot_folder)

    copy_snapshot.copy_snapshot_folder(); 


@cli.command(help='Take and store Keyspace backup to s3 bucket without running nodetool, sstableloader, and cqlsh')
@click.option('-k' ,'--keyspace', 'keyspace', required = True, help = constant.KEYSPACE_OPTION)
@click.option('-s3' ,'--s3-bucket', 's3', required = True, help = constant.S3_BUCKET_OPTION)
@click.option('-id' ,'--aws-access-key-id', 'id', help = constant.AWS_ACCESS_KEY_ID_OPTION)
@click.option('-key' ,'--aws-secret-access-key', 'key', help = constant.AWS_SECRET_ACCESS_KEY_OPTION)
@click.option('-r' ,'--aws-region', 'region', default = 'us-east-1',help = constant.AWS_REGION_OPTION)
@click.option('-cks' ,'--create-keyspace-schema', 'schema', is_flag = True , help = constant.CREATE_KEYSPACE_SCHEMA_OPTION)
@click.option('-h' ,'--cassandra-host', 'host' , help = constant.CASSANDRA_HOST_OPTION)
@click.option('-u' ,'--cassandra-user', 'user' , help = constant.CASSANDRA_USER_OPTION)
@click.option('-p' ,'--cassandra-password', 'password', help = constant.CASSANDRA_PASSWORD_OPTION)
@click.option('--no-clear-snapshot', is_flag = True, help = constant.NO_CLEAR_SNAOSHOT_OPTION)
@click.option('--ssm-secret', is_flag = True, help = constant.SSM_SECRET_OPTION)
@click.option('-sn' ,'--secret-name', help = constant.SECRET_NAME_OPTION)
@click.option('-sc' ,'--ssl-cqlsh', is_flag = True, help = constant.SSL_CONNECTION_CQLSH_OPTION)
@click.option('-sfo' ,'--snapshot-folder-override', help = constant.SNAPSHOT_FOLDER_S3_OVERRIDE_OPTION)

def store_backup_keyspace(keyspace, s3, id, key, schema, host, user, password, no_clear_snapshot, ssm_secret, region,secret_name, ssl_cqlsh, 
    snapshot_folder_override):

    print(constant.INITIAL_MESSAGE)
    
    store_backup = StoreBackupKeyspace(keyspace, s3, id, key, schema, host, user, password, no_clear_snapshot, ssm_secret, region,secret_name,
        ssl_cqlsh, snapshot_folder_override)

    store_backup.store_backup_keyspace()


@cli.command(help='Restore Keyspace backup from s3 bucket without running nodetool, sstableloader, and cqlsh')
@click.option('-s3' ,'--s3-bucket', 's3', required = True, help = constant.S3_BUCKET_OPTION)
@click.option('-id' ,'--aws-access-key-id', 'id', help = constant.AWS_ACCESS_KEY_ID_OPTION)
@click.option('-key' ,'--aws-secret-access-key', 'key', help = constant.AWS_SECRET_ACCESS_KEY_OPTION)
@click.option('-cks' ,'--create-keyspace-schema', 'schema', is_flag = True , help = constant.CREATE_KEYSPACE_SCHEMA_OPTION)
@click.option('-sf' ,'--snapshot-folder', required = True, help = constant.SNAPSHOT_FOLDER_OPTION)
@click.option('-k' ,'--keyspace', 'keyspace', required = True, help = constant.KEYSPACE_OPTION)
@click.option('-h' ,'--cassandra-host', 'host', required = False, help = constant.CASSANDRA_HOST_OPTION)
@click.option('-u' ,'--cassandra-user', 'user', help = constant.CASSANDRA_USER_OPTION)
@click.option('-p' ,'--cassandra-password', 'password', help = constant.CASSANDRA_PASSWORD_OPTION)
@click.option('--ssm-secret', is_flag = True, help = constant.SSM_SECRET_OPTION)
@click.option('-sn' ,'--secret-name', help = constant.SECRET_NAME_OPTION)
@click.option('-r' ,'--aws-region', 'region', default = 'us-east-1', help = constant.AWS_REGION_OPTION)
@click.option('--drop-keyspace', is_flag = True, help = constant.DROP_KEYSPACE_OPTION)
@click.option('--data-insert-chunk-size', default=40000, show_default=True, type=int, help = constant.DATA_INSERT_CHUNK_SIZE)
@click.option('--csv-field-size-limit', default=100000000, show_default=True, type=int, help = constant.CSV_FIELD_SIZE_LIMIT)
@click.option('--session-concurrency-level', default=100, show_default=True, type=int, help = constant.SESSION_CONCURRENCY_LEVEL)



def restore_backup_keyspace(s3, id, key, keyspace, region, schema, snapshot_folder, host, user, password, 
    ssm_secret, secret_name, drop_keyspace, data_insert_chunk_size, csv_field_size_limit, session_concurrency_level):

    print(constant.INITIAL_MESSAGE)
    
    restore_backup = RestoreBackupKeyspace(s3, id, key, keyspace, region, schema, snapshot_folder, host, user, password,
        ssm_secret, secret_name, drop_keyspace, data_insert_chunk_size, csv_field_size_limit, session_concurrency_level)

    restore_backup.restore_backup_keyspace()

if __name__ == '__main__':
    cli()





