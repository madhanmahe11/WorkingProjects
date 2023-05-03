from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from datetime import datetime
import concurrent.futures
import pandas as pd
import logging
import config
import pyodbc
import os

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

#To connect the blob service client by using azure storage account connection string
blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING)

def read_secret_from_key_vault(key_valut_name, secret_name):
    """ Read secret from Azure key vault

    Args:
        key_valut_name: Name of Azure key vault
        secret_name: Name of secret

    Returns:
        str: value of secret
    """
    Uri = f"https://{key_valut_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=Uri, credential=credential)
    return client.get_secret(secret_name).value
    
def create_db_connection(host, db_name, user_name, password):
    ''' Create DB connection from credentials
    Args:
        host: Name of DB host/ip address
        db_name: Database name
        user_name: Database user name
        password: Database password
    Returns:
        Connection: Db connection
    '''
    con_str = f'DRIVER=SQL Server;SERVER={host};DATABASE={db_name};ENCRYPT=no;UID={user_name};PWD={password}'
    return pyodbc.connect(con_str)

def get_active_gp_databases(host, db_name, user_name,password):
    ''' To get list of GP database from EDW database
    Args:
        host: Name of DB host/ip address
        db_name: Database name
        user_name: Database user name
        password: Database password
    Returns:
        List: List of GP databases
    '''
    conn = create_db_connection(host, db_name, user_name, password)
    rows = exec_query(conn, config.QUERY_GET_GP_DB)
    conn.close()
    return [row[0] for row in rows]

def split_jobs(db_list, max_job_count):
    ''' split db list into multiple junk to run parallely
    Args:
        db_list: List of DB
        max_job_count: Max job count
    Returns:
        List: Return list of list. which contains list of DB splitted based on max job count
        Example [['db1', 'db2'], ['db3', 'db4']]
    '''
    return [db_list[i:i + max_job_count] for i in range(0, len(db_list), max_job_count)]

def exec_query(conn, query):
    """ Get records from using SQL query 
    Args:
        conn: Db connection
        query: SQL query
    Returns:
        List: List of rows
    """
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

def get_all_tables(conn, db_name, query):
    ''' To get list of tables from database
    Args:
        conn: db connection
        db_name: Database name
        query: SQL query
    Returns:
        List: List of tables
    '''
    rows = exec_query(conn, query.format(db_name=db_name))
    return [table[0] for table in rows]

def generate_parquet_file(table_name,conn):
    ''' To write the parquet file from table records
    Args:
        table_name: table name
        conn: db connection
    Returns:
        bool: if parquet file generate successfully returns true else false 
        str: filename for the generated parquet file
    '''
    try:
        read_records = pd.read_sql_query(config.QUERY_GET_ALL_RECORDS_FROM_TABLE.format(table_name=table_name), conn)
        df = pd.DataFrame(read_records)
        parquet_filename = f"{table_name}{datetime.utcnow().strftime('_%d-%b-%Y %I_%M_%S %p')}"
        df.to_parquet(f".\{parquet_filename}", compression='gzip')
        return True,parquet_filename
    except:
        return False,None

def blob_upload(parquet_filename,db_name,table_name):
    ''' Upload the parquet file into azure blob
    Args:
        parquet_filename: parquet filename
        db_name: database name
        table_name: table name
    '''
    dt = datetime.utcnow()
    blob_client = blob_service_client.get_blob_client(container=config.AZURE_STORAGE_ACCOUNT_CONTAINER_NAME, blob=f"GP/{dt.strftime('%Y')}/{dt.strftime('%b')}/{dt.strftime('%d-%b-%Y')}/{db_name}/{table_name}{dt.strftime('_%d-%b-%Y %I:%M:%S %p')}")
    file = open(f".\{parquet_filename}", "rb")
    blob_client.upload_blob(file)
    file.close()

def remove_parquet_file(parquet_filename):
    ''' Remove the parquet file from local
    Args:
        parquet_filename: parquet filename
    '''
    os.remove(f".\{parquet_filename}")

def parquet_job(db_job_info):
    ''' To generate and upload the parquet file for each tables
    Args:
        db_job_info: db info with db_name,username,password,host
    Returns:
        bool: true
    '''
    logging.info('start')
    db_name = db_job_info['db_name']
    conn = create_db_connection(db_job_info['host'], db_name, db_job_info['username'], db_job_info['password'])
    tables = get_all_tables(conn, db_name, config.QUERY_GET_ALL_TABLES_FOR_DB)
    logging.info(f'db: {db_name} and tables: {tables}')
    for table in tables:
        logging.info(f'generating parquet file for db:  {db_name} table: {table}')
        generate_parquet = generate_parquet_file(table,conn)

        is_parquet_generated = generate_parquet[0]

        if is_parquet_generated:
            parquet_filename = generate_parquet[1]
            logging.info(f'upload parquet file to blob {db_name} table: {table} file name: {parquet_filename}.parquet')
            blob_upload(parquet_filename,db_name,table)
            logging.info(f'deleting local parquet file {db_name} table: {table} file name: {parquet_filename}.parquet')
            remove_parquet_file(parquet_filename)

    conn.close()
    logging.info('end')
    return True

    
def run_jobs(db_lists, gp_db_username, gp_db_password):
    ''' To execute the jobs asynchronously
    Args:
        db_lists: splitted DB lists based on max job count
        gp_db_username: username for GP database 
        gp_db_password: password for GP database 
    '''
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for db_list in db_lists:
            logging.info(f'db_list: {db_list}')
            logging.info(f'add DB detail to db list')
            db_job_info = []
            for db in db_list:
                local_dic = {   'db_name': db, 
                                'host': config.GP_DB_SERVER_HOST, 
                                'username': gp_db_username,
                                'password': gp_db_password}
                
                db_job_info.append(local_dic)
            for db in zip(db_job_info, executor.map(parquet_job, db_job_info)):
                logging.info(f"parquet generation completed for {db[0]['db_name']}")    

if __name__ == "__main__":

    # Read EDW secrets
    edw_db_username = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.EDW_KEY_VAULT_SECRET_NAME_DB_USER_NAME)
    edw_db_password = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.EDW_KEY_VAULT_SECRET_NAME_DB_PASSWORD)

    # Get Active GP Databases
    gp_db_list = get_active_gp_databases(   config.EDW_DB_SERVER_HOST, 
                                            config.EDW_DB_NAME, edw_db_username,edw_db_password)    
    
    logging.info(f'gp_db_list: {gp_db_list}')
    
    # Read GP secrets
    GP_USER_DB_NAME = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.GP_KEY_VAULT_SECRET_NAME_DB_USER_NAME)
    GP_USER_DB_PASSWORD = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.GP_KEY_VAULT_SECRET_NAME_DB_PASSWORD)

    #To split the jobs based on max job count
    job_details = split_jobs(gp_db_list, config.MAX_JOB_COUNT)

    logging.info(f'job_details: {job_details}')

    #To execute the jobs asynchronously
    run_jobs(job_details, GP_USER_DB_NAME, GP_USER_DB_PASSWORD)    