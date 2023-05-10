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

def read_secret_from_key_vault(key_vault_name, secret_name):
    """ Read secret from Azure key vault

    Args:
        key_vault_name: Name of Azure key vault
        secret_name: Name of secret

    Returns:
        str: value of secret
    """
    Uri = f"https://{key_vault_name}.vault.azure.net"
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

def get_list_extract_quries(host, db_name, user_name, password):
    ''' To get list of rows 
    Args:
        host: Name of DB host/ip address
        db_name: Database name
        user_name: Database user name
        password: Database password
    Returns:
        List: List of rows
    '''
    conn = create_db_connection(host, db_name, user_name, password)               
    rows = exec_sp(conn, config.SP_GET_SCRIPTS
                            .format(    table_name = "base.Record",
                                        filter_column = "createdat",
                                        start_date = "2022-11-30",
                                        end_date = "2023-03-31"   ))
    conn.close()
    return [row for row in rows]

def split_jobs(result_set, max_job_count):
    ''' split result set into multiple junk to run parallely
    Args:
        result_set: List of rows
        max_job_count: Max job count
    Returns:
        List: Return list of list. which contains list of row splitted based on max job count
        Example [['row1', 'row2'], ['row3', 'row4']]
    '''
    return [result_set[i:i + max_job_count] for i in range(0, len(result_set), max_job_count)]

def exec_sp(conn, sp):
    """ Execute stored procedure 
    Args:
        conn: Db connection
        sp: stored procedure
    Returns:
        List: List of rows
    """
    cursor = conn.cursor()
    cursor.execute(sp)
    rows = cursor.fetchall()
    return rows


def generate_parquet_file(script, file_name, conn):
    ''' To write the parquet file by execute the script 
    Args:
        script: script to get records
        file_name: file name for generate parquet file
        conn: db connection
    Returns:
        list: list. which contains parquet file informations.
        Example [bool(if parquet file generate successfully returns true else false),
                int(affected rows),
                str(error message)]
    '''
    try:
        read_records = pd.read_sql_query(script, conn)
        df = pd.DataFrame(read_records)
        parquet_filename = f"{file_name}"
        df.to_parquet(f".\{parquet_filename}", compression='gzip')
        parquet_info = True,len(df),None
        return parquet_info
    except:
        parquet_info = False,0,'failed to execute the sp' 
        return parquet_info

def blob_upload(file_name):
    ''' Upload the parquet file into azure blob
    Args:
        file_name: parquet filename
    '''
    dt = datetime.utcnow()
    blob_name_prefix = config.BLOB_NAME_PREFIX.format(  year = dt.strftime('%Y'),
                                                        month = dt.strftime('%b'),
                                                        date = dt.strftime('%d-%b-%Y'))
    blob_client = blob_service_client.get_blob_client(container=config.AZURE_STORAGE_ACCOUNT_CONTAINER_NAME, 
                                                      blob=f"{blob_name_prefix}/{file_name}{dt.strftime('_%d-%b-%Y %I:%M:%S %p')}")
    file = open(f".\{file_name}", "rb")
    blob_client.upload_blob(file)
    file.close()

def remove_parquet_file(file_name):
    ''' Remove the parquet file from local
    Args:
        file_name: parquet filename
    '''
    os.remove(f".\{file_name}")

def parquet_job(job_info):
    ''' To generate and upload the parquet file for each tables
    Args:
        job_info: job info with row,username,password,host
    Returns:
        bool: true
    '''
    logging.info('start')
    script = job_info['result'][1]
    file_name = job_info['result'][2]
    expected_row_count = job_info['result'][3]

    if expected_row_count == 0:
        logging.info('end')
        return False
    
    logging.info(f'script id: {job_info["result"][0]} and filename: {file_name}')
    conn = create_db_connection(job_info['host'], config.FORMS_DB_NAME, job_info['username'], job_info['password'])

    logging.info(f'generating parquet file for filename:  {file_name} script: {script}')
    generate_parquet = generate_parquet_file(script , file_name, conn)
    is_parquet_generated = generate_parquet[0]
    conn.close()

    if is_parquet_generated:
        logging.info(f'upload parquet file to blob {file_name}')
        blob_upload(file_name)

        logging.info(f'deleting local parquet file {file_name}')
        remove_parquet_file(file_name)

    logging.info('end')
    return True
    
def run_jobs(results, form_db_username, form_db_password):
    ''' To execute the jobs asynchronously
    Args:
        results: splitted row lists based on max job count
        form_db_username: username for Form database 
        form_db_password: password for Form database 
    '''
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for result in results:
            logging.info(f'add DB detail to db list')
            job_info = []
            for row in result:
                local_dic = {   'result': row,
                                'host': config.FORMS_DB_SERVER_HOST, 
                                'username': form_db_username,
                                'password': form_db_password    }
                
                job_info.append(local_dic)
            for row in zip(job_info, executor.map(parquet_job, job_info)):
                if(row[1]):
                    logging.info(f"parquet generation completed for script id : {row[0]['result'][0]}  filename : {row[0]['result'][2]} ") 
                else:
                    logging.info(f"expected row count is zero for the script id : {row[0]['result'][0]}  filename : {row[0]['result'][2]}")


if __name__ == "__main__":

    # Read form secrets
    forms_db_username = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.FORMS_KEY_VAULT_SECRET_NAME_DB_USER_NAME)
    forms_db_password = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.FORMS_KEY_VAULT_SECRET_NAME_DB_PASSWORD)

    # Get results from sp
    result_set = get_list_extract_quries(config.FORMS_DB_SERVER_HOST, 
                                         config.FORMS_DB_NAME, 
                                        forms_db_username,
                                        forms_db_password)    

    #To split the jobs based on max job count
    job_details = split_jobs(result_set, config.MAX_JOB_COUNT)

    logging.info(f'job_details: {job_details}')

    #To execute the jobs asynchronously
    run_jobs(job_details, forms_db_username, forms_db_password)