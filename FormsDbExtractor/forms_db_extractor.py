from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from datetime import datetime
import concurrent.futures
import pandas as pd
import argparse
import logging
import config
import pyodbc
import csv
import os

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

#To connect the blob service client by using azure storage account connection string
try:
    blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING)
except:
    raise Exception(f'Invalid azure blob connection string : {config.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING}')


def read_sp_args():
    """ To read input values for sp arguments
    Returns:
        list: list of input values
        Example: [tablename,filtercolumn,startdate,enddate]
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-TableName')
    parser.add_argument('-FilterColumn')
    parser.add_argument("-StartDate")
    parser.add_argument("-EndDate")
    args = parser.parse_args()
    return args.TableName, args.FilterColumn, args.StartDate, args.EndDate

def read_secret_from_key_vault(key_vault_name, secret_name):
    """ Read secret from Azure key vault

    Args:
        key_vault_name: Name of Azure key vault
        secret_name: Name of secret

    Returns:
        str: value of secret
    """
    try:
        Uri = f"https://{key_vault_name}.vault.azure.net"
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=Uri, credential=credential)
        return client.get_secret(secret_name).value
    except:
        raise Exception(f'Failed to read secrets for the secret name : {secret_name}')

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
    try:
        con_str = f'DRIVER=SQL Server;SERVER={host};DATABASE={db_name};ENCRYPT=no;UID={user_name};PWD={password}'
        return pyodbc.connect(con_str)
    except:
        raise Exception(f'Invalid SQL credentials : \n Host : {host} \n DatabaseName : {db_name}\n UserName : {user_name} \n Password : {password}')
    

def get_list_extract_queries(host, db_name, user_name, password, sp_args):
    ''' To get list of rows 
    Args:
        host: Name of DB host/ip address
        db_name: Database name
        user_name: Database user name
        password: Database password
        sp_args: list of arguments for SP
    Returns:
        List: List of rows
    '''
    conn = create_db_connection(host, db_name, user_name, password)               
    rows = exec_sp(conn, config.SP_GET_SCRIPTS
                            .format(    table_name = sp_args[0],
                                        filter_column = sp_args[1],
                                        start_date = sp_args[2],
                                        end_date = sp_args[3]   ))
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


def generate_audit_csv_file():
    ''' To generate the new csv file with column names 
    Returns:
        str: filename for Forms db auditing
    '''

    logging.info(f'Generate the new csv file')
    audit_csv_filename = f'forms_extract_audition_{datetime.utcnow().strftime("%d-%b-%Y %I_%M_%S %p")}'
    with open(f'{audit_csv_filename}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        column_names = ["Type_Of_Data","DB","Source_Table_Name","Last_Run_Date","RunStatus","AffectedRows","ErrorMessage"]
        writer.writerow(column_names)

    return audit_csv_filename


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
    try:
        dt = datetime.utcnow()
        blob_name_prefix = config.BLOB_NAME_PREFIX.format(  year = dt.strftime('%Y'),
                                                            month = dt.strftime('%b'),
                                                            date = dt.strftime('%d-%b-%Y') )
        blob_client = blob_service_client.get_blob_client(container=config.AZURE_STORAGE_ACCOUNT_CONTAINER_NAME, 
                                                        blob=f"{blob_name_prefix}/{file_name}{dt.strftime('_%d-%b-%Y %I:%M:%S %p')}")
        file = open(f".\{file_name}", "rb")
        blob_client.upload_blob(file)
        file.close()
        return True
    except:
        return False

def remove_parquet_file(file_name):
    ''' Remove the parquet file from local
    Args:
        file_name: parquet filename
    '''
    os.remove(f".\{file_name}")


def audit_row_dic(db, script, run_status, affected_rows, error_message):
    ''' To add the row into a dictionary
    Args:
        db: Database name
        table: script
        run_status: parquet file status
        affected_rows: no. of rows affected
        error_message: reason for parquet generation failed
    Returns:
        dict: dict. which contains str(column names) and values
    '''
    row_dic = {'Type_Of_Data': 'raw',
                'DB': db,
                'Source_Table_Name': script,
                'Last_Run_Date': datetime.utcnow(),  
                'RunStatus': run_status,
                'AffectedRows': affected_rows,
                'ErrorMessage':error_message}
    return row_dic

def add_row_into_audit_file(audit_row, audit_csv_filename):
    ''' To add the row into a existing csv file
    Args:
        audit_row: dict contains field_names and values for csv file
        audit_csv_filename: filename for forms db audition 
    '''
    with open(f'{audit_csv_filename}.csv', 'a', newline='') as file:
        field_names = ["Type_Of_Data","DB","Source_Table_Name","Last_Run_Date","RunStatus","AffectedRows","ErrorMessage"]
        dictwriter_object = csv.DictWriter(file, fieldnames=field_names)
        dictwriter_object.writerow(audit_row)


def parquet_job(job_info):
    ''' To generate and upload the parquet file for each tables
    Args:
        job_info: job info with row,username,password,host
    Returns:
        bool: if parquet generate and upload into the blob successfully it returns true else false 
    '''
    logging.info('start')
    script = job_info['result'][1]
    file_name = job_info['result'][2]
    expected_row_count = job_info['result'][3]

    if expected_row_count == 0:
        logging.info(f"expected row count is zero for the script id : {job_info['result'][0]}  filename : {job_info['result'][2]}")
        logging.info('end')
        return False
    
    logging.info(f'script id: {job_info["result"][0]} and filename: {file_name}')
    conn = create_db_connection(job_info['host'], config.FORMS_DB_NAME, job_info['username'], job_info['password'])

    logging.info(f'generating parquet file for filename:  {file_name} script: {script}')
    generate_parquet = generate_parquet_file(script , file_name, conn)

    is_parquet_generated = generate_parquet[0]
    run_status = 'success' if is_parquet_generated else 'fail'
    affected_rows = generate_parquet[1]
    error_message = generate_parquet[2]

    conn.close()

    if is_parquet_generated:
        logging.info(f'upload parquet file to blob {file_name}')
        is_blob_upload = blob_upload(file_name)

        if not is_blob_upload:
            is_parquet_generated = False
            run_status = 'fail'
            logging.info(f'blob upload failed for the filename : {file_name}')

        logging.info(f'deleting local parquet file {file_name}')
        remove_parquet_file(file_name)

    logging.info(f'insert the data into csv file for script id : {job_info["result"][0]}  filename : {file_name}  run status: {run_status}')
    audit_row = audit_row_dic(None, script, run_status, affected_rows, error_message)
    add_row_into_audit_file(audit_row, job_info['audit_csv_filename'])

    logging.info('end')
    return is_parquet_generated
    
def run_jobs(results, forms_db_username, forms_db_password, audit_filename):
    ''' To execute the jobs asynchronously
    Args:
        results: splitted row lists based on max job count
        forms_db_username: username for Forms database 
        forms_db_password: password for Forms database
        audit_filename: filename for Forms db audition
    '''
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for result in results:
            logging.info(f'add result to job info')
            job_info = []
            for row in result:
                local_dic = {   'result': row,
                                'host': config.FORMS_DB_SERVER_HOST, 
                                'username': forms_db_username,
                                'password': forms_db_password,
                                'audit_csv_filename': audit_filename    }
                
                job_info.append(local_dic)
            for row in zip(job_info, executor.map(parquet_job, job_info)):
                if row[1]:
                    logging.info(f"parquet generation completed for the script id : {row[0]['result'][0]}  filename : {row[0]['result'][2]} ") 
                else:
                    logging.info(f"parquet generation failed for the script id : {row[0]['result'][0]}  filename : {row[0]['result'][2]} ") 


if __name__ == "__main__":

    # Read SP arguments from inputs
    sp_args = read_sp_args()

    # Read forms secrets
    forms_db_username = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.FORMS_KEY_VAULT_SECRET_NAME_DB_USER_NAME)
    forms_db_password = read_secret_from_key_vault(config.KEY_VAULT_NAME, config.FORMS_KEY_VAULT_SECRET_NAME_DB_PASSWORD)

    # Get results from sp
    result_set = get_list_extract_queries(config.FORMS_DB_SERVER_HOST, 
                                          config.FORMS_DB_NAME, 
                                          forms_db_username,
                                          forms_db_password,
                                          sp_args)    

    #To split the jobs based on max job count
    job_details = split_jobs(result_set, config.MAX_JOB_COUNT)

    logging.info(f'job_details: {job_details}')

    #To generate the csv file
    audit_filename = generate_audit_csv_file()

    #To execute the jobs asynchronously
    run_jobs(job_details, forms_db_username, forms_db_password, audit_filename)