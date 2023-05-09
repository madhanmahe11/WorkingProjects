# EDW database details to get GP database list
EDW_DB_SERVER_HOST='localhost\SQLEXPRESS'
EDW_DB_NAME='edw_dev'

# GP database details
GP_DB_SERVER_HOST='localhost\SQLEXPRESS'

# Azure Key vault name
KEY_VAULT_NAME='keyvault-dev'

# Azure keyvault-dev Key vault secrets
EDW_KEY_VAULT_SECRET_NAME_DB_USER_NAME='SourceEDWUserName'
EDW_KEY_VAULT_SECRET_NAME_DB_PASSWORD='SourceEDWPwd'
GP_KEY_VAULT_SECRET_NAME_DB_USER_NAME='SourceGPUserName'
GP_KEY_VAULT_SECRET_NAME_DB_PASSWORD='SourceGPPwd'

# Query to get GP database
QUERY_GET_GP_DB=""" select distinct GP_Database_Name from [dbo].[Dim_Location] 
                            where 	Is_Active = 'Y' 
                                and GP_Database_Name is not null
                                and GP_Database_Name <> '' 
                        """

# Max job count
MAX_JOB_COUNT=5

# Query template to get all table for a DB
QUERY_GET_ALL_TABLES_FOR_DB= """  SELECT TABLE_NAME
                                  FROM {db_name}.INFORMATION_SCHEMA.TABLES 
                                  WHERE TABLE_TYPE = 'BASE TABLE'
                            """

# Query template to get all records in a Table
QUERY_GET_ALL_RECORDS_FROM_TABLE= """  SELECT * FROM [{table_name}]  """

# Azure storage account connection string
AZURE_STORAGE_ACCOUNT_CONNECTION_STRING= "DefaultEndpointsProtocol=https;AccountName=storageaccountuh;AccountKey=smab4yOdWek5Mg4+lf89xFB50ggG9KeDhHXszVqa/sibs9rMlYHy6CMH54ASm9dxd0V94toOmEjG+AStoYY3AQ==;EndpointSuffix=core.windows.net"

# Azure container name
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME="raw"

# Azure blob name
BLOB_NAME_PREFIX = "GP/{year}/{month}/{date}/{db_name}"


