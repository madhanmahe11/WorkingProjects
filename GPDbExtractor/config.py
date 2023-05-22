# EDW database details to get GP database list
EDW_DB_SERVER_HOST = '' # Connection Server
EDW_DB_NAME = ''  # DatabaseName
EDW_DB_USER_NAME = ''  # Azure DBUserName

# GP database details
GP_DB_SERVER_HOST = ''  # Connection Server
GP_DB_USER_NAME = ''    # Azure DBUserName

# Azure Key vault name
KEY_VAULT_NAME = ''  # Azure Keyvault Name

# Azure keyvault-dev Key vault secrets
EDW_KEY_VAULT_SECRET_NAME_DB_PASSWORD = ''        # Azure Keyvault SecretName For EDW DBUserPassword

GP_KEY_VAULT_SECRET_NAME_DB_PASSWORD = ''          # Azure Keyvault SecretName For GP DBUserPassword

# Query to get GP database
QUERY_GET_GP_DB = """ select distinct GP_Database_Name from [dbo].[Dim_Location] 
                            where 	Is_Active = 'Y' 
                                and GP_Database_Name is not null
                                and GP_Database_Name <> '' 
                        """

# Max job count
MAX_JOB_COUNT = 5

# Query template to get all table for a DB
QUERY_GET_ALL_TABLES_FOR_DB = """  SELECT TABLE_NAME
                                  FROM {db_name}.INFORMATION_SCHEMA.TABLES 
                                  WHERE TABLE_TYPE = 'BASE TABLE'
                            """

# Query template to get all records in a Table
QUERY_GET_ALL_RECORDS_FROM_TABLE = """  SELECT * FROM [{table_name}]  """

AZURE_STORAGE_ACCOUNT_NAME = ""    # Azure Storage Account Name

AZURE_STORAGE_ACCOUNTKEY_KEY_VAULT_SECRET_NAME = ""      # Azure Storage AccountKey Keyvault Secret Name 

# Azure container name
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = ""       # Azure ContainerName

# Azure storage account connection string
AZURE_STORAGE_ACCOUNT_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName={accountname};AccountKey={accountkey};EndpointSuffix=core.windows.net"    # Azure Storage Account Connection String

# Azure blob name
BLOB_NAME_PREFIX = "GP/{year}/{month}/{date}"       # Azure Blob Name Prefix

# Hour to schedule the job
HOUR_TO_SCHEDULE_JOB = 1