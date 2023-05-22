# FORMS database details
FORMS_DB_SERVER_HOST = 'BTG-TRN2016'  # Connection Server
FORMS_DB_NAME = 'FORMS_STAGE' # DatabaseName
FORMS_DB_USER_NAME = 'bidev'  # DBUserName

# Azure Key vault name
KEY_VAULT_NAME = 'keyvault-btg-bi-dev'  # Azure Keyvault Name

# Azure keyvault-dev Key vault secrets
FORMS_KEY_VAULT_SECRET_NAME_DB_PASSWORD = 'SourceStage'  # Azure Keyvault SecretName For DBUserPassword

# Max job count
MAX_JOB_COUNT = 5

AZURE_STORAGE_ACCOUNT_NAME = "storageaccountbtgbidev"    # Azure Storage Account Name

AZURE_STORAGE_ACCOUNTKEY_KEY_VAULT_SECRET_NAME = "dlaccesskey"      # Azure Storage AccountKey Keyvault Secret Name 

# Azure container name
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = "raw"       # Azure ContainerName

# Azure storage account connection string
AZURE_STORAGE_ACCOUNT_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName={accountname};AccountKey={accountkey};EndpointSuffix=core.windows.net"    # Azure Storage Account Connection String

# Azure blob name
BLOB_NAME_PREFIX = "Forms/{year}/{month}/{date}"

# Stored procedure to get list of scripts
SP_GET_SCRIPTS = "EXEC usp_GenerateExtractionScripts_Monthly '{table_name}','{filter_column}','{start_date}','{end_date}'"

# Hour to schedule the job
HOUR_TO_SCHEDULE_JOB = 1