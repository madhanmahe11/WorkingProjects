# FORMS database details
FORMS_DB_SERVER_HOST = ''  # Connection Server
FORMS_DB_NAME = '' # DatabaseName

# Azure Key vault name
KEY_VAULT_NAME = ''  # Azure Keyvault Name

# Azure keyvault-dev Key vault secrets
FORMS_KEY_VAULT_SECRET_NAME_DB_USER_NAME = '' # Azure Keyvault SecretName For DBUserName
FORMS_KEY_VAULT_SECRET_NAME_DB_PASSWORD = ''  # Azure Keyvault SecretName For DBUserPassword

# Max job count
MAX_JOB_COUNT = 5

# Azure storage account connection string
AZURE_STORAGE_ACCOUNT_CONNECTION_STRING = "" # Azure Storage Account Connection String

# Azure container name
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = ""   # Azure ContainerName

# Azure blob name
BLOB_NAME_PREFIX = "Forms/{year}/{month}/{date}"

# Stored procedure to get list of scripts
SP_GET_SCRIPTS = "EXEC usp_GenerateExtractionScripts_Monthly '{table_name}','{filter_column}','{start_date}','{end_date}'"
