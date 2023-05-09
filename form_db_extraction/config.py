# FORM database details
FORM_DB_SERVER_HOST = 'localhost\SQLEXPRESS'
FORM_DB_NAME = 'form_dev'

# Azure Key vault name
KEY_VAULT_NAME = 'keyvault-dev'

# Azure keyvault-dev Key vault secrets
FORM_KEY_VAULT_SECRET_NAME_DB_USER_NAME = 'SourceFormUserName'
FORM_KEY_VAULT_SECRET_NAME_DB_PASSWORD = 'SourceFormPwd'

# Max job count
MAX_JOB_COUNT = 5

# Azure storage account connection string
AZURE_STORAGE_ACCOUNT_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storageaccountuh;AccountKey=smab4yOdWek5Mg4+lf89xFB50ggG9KeDhHXszVqa/sibs9rMlYHy6CMH54ASm9dxd0V94toOmEjG+AStoYY3AQ==;EndpointSuffix=core.windows.net"

# Azure container name
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = "raw"

# Azure blob name
BLOB_NAME_PREFIX = "Form/{year}/{month}/{date}"

# Stored procedure to get list of scripts
SP_GET_SCRIPTS = "EXEC usp_GenerateExtractionScripts_Monthly '{table_name}','{filter_column}','{start_date}','{end_date}'"
