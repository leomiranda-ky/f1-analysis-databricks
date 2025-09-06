SCOPE = dbutils.secrets.listScopes()[0].name
CONTAINERS = ["raw", "processed", "presentation", "demo"]


def mount_adls(storage_account_name: str, container_name: str, scope: str):
    """
    Mount an Azure Data Lake Storage Gen2 (ADLS) container into Databricks File System (DBFS).

    This function uses a service principal stored in a Databricks secret scope to
    authenticate with Azure and mount a given ADLS Gen2 container to /mnt/<storage>/<container>.

    Args:
        storage_account_name (str): The name of the Azure Storage Account.
        container_name (str): The name of the ADLS container to mount (e.g., "raw", "processed").
        scope (str): The Databricks secret scope containing:
            - f1-app-client-id
            - f1-app-tenant-id
            - f1-app-secret

    Behavior:
        - If the mount point already exists, it will be unmounted and remounted.
        - Mounts the container under `/mnt/{storage_account_name}/{container_name}`.

    Example:
        >>> mount_adls("f1lake", "raw", "f1-scope")
        # Data is accessible at: /mnt/f1lake/raw

    Notes:
        - Requires that the Databricks secret scope is already created with the proper keys.
    """
    client_id = dbutils.secrets.get(scope, "f1-app-client-id")
    tenant_id = dbutils.secrets.get(scope, "f1-app-tenant-id")
    client_secret = dbutils.secrets.get(scope, "f1-app-secret")

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }

    mount_point = f"/mnt/{storage_account_name}/{container_name}"
    if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_point)

    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs,
    )


# Run mounts for all containers
for container in CONTAINERS:
    mount_adls("f1lake", container, SCOPE)
