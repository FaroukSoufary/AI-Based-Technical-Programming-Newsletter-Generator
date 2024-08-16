import snowflake.connector



"""
Uses the snowflake python connector to create a snowflake stage and then links it to the azure storage account
For this project, you will need to create two stages, one for questions container and one for answers container

"""

def create_snowflake_stage(stage_name, snowflake_acc, snowflake_user, snowflake_password, snowflake_db, snowflake_schema, snowflake_wh, azure_container_name, azure_storage_acc, azure_sas_token):

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_acc,
        warehouse=snowflake_wh,
        database=snowflake_db,
        schema=snowflake_schema
    )

    create_stage_sql = f"""CREATE OR REPLACE STAGE {stage_name} URL = 'azure://{azure_storage_acc}.blob.core.windows.net/{azure_container_name}'
    CREDENTIALS = (
        AZURE_SAS_TOKEN='{azure_sas_token}'
    );"""

    conn.cursor().execute(create_stage_sql)

    conn.close()
