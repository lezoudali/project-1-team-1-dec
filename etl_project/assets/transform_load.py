from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy import MetaData
from etl_project.connectors.postgresql import PostgreSqlClient


def staging_transform(
    sql_template: Template,
    postgresql_client: PostgreSqlClient,
    source_table_name: str
):
    """Transform staging table."""
    extract_type = sql_template.make_module().config.get("extract_type")
    if extract_type == "full":
        sql = sql_template.render()
        return [dict(row) for row in postgresql_client.execute_sql(sql)]
    elif extract_type == "incremental":
        # incremental extraction is based on current date
        current_date = datetime.today().strftime('%Y-%m-%d')
        sql = sql_template.render(
            is_incremental=True, 
            source_table_name=source_table_name,
            incremental_value=current_date
        )
        return [dict(row) for row in postgresql_client.execute_sql(sql)]
    else:
        raise Exception(
            f"Extract type {extract_type} is not supported. Please use either 'full' or 'incremental' extract type."
        )


def transform_load(
    environment_path: str,
    postgresql_client: PostgreSqlClient,
    metadata: MetaData,
    source_table_name: str,
    target_table_name: str
):
    """Create serving table based off a staging table and load to database."""
    environment = Environment(loader=FileSystemLoader(environment_path))
    for sql_path in environment.list_templates():
        sql_template = environment.get_template(sql_path)

        # check if source table exists in source database
        if postgresql_client.table_exists(
            source_table_name
        ):
            # extract data from staging and apply transformation defined in sql template
            data = staging_transform(
                sql_template=sql_template,
                postgresql_client=postgresql_client,
                source_table_name=source_table_name
            )

            postgresql_client.upsert(
                data=data,
                table=target_table_name,
                metadata=metadata
            )
        else:
            print("Table {source_table_name} does not exist in source database. Transformation and load did not happen.")
