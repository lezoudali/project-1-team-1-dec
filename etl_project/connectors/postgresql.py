from sqlalchemy import create_engine, MetaData, Table, Column, inspect
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    def __init__(self, server_name: str, database_name: str, username: str, password: str, port: int):
        self.server_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            host=server_name,
            database=database_name,
            port=port,
            username=username,
            password=password
        )

        self.engine = create_engine(connection_url)

    def get_metadata(self) -> MetaData:
        """
        Gets the metadata object for all tables for a given database
        """
        metadata = MetaData(bind=self.engine)
        metadata.reflect()
        return metadata

    def execute_sql(self, sql: str) -> None:
        return self.engine.execute(sql).all()

    def table_exists(self, table_name: str) -> bool:
        """
        Checks if the table already exists in the database.
        """
        return inspect(self.engine).has_table(table_name)
    
    def create_table(self, table_name: str, metadata: MetaData) -> None:
        """
        Create table provided in the metadata object.
        """
        existing_table = metadata.tables[table_name]
        new_metadata = MetaData()
        columns = [
            Column(column.name, column.type, primary_key=column.primary_key)
            for column in existing_table.columns
        ]
        new_table = Table(table_name, new_metadata, *columns)
        new_metadata.create_all(bind=self.engine)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Upserts data into a database table. This method creates the table also if it doesn't exist.
        """
        self.create_table(table_name=table.name, metadata=metadata)
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)
