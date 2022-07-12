from generator import records_generator
from db import db_queries

if __name__ == '__main__':
    try:
        connection = db_queries.db_connection()
        db_queries.create_table(connection)
        records_generator.create_insert_records(connection)
    finally:
        db_queries.connection_close(connection)
