import psycopg2
from db.config import host, db_name, password, user


def db_connection():
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    connection.autocommit = True
    return connection


def create_table(connection):
    try:
        print("Cоздание таблицы")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DROP TABLE IF EXISTS gga_index;
                CREATE Table gga_index(
                id serial PRIMARY KEY,
                GGACode TEXT,
                Field TEXT,
                TypeData TEXT,
                Well TEXT,
                PathFile TEXT,
                Dates date
     );
                """
            )
            print("[INFO] Table created successfully")
    except psycopg2.Error as e:
        print(e)


def connection_close(connection):
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
