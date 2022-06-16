import psycopg2
def table_exists(connection):
    exists = False
    try:
        cur = connection.cursor()
        cur.execute("select exists(select relname from pg_class where relname='gga_index')")
        exists = cur.fetchone()[0]
        print(exists)
    except psycopg2.Error as e:
        print(e)
    return exists

def create_table(connection):
    try:
        print("создание таблицы")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE Table gga_index(
                id serial PRIMARY KEY,
                Code varchar(50) NOT NULL,
                Field varchar(50) NOT NULL,
                TypeData varchar(50) NOT NULL,
                Well varchar(50) NOT NULL,
                PathFile varchar(200) NOT NULL,
                Dates date NOT NULL
     );
                """
            )
            print("[INFO] Table created successfully")
    except psycopg2.Error as e:
        print(e)

def drop_table(connection):
    exists = False
    try:
        print("удаление таблицы")
        with connection.cursor() as cursor:
            cursor.execute("drop table " + "gga_index" )
    except psycopg2.Error as e:
        print(e)



