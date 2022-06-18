if __name__ == '__main__':
    import psycopg2
    import functions
    from config import host, db_name, password, user
    from gener_first import general_first
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password= password,
            database=db_name
        )
        connection.autocommit = True
        exists = functions.table_exists(connection)

        if (exists):
            functions.drop_table(connection)
            functions.create_table(connection)
        else:
            functions.create_table(connection)
            print("[INFO] Table created successfully")
        general_first(connection)
        print("End insert")

                            # тут бы вывести сообщение о завершении и результаты измерений
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")
