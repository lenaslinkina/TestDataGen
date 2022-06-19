if __name__ == '__main__':
    import psycopg2
    import functions
    import gen_records
    import genfuns
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
        functions.create_table(connection)

 #       general_first(connection)
        records = (list((gen_records.records_cr())))
        print(len(records))
        genfuns.insert_execute_values(connection, records)
        print("End insert")


        # тут бы вывести сообщение о завершении и результаты измерений
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")
