from array import array

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
        fields = functions.field()
        records = []
        for f in range(0, len(fields) - 1):
            if(len(records)<500000):
                records.append(gen_records.records_cr(fields, f))
            else:
                genfuns.copy_string_iterator(connection, records, 10000)
                records.clear()


        #records = gen_records.records_cr()



 #       general_first(connection)
        #records = (((gen_records.records_cr())))
        #gen_records.records_cr()
        #print(len(records))
        # genfuns.ingeneral_first(connection, records)
        # functions.create_table(connection)
        #genfuns.insert_executemany_iterator(connection, records)
        # functions.create_table(connection)
        # genfuns.insert_executemany(connection, records)
        # functions.create_table(connection)
        # genfuns.insert_executemany_iterator(connection, records)
        # functions.create_table(connection)
        # genfuns.insert_execute_batch(connection, records, 100)
        # functions.create_table(connection)
        # genfuns.insert_execute_batch(connection, records, 1000)
        # functions.create_table(connection)
        # genfuns.insert_execute_batch(connection, records, 10000)
        # functions.create_table(connection)
        # genfuns.insert_execute_batch_iterator(connection, records, 100)
        # functions.create_table(connection)
        # genfuns.insert_execute_batch_iterator(connection, records, 1000)
        # functions.create_table(connection)
        # genfuns.insert_execute_batch_iterator(connection, records, 10000)
        # functions.create_table(connection)
        # genfuns.insert_execute_values(connection, records)
        # functions.create_table(connection)
        # genfuns.insert_execute_values(connection, records, 100)
        # functions.create_table(connection)
        # genfuns.insert_execute_values(connection, records, 1000)
        # functions.create_table(connection)
        # genfuns.insert_execute_values(connection, records, 10000)
        # functions.create_table(connection)
        # genfuns.copy_stringio(connection, records)
        # functions.create_table(connection)
        # genfuns.copy_string_iterator(connection, records, 1000)
        # functions.create_table(connection)
        # genfuns.copy_string_iterator(connection, records, 10000)
        # functions.create_table(connection)
        # genfuns.copy_string_iterator(connection, records, 10000)
        print("End insert")


        # тут бы вывести сообщение о завершении и результаты измерений
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")
