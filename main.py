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


        def recordscount():
            with connection.cursor() as cursor:
                cursor.execute(
                    "select count(*) from gga_index")
                print(f"число записей: {cursor.fetchone()}")
        connection.autocommit = True

        fields = functions.field()
        functions.create_table(connection)
        #Самый быстрый вариант
        gen_records.records_cr(fields, connection)
            # lists.append(records)
            # if (len(lists)>1000000):
        genfuns.copy_string_iterator(connection, gen_records.records_cr(fields), 10000)
        recordscount()

            # copy_string_iterator() # Time # 24.97 # Memory # 0.00390625



        def test():
            functions.create_table(connection)
            genfuns.ingeneral_first(connection, gen_records.records_cr(fields))
            recordscount()
            # ingeneral_first() # Time# 167.9 # Memory # 0.015625

            functions.create_table(connection)
            genfuns.insert_executemany_iterator(connection, gen_records.records_cr(fields))
            recordscount()
            # insert_executemany_iterator()# Time  # 175.1        # Memory        # 0.78125

            functions.create_table(connection)
            genfuns.insert_executemany(connection, gen_records.records_cr(fields))
            recordscount()
            # insert_executemany()        # Time        # 152.3        # Memory        # 0.78125

            functions.create_table(connection)
            genfuns.insert_executemany_iterator(connection, gen_records.records_cr(fields))
            recordscount()
            # insert_executemany_iterator()        # Time        # 170.4        # Memory        # 0.0

            functions.create_table(connection)
            genfuns.insert_execute_batch(connection, gen_records.records_cr(fields), 100)
            recordscount()
            # insert_execute_batch()        # Time        # 101.6        # Memory        # 0.25

            functions.create_table(connection)
            genfuns.insert_execute_batch(connection, gen_records.records_cr(fields), 1000)
            recordscount()
            # insert_execute_batch()        # Time        # 109.7        # Memory        # 0.35546875

            functions.create_table(connection)
            genfuns.insert_execute_batch(connection, gen_records.records_cr(fields), 10000)
            recordscount()
            # insert_execute_batch()        # Time        # 157.1        # Memory        # 0.2421875

            functions.create_table(connection)
            genfuns.insert_execute_batch_iterator(connection, gen_records.records_cr(fields), 100)
            recordscount()
            # insert_execute_batch_iterator()        # Time        # 139.2        # Memory        # 0.25

            functions.create_table(connection)
            genfuns.insert_execute_batch_iterator(connection, gen_records.records_cr(fields), 1000)
            recordscount()
            # insert_execute_batch_iterator()        # Time        # 76.73        # Memory        # 0.0078125

            functions.create_table(connection)
            genfuns.insert_execute_batch_iterator(connection, gen_records.records_cr(fields), 10000)
            recordscount()
            # insert_execute_batch_iterator()        # Time        # 106.3        # Memory        # 0.83203125

            functions.create_table(connection)
            genfuns.insert_execute_values(connection, gen_records.records_cr(fields))
            recordscount()
            # insert_execute_values()        # Time        # 51.59        # Memory        # 0.0

            functions.create_table(connection)
            genfuns.insert_execute_values(connection, gen_records.records_cr(fields), 100)
            # insert_execute_values()        # Time        # 47.35        # Memory        # 0.0
            recordscount()

            functions.create_table(connection)
            genfuns.insert_execute_values(connection, gen_records.records_cr(fields), 1000)
            recordscount()
            # insert_execute_values()        # Time        # 43.35        # Memory        # 0.0078125

            functions.create_table(connection)
            genfuns.insert_execute_values(connection, gen_records.records_cr(fields), 10000)
            # insert_execute_values()        # Time        # 50.21        # Memory        # 0.0
            recordscount()

            functions.create_table(connection)
            genfuns.copy_stringio(connection, gen_records.records_cr(fields))
            recordscount()
            # copy_stringio()        # Time        # 40.36        # Memory        # 0.24609375

            functions.create_table(connection)
            genfuns.copy_string_iterator(connection, gen_records.records_cr(fields), 1000)
            recordscount()
            # copy_string_iterator()        # Time        # 27.34        # Memory        # 0.0

            functions.create_table(connection)
            genfuns.copy_string_iterator(connection, gen_records.records_cr(fields), 10000)
            recordscount()
            # copy_string_iterator()# Time# 25.72 # Memory # 0.0 - лучший


        print(len(list(gen_records.records_cr(fields))))
        print("End insert")
        # тут бы вывести сообщение о завершении и результаты измерений
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")
