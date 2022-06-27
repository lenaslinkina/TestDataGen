from data import get_data
import insert_records_test


def test(connection):
    fields = get_data.get_fields()
    dbQueries.create_table(connection)
    insert_records_test.ingeneral_first(connection, fields)
    # ingeneral_first() # Time# 167.9 # Memory # 0.015625

    dbQueries.create_table(connection)
    insert_records_test.insert_executemany_iterator(connection, fields)
    # С итератором
    # insert_executemany_iterator()# Time  # 175.1        # Memory        # 0.78125

    dbQueries.create_table(connection)
    insert_records_test.insert_executemany(connection, fields)
    # executemany(query, vars_list)  Выполните операцию с базой данных (запрос или команду) для всех кортежей параметров или сопоставлений, найденных в последовательности vars_list.
    # insert_executemany()        # Time        # 152.3        # Memory        # 0.78125

    dbQueries.create_table(connection)
    insert_records_test.insert_executemany_iterator(connection, fields)
    # insert_executemany_iterator()        # Time        # 170.4        # Memory        # 0.0

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_batch(connection, fields)
    # psycopg2.extras.execute_batch(cur, sql, argslist, page_size=100) Выполняйте группы инструкций за меньшее количество обходов сервера + page_size
    # insert_execute_batch()        # Time        # 101.6        # Memory        # 0.25

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_batch(connection, fields, 1000)
    # insert_execute_batch()        # Time        # 109.7        # Memory        # 0.35546875

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_batch(connection, fields, 10000)
    # insert_execute_batch()        # Time        # 157.1        # Memory        # 0.2421875

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_batch_iterator(connection, fields, 100)
    # c итератором
    # insert_execute_batch_iterator()        # Time        # 139.2        # Memory        # 0.25

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_batch_iterator(connection, fields, 1000)
    # insert_execute_batch_iterator()        # Time        # 76.73        # Memory        # 0.0078125

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_batch_iterator(connection, fields, 10000)
    # insert_execute_batch_iterator()        # Time        # 106.3        # Memory        # 0.83203125

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_values(connection, fields)
    # psycopg2.extras.execute_values(cur, sql, argslist, template=None, page_size=100, fetch=False)
    # Функция execute_valuesработает, генерируя огромный список ЗНАЧЕНИЙ для запроса.
    # insert_execute_values()        # Time        # 51.59        # Memory        # 0.0

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_values(connection, fields, 100)
    # то же самое c page, почти такие же результаты
    # insert_execute_values()        # Time        # 47.35        # Memory        # 0.0
    insert_records_test.insert_execute_values(connection, fields, 1000)
    # insert_execute_values()        # Time        # 43.35        # Memory        # 0.0078125

    dbQueries.create_table(connection)
    insert_records_test.insert_execute_values(connection, fields, 10000)
    # insert_execute_values()        # Time        # 50.21        # Memory        # 0.0

    dbQueries.create_table(connection)
    insert_records_test.copy_stringio(connection, fields)
    # COPYКоманда оптимизирована для загрузки большого количества строк; она менее гибкая, чем INSERT, но требует значительно меньших затрат при больших загрузках данных.
    # примерно такой же результат по времени
    # copy_stringio()        # Time        # 40.36        # Memory        # 0.24609375

    dbQueries.create_table(connection)
    insert_records_test.copy_string_iterator(connection, fields, 1000)
    # c page самый быстрый результат
    # copy_string_iterator()        # Time        # 27.34        # Memory        # 0.0

    dbQueries.create_table(connection)
    insert_records_test.copy_string_iterator(connection, fields, 10000)
    # почти то же самое по времени
    # copy_string_iterator()# Time# 25.72 # Memory # 0.0 - лучший

