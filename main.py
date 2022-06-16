import random
import os
import psycopg2
import functions
from config import host, db_name, password, user
from variables import lett, lettEnd, letters, start, end

# содержимое блока try вынести в отдельные функции создание таблицы и генератор что бы было читабельней
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
    # Предусмотреть пересоздание таблицы если такая уже есть или создание ещё одной с именем gga_index_{N+1}
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

    # Заполняем запрос, все в одну таблицу
    codeArr = ["" for x in range(20)]
    for i in range(0, 19):
        lenghtCode = random.randint(5, 20)
        code = "GGA_" + ''.join(random.choice(letters) for i in range(lenghtCode))
        codeArr[i] = "GGA_" + code
        fieldCount = random.randint(10, 50)
        # найди классификтор ГИС (https://ru.wikipedia.org/wiki/Геофизические_исследования_скважин)
        # составь классификатор кодов типов исследований и используй его в генераторе
        tipdan = "none"
        for l in range(0, fieldCount):
            lenghtCode = random.randint(5, 13)
            # используй реальные названия мест-ий, прямо можно захардкодить словрь.  например от сюда
            # https://geonedra.ru/knowledge-base/oil-gas-fields-all-2021/
            field = ''.join(random.choice(lett) for i in range(lenghtCode)) + random.choice(lettEnd)
            countWell = random.randint(10, 200)
            for j in range(0, countWell):
                lenghtCode = random.randint(1, 4)
                # нумерация скважин на месторождении линейная, формата (N+1)_bore_suffix
                # Найди правло для РФ (нумерация скважин на месторождении) и реализуй что бы было похоже на правду.
                well = (str(random.randint(1, 99)) + ''.join(random.choice(letters) for i in range(lenghtCode)))
                countDate = random.randint(3, 20)
                for k in range(0, countDate):
                    lenghtCode = random.randint(1, 4)
                    # путь относительный, формат {code}\{field}\{well}\{tipdan}\{random_date}\{filename}.{ext}
                    path = "C:/Users/lena/Documents/" + str(
                        ''.join(random.choice(letters) for i in range(5))) + "/" + ''.join(
                        random.choice(letters) for i in range(5))
                    countPath = random.randint(1, 2)
                    for u in range(0, countPath):
                        # С датами вроде все норм.
                        random_date = start + (end - start) * random.random()
                        # очень накладно вставлять каждую строчку отдельно
                        # реализуй batch insert
                        # сначала замерий время в варианте как сейчас, потом с массовой вставкой
                        # Вообще, ожидаю тут от тебя примерно такого исследования
                        # (https://hakibenita.com/fast-load-data-python-postgresql)
                        # и оформления твоей авторской статьи в этом репозитории на GitHub
                        # должна разбираться в предмете.
                        # генератор должен создавать 100M записей со скоростью 100К-500К/сек
                        # так же в процессе выводить прогресс раз в 1 сек
                        with connection.cursor() as cursor:
                            cursor.execute(
                                f"""Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES
                            ('{code}', '{field}', '{tipdan}', '{well}', '{path}', '{random_date}');"""
                            )

                        # тут бы вывести сообщение о завершении и результаты измерений
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
