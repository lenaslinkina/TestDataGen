import numpy
import random
import string
import psycopg2
from config import host, db_name, password, user
from datetime import datetime, timedelta, date
from faker import Faker
try:
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    connection.autocommit=True

    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE Table test(
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
    #     print("[INFO] Table created successfully")
    fake = Faker()

    print(fake.name())
    lett = 'ЦУКЕНГШЩЗХФВАПРОЛДЖЭЯЧСМИТБЮ'
    lettEnd = ['АЯ', 'ОЕ']
    letters = string.ascii_uppercase
    arrays = ["ewfer", "sdsdf", "wwdwdwe"]
    single_random_choice = numpy.random.choice(arrays, size=1)
    print("один случайный выбор из массива 1-D", single_random_choice, "^^^", ''.join(random.choice(letters) for i in range(1)))
    rand_string = ''.join(random.choice(letters) for i in range(1))


#Заполнение дат
    min_year=1990
    max_year=datetime.now().year

    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    countDate = random.randint(1, 50)


#Запрос должен вкелючить все строки

    codeArr = ["" for x in range(20)]
    for i in range(0, 19):
        lenghtCode = random.randint(5, 20)
        code = "GGA_" + ''.join(random.choice(letters) for i in range(lenghtCode))
        codeArr[i] = "GGA_" + code
        ranges = random.randint(10, 40)
        tipdan="none"
        for i in range(0, ranges):
            lenghtCode = random.randint(5, 13)
            field = ''.join(random.choice(lett) for i in range(lenghtCode)) + random.choice(lettEnd)
            countSkv = random.randint(10, 500)
            for i in range(0, countSkv):
                lenghtCode = random.randint(1, 4)
                well = (str(random.randint(1, 99)) + ''.join(random.choice(letters) for i in range(lenghtCode)))
                wellCount = random.randint(3, 300)
                for k in range(0, wellCount):
                    lenghtCode = random.randint(1, 4)
                    path = "C:/Users/lena/Documents/"+str(''.join(random.choice(letters) for i in range(5))) + "\\" +  ''.join(random.choice(letters) for i in range(5))
                    random_date = start + (end - start) * random.random()

                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"""Insert into test (Code, Field, TypeData, Well, PathFile, Dates) VALUES
                            ('{code}', '{field}', '{tipdan}', '{well}', '{path}', '{random_date}');"""
                        )
finally:
    if connection:
        connection.close()
        print(f"Insert into test (Code, Field, TypeData, Well, PathFile, Dates) VALUES ('{code}', '{field}', '{tipdan}', '{well}', '{path}', {random_date});")
        print("[INFO] PostgreSQL connection closed")















