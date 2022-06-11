import random
import psycopg2
from config import host, db_name, password, user
from variables import lett, lettEnd, letters, start, end

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
#Заполняем запрос, все в одну таблицу
    codeArr = ["" for x in range(20)]
    for i in range(0, 19):
        lenghtCode = random.randint(5, 20)
        code = "GGA_" + ''.join(random.choice(letters) for i in range(lenghtCode))
        codeArr[i] = "GGA_" + code
        fieldCount = random.randint(10, 50)
        tipdan="none"
        for l in range(0, fieldCount):
            lenghtCode = random.randint(5, 13)
            field = ''.join(random.choice(lett) for i in range(lenghtCode)) + random.choice(lettEnd)
            countWell = random.randint(10, 200)
            for j in range(0, countWell):
                lenghtCode = random.randint(1, 4)
                well = (str(random.randint(1, 99)) + ''.join(random.choice(letters) for i in range(lenghtCode)))
                countDate = random.randint(3, 20)
                for k in range(0, countDate):
                    lenghtCode = random.randint(1, 4)
                    path = "C:/Users/lena/Documents/"+str(''.join(random.choice(letters) for i in range(5))) + "/" +  ''.join(random.choice(letters) for i in range(5))
                    countPath = random.randint(1, 2)
                    for u in range(0, countPath):
                        random_date = start + (end - start) * random.random()
                        with connection.cursor() as cursor:
                            cursor.execute(
                            f"""Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES
                            ('{code}', '{field}', '{tipdan}', '{well}', '{path}', '{random_date}');"""
                            )
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")















