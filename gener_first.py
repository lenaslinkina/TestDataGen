
import random
import psycopg2
import functions
from variables import lett, start, end, codes
import time_memory
@time_memory.profile
def general_first(connection):
    exists = False
    try:

        for i in range(len(codes)):
            fields = functions.field()
            code ="GGA_" + ''.join(random.choice(codes) for i in range(1))
           # for l in range(0, len(fields)):
            # пока пусть 100 остается
            for f in range(0, 2):
                field = fields[f].get_text()
                countWell = random.randint(10, 20)
                for w in range(0, countWell):
                    well = (str(w)) + ''.join(random.choice(lett) for i in range(1))
                    type = functions.type()
                    countDate = random.randint(3, 4)
                    for k in range(0, countDate):
                       random_date = start + (end - start) * random.random()
                       path = f"{code}\{field}\{well}\{type}\{random_date}\\" + "filename.ext"
                       with connection.cursor() as cursor:
                           cursor.execute(
                                    f"""Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES
                                   ('{code}', '{field}', '{type}', '{well}', '{path}', '{random_date}');"""
                            )
    except psycopg2.Error as e:
        print(e)
# Заполняем запрос, все в одну таблицу

