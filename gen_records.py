import random
import functions
import genfuns
from variables import lett, start, end, codes

ranges = []

def records_cr(fields, connection):
    for f in range(0, len(fields)-1):

        field = fields[f].get_text()
        code = "GGA_" + ''.join(random.choice(codes) for i in range(1))
        countWell = random.randint(3, 999)
        print("Сгенерировано записей: "+ str(len(ranges)))
        if (len(ranges)>1500000):
            genfuns.copy_string_iterator(connection, ranges, 100000)
            recordscount(connection)
            ranges.clear()
        for w in range(0, countWell):
            well = (str(w+1)) + ''.join(random.choice(lett) for i in range(1))
            type = functions.type()
            countDate = random.randint(1, 50)
            for k in range(0, countDate):
                random_date = str(start + (end - start) * random.random())
                path = f"{code}/{field}/{well}/{type}/{random_date}/" + "filename.ext"
                tuple = {'Code': code, 'Field': field, 'Well': well, 'Type': type,'Path': path, 'Dates': random_date}
                ranges.append(tuple)

                #yield tulpe






def recordscount(connection):
     with connection.cursor() as cursor:
                cursor.execute(
                    "select count(*) from gga_index")
                print(f"число записей : {cursor.fetchone()}")




