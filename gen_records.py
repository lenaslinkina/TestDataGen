import json
import random
import functions
from variables import lett, start, end, codes


records = []
def records_cr():
        count = 0
        for i in range(len(codes)-1):
            fields = functions.field()
            code = "GGA_" + ''.join(random.choice(codes) for i in range(1))
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
                        tulpe = {'Code': code, 'Field': field, 'Well': well, 'Type': type,'Path': path, 'Dates': random_date}
                        records.append(tulpe)

                        count += 1


        return records
