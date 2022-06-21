import json
import pickle
import random
import functions
import genfuns
import time_memory
from variables import lett, start, end, codes
import json
def write(new_data, filename):

    with open(filename,  encoding = 'utf-8') as f:
        data = json.load(f)
        data.append(new_data)
        with open(filename, 'w', encoding = 'utf-8' ) as outfile:
            json.dump(data, outfile, ensure_ascii=False, indent=2)



ranges = []

@time_memory.profile
def records_cr(fields, connection):
    for f in range(0, len(fields)):
        field = fields[f].get_text()
        code = "GGA_" + ''.join(random.choice(codes) for i in range(1))
        countWell = random.randint(3, 999)
        if (len(ranges)>1500000):
            genfuns.copy_string_iterator(connection, ranges, 100000)
            ranges.clear()
        for w in range(0, countWell):
            well = (str(w+1)) + ''.join(random.choice(lett) for i in range(1))
            type = functions.type()
            countDate = random.randint(1, 50)
            for k in range(0, countDate):
                random_date = str(start + (end - start) * random.random())
                path = f"{code}/{field}/{well}/{type}/{random_date}/" + "filename.ext"
                tulpe = {'Code': code, 'Field': field, 'Well': well, 'Type': type,'Path': path, 'Dates': random_date}
                ranges.append(tulpe)


               # new_data = json.dumps(tulpe)

               # write(tulpe, "datarec.json")



#records = json.load("datarec.json")

