import json
import pickle
import random
import functions
import time_memory
from variables import lett, start, end, codes
import json
def write(new_data, filename):

    with open(filename,  encoding = 'utf-8') as f:
        data = json.load(f)
        data.append(new_data)
        with open(filename, 'w', encoding = 'utf-8' ) as outfile:
            json.dump(data, outfile, ensure_ascii=False, indent=2)




records = []
@time_memory.profile
def records_cr(fields, f):



    field = fields[f].get_text()
    code = "GGA_" + ''.join(random.choice(codes) for i in range(1))
    countWell = random.randint(3, 999)
    for w in range(0, countWell):
        well = (str(w)) + ''.join(random.choice(lett) for i in range(1))
        type = functions.type()
        countDate = random.randint(1, 50)
        for k in range(0, countDate):
            random_date = str(start + (end - start) * random.random())
            path = f"{code}/{field}/{well}/{type}/{random_date}/" + "filename.ext"
            tulpe = {'Code': code, 'Field': field, 'Well': well, 'Type': type,'Path': path, 'Dates': random_date}
           # new_data = json.dumps(tulpe)
            records.append(tulpe)
           # write(tulpe, "datarec.json")



#records = json.load("datarec.json")
    return records
