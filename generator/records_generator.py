import random
import io
from data import get_data
from typing import Optional, Iterator, Any, Dict
from data.variables import lett, start, end, codes


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


class StringIteratorIO(io.TextIOBase):

    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


def inserting_records(connection, records: Iterator[Dict[str, Any]], size: int = 8192) -> None:
    with connection.cursor() as cursor:
        records_string_iterator = StringIteratorIO((
            '|'.join(map(clean_csv_value, (
                record['Code'],
                record['Field'],
                record['Type'],
                record['Well'],
                record['Path'],
                record['Dates'],
            ))) + '\n'
            for record in records
        ))
        cursor.copy_from(records_string_iterator, 'gga_index', sep='|',
                         columns=('code', 'field', 'typedata', 'well', 'pathfile', 'dates'), size=size)


def create_insert_records(connection):
    ranges = []
    fields = get_data.get_fields()
    for f in range(0, len(fields) - 1):

        field = fields[f]
        code = "GGA_" + ''.join(random.choice(codes) for i in range(1))
        count_well = random.randint(3, 999)
        print("Сгенерировано записей: " + str(len(ranges)))
        if (len(ranges) > 1500000):
            print("Вставка записей в базу")
            inserting_records(connection, ranges, 100000)
            count_records(connection)
            ranges.clear()
        for w in range(0, count_well):
            well = (str(w + 1)) + ''.join(random.choice(lett) for i in range(1))
            type = get_data.get_types()
            count_date = random.randint(1, 50)
            for k in range(0, count_date):
                random_date = str(start + (end - start) * random.random())
                path = f"{code}/{field}/{well}/{type}/{random_date}/" + "filename.ext"
                tuple = {'Code': code, 'Field': field, 'Well': well, 'Type': type, 'Path': path, 'Dates': random_date}
                ranges.append(tuple)

    print("Конец вставки записей в базу")


def count_records(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            "select count(*) from gga_index")
        print(f"число записей : {cursor.fetchone()}")
