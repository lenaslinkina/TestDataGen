import random
from datetime import time, datetime

import psycopg2
import psycopg2.extras
import time_memory
from typing import Iterator, Dict, Any, Optional
import io


@time_memory.profile
def ingeneral_first(connection, alls: Iterator[Dict[str, Any]])-> None:
    try:
        with connection.cursor() as cursor:
            for all in alls:
                cursor.execute("""
                    Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES (
                        %(Code)s,
                        %(Field)s,
                        %(Type)s,
                        %(Well)s,
                        %(Path)s,
                        %(Dates)s
                    );
                """, {**all})
    except psycopg2.Error as e:
        print(e)

@time_memory.profile
def insert_executemany_iterator(connection, records: Iterator[Dict[str, Any]])-> None:
    try:
        with connection.cursor() as cursor:
            cursor.executemany("""
                    Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES (
                            %(Code)s,
                            %(Field)s,
                            %(Type)s,
                            %(Well)s,
                            %(Path)s,
                            %(Dates)s
                   );
               """, ({
                **record
            } for record in records))

    except psycopg2.Error as e:
        print(e)

@time_memory.profile
def insert_executemany(connection, records: Iterator[Dict[str, Any]]) -> None:
    try:
        with connection.cursor() as cursor:


            all_records = [{ **record} for record in records]


            cursor.executemany("""
                 Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES (
                            %(Code)s,
                            %(Field)s,
                            %(Type)s,
                            %(Well)s,
                            %(Path)s,
                            %(Dates)s
                );
            """, all_records)

    except psycopg2.Error as e:
        print(e)


@time_memory.profile
def insert_executemany_iterator(connection, records: Iterator[Dict[str, Any]]) -> None:
    try:
        with connection.cursor() as cursor:
            cursor.executemany("""
                    Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES (
                            %(Code)s,
                            %(Field)s,
                            %(Type)s,
                            %(Well)s,
                            %(Path)s,
                            %(Dates)s
                );
               """, ({
                **record } for record in records))
    except psycopg2.Error as e:
        print(e)

@time_memory.profile
def insert_execute_batch(connection, records: Iterator[Dict[str, Any]], page_size: int = 100) -> None:
    try:
        with connection.cursor() as cursor:
            all_records = [{ **record} for record in records]
            psycopg2.extras.execute_batch(cursor, """
                Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES (
                                %(Code)s,
                                %(Field)s,
                                %(Type)s,
                                %(Well)s,
                                %(Path)s,
                                %(Dates)s
                    );
            """, all_records, page_size=page_size)
    except psycopg2.Error as e:
        print(e)
@time_memory.profile
def insert_execute_batch_iterator(connection, records: Iterator[Dict[str, Any]], page_size: int = 100) -> None:
    try:
        with connection.cursor() as cursor:
            iter_records = ({
                **record} for record in records)

            psycopg2.extras.execute_batch(cursor, """
                Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES (
                                %(Code)s,
                                %(Field)s,
                                %(Type)s,
                                %(Well)s,
                                %(Path)s,
                                %(Dates)s
                );
            """, iter_records, page_size=page_size)
    except psycopg2.Error as e:
        print(e)

@time_memory.profile
def insert_execute_values(connection, records: Iterator[Dict[str, Any]]) -> None:
    try:
        with connection.cursor() as cursor:

            psycopg2.extras.execute_values(cursor, """
            Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES %s;""",  [(
                                record['Code'],
                                record['Field'],
                                record['Type'],
                                record['Well'],
                                record['Path'],
                                record['Dates']
                )for record in records]);
    except psycopg2.Error as e:
        print(e)

@time_memory.profile
def insert_execute_values(connection, records: Iterator[Dict[str, Any]], page_size: int = 100) -> None:
    try:
        with connection.cursor() as cursor:

            psycopg2.extras.execute_values(cursor, """
            Insert into gga_index (Code, Field, TypeData, Well, PathFile, Dates) VALUES %s;""",  ((
                                record['Code'],
                                record['Field'],
                                record['Type'],
                                record['Well'],
                                record['Path'],
                                record['Dates']
                )for record in records ),page_size=page_size);
    except psycopg2.Error as e:
        print(e)


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


@time_memory.profile
def copy_stringio(connection, records: Iterator[Dict[str, Any]]) -> None:
    with connection.cursor() as cursor:

        csv_file_like_object = io.StringIO()
        for record in records:

            csv_file_like_object.write('|'.join(map(clean_csv_value, (

                record['Code'],
                record['Field'],
                record['Type'],
                record['Well'],
                record['Path'],
                record['Dates'],
            ))) + '\n')

        csv_file_like_object.seek(0)
        cursor.copy_from(csv_file_like_object, 'gga_index', sep='|', columns=('code', 'field', 'typedata',  'well','pathfile', 'dates'))


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


@time_memory.profile
def copy_string_iterator(connection, records: Iterator[Dict[str, Any]], size: int = 8192) -> None:

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
        cursor.copy_from(records_string_iterator, 'gga_index', sep='|', columns=('code', 'field', 'typedata', 'well', 'pathfile',  'dates'), size=size)


