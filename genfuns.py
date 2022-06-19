import random
import psycopg2
import psycopg2.extras
import time_memory
from typing import Iterator, Dict, Any



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