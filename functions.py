import psycopg2
from bs4 import BeautifulSoup
import random
import lxml
# def table_exists(connection):
#     exists = False
#     try:
#         cur = connection.cursor()
#         cur.execute("select exists(select relname from pg_class where relname='gga_index')")
#         exists = cur.fetchone()[0]
#         print(exists)
#     except psycopg2.Error as e:
#         print(e)
#     return exists

def create_table(connection):
    try:
        print("создание таблицы")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DROP TABLE IF EXISTS gga_index;
                CREATE Table gga_index(
                id serial PRIMARY KEY,
                Code TEXT,
                Field TEXT,
                TypeData TEXT,
                Well TEXT,
                PathFile TEXT,
                Dates date
     );
                """
            )
            print("[INFO] Table created successfully")
    except psycopg2.Error as e:
        print(e)

# def drop_table(connection):
#     exists = False
#     try:
#         print("удаление таблицы")
#         with connection.cursor() as cursor:
#             cursor.execute("drop table " + "gga_index" )
#     except psycopg2.Error as e:
#         print(e)
#

def field():
    with open("field.html", encoding='utf-8') as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        elements = soup.findAll("td", {'class': 'column-1'})
        return  elements
        # print(elements[3].get_text())
        # for element in elements:
        #     print(element.get_text())


def type():
    with open("type.txt") as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    i = random.randint(0, len(content)-1)
    content = [x.strip() for x in content]
    type = content[i]
    return type
# print(content)
