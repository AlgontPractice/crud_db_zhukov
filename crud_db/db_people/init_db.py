#
# import psycopg2
# from psycopg2 import Error
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
#
# from lib2to3.pytree import Base
# from pickle import STRING
# from sqlalchemy import Column, Integer, TIMESTAMP, create_engine
#
#
# def create_db():
#     try:
#         # Подключение к существующей базе данных
#         connection = psycopg2.connect(user="postgres",
#                                       # пароль, который указали при установке PostgreSQL
#                                       password="postgres",
#                                       host="192.168.1.245",
#                                       port="5432")
#         connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#         # Курсор для выполнения операций с базой данных
#         cursor = connection.cursor()
#         cursor.execute('DROP TABLE IF EXISTS peop_onion')
#         sql_create_table = '''CREATE TABLE peop_onion (
#                                           id serial PRIMARY KEY,
#                                           first_name varchar(255),
#                                           last_name varchar(255))'''
#         cursor.execute(sql_create_table)
#
#         cursor.execute("INSERT INTO peop_onion VALUES (1, 'Ivan', "+"'Ivanov');")
#         cursor.execute("SELECT * FROM peop_onion")
#         print(cursor.fetchone())
#     except (Exception, Error) as error:
#         print("Ошибка при работе с PostgreSQL", error)
#
#     finally:
#         cursor.close()
#         connection.close()
#         print("Соединение с PostgreSQL закрыто")
#
#
# create_db()
import asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa


metadata = sa.MetaData()




# async def create_table(engine):
#     async with engine.acquire() as conn:
#         await conn.execute('DROP TABLE IF EXISTS tbl')
#         await conn.execute('''CREATE TABLE tbl (
#                                   id serial PRIMARY KEY,
#                                   val varchar(255))''')


async def go():
    print(000000000000)
    async with create_engine(user='postgres',
                             database='people_onion',
                             host='192.168.1.245',
                             password='postgres') as engine:
        print(1111)
        # await create_table(engine)
        async with engine.acquire() as conn:
            print(898989)
            # await conn.execute(tbl.insert().values(val='abc'))
            #
            # async for row in conn.execute(tbl.select()):
            #     print(row.id, row.val)


loop = asyncio.new_event_loop()
loop.run_until_complete(go())