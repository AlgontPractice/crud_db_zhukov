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
import sys, asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa

if sys.version_info >= (3, 8) and sys.platform.lower().startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
metadata = sa.MetaData()

peop = sa.Table('peop', metadata,
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('first_name', sa.String(255)),
                sa.Column('last_name', sa.String(255)))


async def create_table(engine):
    async with engine.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS peop')
        await conn.execute('''CREATE TABLE peop (
                                  id serial PRIMARY KEY,
                                  first_name varchar(255),
                                  last_name varchar(255))''')


async def add(engine, person: dict) -> str:
    async with engine.acquire() as conn:
        await conn.execute(peop.insert().values(first_name=person['first_name'], last_name=person['last_name']))
        x = 0
        async for row in conn.execute(peop.select().where(peop.c.first_name == person['first_name'])):
            if x < row.id:
                x = row.id
        return x
async def set(engine, person: dict):
    async with engine.acquire() as conn:
        await conn.execute("UPDATE peop SET ...")

async def get_all(engine):
    async with engine.acquire() as conn:
        async for row in conn.execute(peop.select()):
            print(row.id, row.first_name, row.last_name)

async def go():
    async with create_engine(user='postgres',
                             database='people_onion',
                             host='192.168.1.245',
                             password='postgres') as engine:
        await create_table(engine)

        await add(engine, {"first_name": "Andrew", "last_name": "Star"})
        await add(engine, {"first_name": "Andr3ew", "last_name": "Sta4r"})

        # async with engine.acquire() as conn:
        #     await conn.execute(peop.insert().values(first_name='Andrew', last_name='Star'))
        #     await conn.execute(peop.insert().values(first_name='Andrew', last_name='Star'))
        #
        #     async for row in conn.execute(peop.select()):
        #         print(row.id, row.first_name, row.last_name)
        await get_all(engine)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
