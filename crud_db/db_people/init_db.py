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
from typing import List

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

# Добавление строки в таблицу
async def add(engine, person: dict) -> str:
    async with engine.acquire() as conn:
        await conn.execute(peop.insert().values(first_name=person['first_name'], last_name=person['last_name']))
        x = 0
        async for row in conn.execute(peop.select().where(peop.c.first_name == person['first_name'])):
            if x < row.id:
                x = row.id
        return x

# Изменение строки, находим по id
async def set(engine, person: dict):
    async with engine.acquire() as conn:
        await conn.execute(sa.update(peop).values(first_name=person['first_name'], last_name=person['last_name']).where(peop.c.id == person['id']))

# Удаление строки, находим по id
async def delete(engine, person: dict):
    async with engine.acquire() as conn:
        await conn.execute(sa.delete(peop).where(peop.c.id == person['id']))

# Получение строки, поиск по id
async def get(engine, id: str) -> dict:
    async with engine.acquire() as conn:
        async for row in conn.execute(peop.select().where(peop.c.id == id)):
            print(row.id, row.first_name, row.last_name)
            return {"id": row.id, "first_name": row.first_name, "last_name": row.last_name}

# Вывод всех значений таблицы
async def get_all(engine):
    async with engine.acquire() as conn:
        async for row in conn.execute(peop.select()):
            print(row.id, row.first_name, row.last_name)

def custom_key(people):
    return people[0]
# Получение выборки по фильтрам
async def get_list(engine, filter: dict, order: List[dict], limit: int, offset: int) -> List[dict]:
    async with engine.acquire() as conn:
        # Фильтр по фамилии
            result_ln = [] #результат фильтра по фамилии
            if 'value' in filter['last_name']:
                for i in filter['last_name']['value']:
                    async for row in conn.execute(peop.select().where(i == peop.c.last_name)):
                        result_ln.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'like' in filter['last_name']:
                fword = filter['last_name']['like']
                async for row in conn.execute(peop.select()):
                    if fword in row.last_name:
                        result_ln.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'ilike' in filter['last_name']:
                fword = filter['last_name']['ilike']
                async for row in conn.execute(peop.select()):
                    if fword.lower() in row.last_name.lower():
                        result_ln.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
        #Фильтр по имени
            result_fn = [] #Результат фильтра по имени
            if 'value' in filter['first_name']:
                for i in filter['first_name']['value']:
                    async for row in conn.execute(peop.select().where(i == peop.c.first_name)):
                        result_fn.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'like' in filter['first_name']:
                fword = filter['first_name']['like']
                async for row in conn.execute(peop.select()):
                    if fword in row.first_name:
                        result_fn.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'ilike' in filter['first_name']:
                fword = filter['first_name']['ilike']
                async for row in conn.execute(peop.select()):
                    if fword.lower() in row.first_name.lower():
                        result_fn.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
        #Объедиинение результата result_fn и result_ln
            result = []
            i=0
            while i < len(result_fn):
                j = 0
                while j < len(result_ln):
                    if result_fn[i]['id'] == result_ln[j]['id']:
                        result.append(result_fn[int(i)])
                    j += 1
                i += 1
            #order сортировка
            if 'asc' in order[0]['direction']:
                result.sort(key=lambda d: d[order[0]['field']], reverse = False)
            elif 'desc' in order[0]['direction']:
                result.sort(key=lambda d: d[order[0]['field']], reverse=True)

            res = []
            #limit и offset
            f = 0
            while f < len(result):
                if (f >= offset and f < limit):
                    res.append(result[int(f)])
                f+=1

            for i in range(len(res)):
                print(res[i]['id'], res[i]['first_name'], res[i]['last_name'])



async def get_count(engine, filter: dict) -> int:
    async with engine.acquire() as conn:
        # Фильтр по фамилии
            result_ln = [] #результат фильтра по фамилии
            if 'value' in filter['last_name']:
                for i in filter['last_name']['value']:
                    async for row in conn.execute(peop.select().where(i == peop.c.last_name)):
                        print(row.id, row.first_name, row.last_name)
                        result_ln.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'like' in filter['last_name']:
                fword = filter['last_name']['like']
                async for row in conn.execute(peop.select()):
                    if fword in row.last_name:
                        result_ln.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'ilike' in filter['last_name']:
                fword = filter['last_name']['ilike']
                async for row in conn.execute(peop.select()):
                    if fword.lower() in row.last_name.lower():
                        result_ln.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
        #Фильтр по имени
            result_fn = [] #Результат фильтра по имени
            if 'value' in filter['first_name']:
                for i in filter['first_name']['value']:
                    async for row in conn.execute(peop.select().where(i == peop.c.first_name)):
                        result_fn.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'like' in filter['first_name']:
                fword = filter['first_name']['like']
                async for row in conn.execute(peop.select()):
                    if fword in row.first_name:
                        result_fn.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
            elif 'ilike' in filter['first_name']:
                fword = filter['first_name']['ilike']
                async for row in conn.execute(peop.select()):
                    if fword.lower() in row.first_name.lower():
                        result_fn.append({"id": row.id, "first_name": row.first_name, "last_name": row.last_name})
        #Объедиинение результата result_fn и result_ln
            result = []
            i=0
            while i < len(result_fn):
                j = 0
                while j < len(result_ln):
                    if result_fn[i]['id'] == result_ln[j]['id']:
                        result.append(result_fn[int(i)])
                    j += 1
                i += 1
            print(len(result))
            return len(result)


async def go():
    async with create_engine(user='postgres',
                             database='people_onion',
                             host='192.168.1.245',
                             password='postgres') as engine:
        await create_table(engine)

        await add(engine, {"first_name": "Andrew", "last_name": "Star"})
        await add(engine, {"first_name": "Andr3ew", "last_name": "Sta4r"})

        await add(engine, {"first_name": "A", "last_name": "a1"})
        await add(engine, {"first_name": "b3bb", "last_name": "2a"})
        await add(engine, {"first_name": "cecece", "last_name": "3"})


        # await set(engine, {"id": "2", "first_name": "rer", "last_name": "fef"})
        await get_list(engine, {"first_name": {"like": ""}, "last_name": {"ilike": "A"}}, [{"field": "id", "direction": "asc"}], 4, 0)
        # await get(engine, "2")
        # await get_count(engine, {"first_name": {"like": ""}, "last_name": {"ilike": "a"}})
        # await delete(engine, {"id": "1", "first_name": "rer", "last_name": "fef"})

        # async with engine.acquire() as conn:
        #     await conn.execute(peop.insert().values(first_name='Andrew', last_name='Star'))
        #     await conn.execute(peop.insert().values(first_name='Andrew', last_name='Star'))
        #
        #     async for row in conn.execute(peop.select()):
        #         print(row.id, row.first_name, row.last_name)
        # await get_all(engine)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
