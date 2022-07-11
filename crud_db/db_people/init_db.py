import asyncio
import sys
from typing import List

import sqlalchemy as sa
from aiopg.sa import create_engine

if sys.version_info >= (3, 8) and sys.platform.lower().startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
metadata = sa.MetaData()

peop = sa.Table('peop', metadata,
                sa.Column('user_id', sa.Integer, primary_key=True),
                sa.Column('first_name', sa.String(255)),
                sa.Column('last_name', sa.String(255)))


async def create_table(engine):
    async with engine.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS peop')
        await conn.execute('''CREATE TABLE peop (
                                  user_id serial PRIMARY KEY,
                                  first_name varchar(255),
                                  last_name varchar(255))''')


# Добавление строки в таблицу
async def add(engine, person: dict) -> str:
    async with engine.acquire() as conn:
        await conn.execute(peop.insert().values(first_name=person['first_name'], last_name=person['last_name']))
        x = 0
        async for row in conn.execute(peop.select().where(peop.c.first_name == person['first_name'])):
            if x < row.user_id:
                x = row.user_id
        return x


# Изменение строки, находим по user_id
async def set_m(engine, person: dict):
    async with engine.acquire() as conn:
        await conn.execute(sa.update(peop).values(first_name=person['first_name'], last_name=person['last_name']).where(
            peop.c.user_id == person['user_id']))


# Удаление строки, находим по user_id
async def delete(engine, person: dict):
    async with engine.acquire() as conn:
        await conn.execute(sa.delete(peop).where(peop.c.user_id == person['user_id']))


# Получение строки, поиск по user_id
async def get(engine, user_id: str) -> dict:
    async with engine.acquire() as conn:
        async for row in conn.execute(peop.select().where(peop.c.user_id == user_id)):
            print(row.user_id, row.first_name, row.last_name)
            return {"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name}


# Вывод всех значений таблицы
async def get_all(engine):
    async with engine.acquire() as conn:
        res = []
        async for row in conn.execute(peop.select()):
            res.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        return res


# Получение выборки по фильтрам
async def get_list(engine, filter: dict, order: List[dict], limit: int, offset: int) -> List[dict]:
    async with engine.acquire() as conn:
        # Фильтр по фамилии
        result_ln = []  # результат фильтра по фамилии
        if 'value' in filter['last_name']:
            for i in filter['last_name']['value']:
                async for row in conn.execute(peop.select().where(i == peop.c.last_name)):
                    result_ln.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'like' in filter['last_name']:
            fword = filter['last_name']['like']
            async for row in conn.execute(peop.select()):
                if fword in row.last_name:
                    result_ln.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'ilike' in filter['last_name']:
            fword = filter['last_name']['ilike']
            async for row in conn.execute(peop.select()):
                if fword.lower() in row.last_name.lower():
                    result_ln.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        # Фильтр по имени
        result_fn = []  # Результат фильтра по имени
        if 'value' in filter['first_name']:
            for i in filter['first_name']['value']:
                async for row in conn.execute(peop.select().where(i == peop.c.first_name)):
                    result_fn.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'like' in filter['first_name']:
            fword = filter['first_name']['like']
            async for row in conn.execute(peop.select()):
                if fword in row.first_name:
                    result_fn.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'ilike' in filter['first_name']:
            fword = filter['first_name']['ilike']
            async for row in conn.execute(peop.select()):
                if fword.lower() in row.first_name.lower():
                    result_fn.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        # Объедиинение результата result_fn и result_ln
        result = []
        i = 0
        while i < len(result_fn):
            j = 0
            while j < len(result_ln):
                if result_fn[i]['user_id'] == result_ln[j]['user_id']:
                    result.append(result_fn[int(i)])
                j += 1
            i += 1
        # order сортировка
        if 'asc' in order[0]['direction']:
            result.sort(key=lambda d: d[order[0]['field']], reverse=False)
        elif 'desc' in order[0]['direction']:
            result.sort(key=lambda d: d[order[0]['field']], reverse=True)

        res = []
        # limit и offset
        f = 0
        while f < len(result):
            if (f >= offset):
                res.append(result[int(f)])
            f += 1
        result = []
        f = 0
        while f < len(res):
            if (f < limit):
                result.append(res[int(f)])
            f += 1
        for i in range(len(result)):
            print(result[i]['user_id'], result[i]['first_name'], result[i]['last_name'])
        return result


async def get_count(engine, filter: dict) -> int:
    async with engine.acquire() as conn:
        # Фильтр по фамилии
        result_ln = []  # результат фильтра по фамилии
        if 'value' in filter['last_name']:
            for i in filter['last_name']['value']:
                async for row in conn.execute(peop.select().where(i == peop.c.last_name)):
                    print(row.user_id, row.first_name, row.last_name)
                    result_ln.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'like' in filter['last_name']:
            fword = filter['last_name']['like']
            async for row in conn.execute(peop.select()):
                if fword in row.last_name:
                    result_ln.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'ilike' in filter['last_name']:
            fword = filter['last_name']['ilike']
            async for row in conn.execute(peop.select()):
                if fword.lower() in row.last_name.lower():
                    result_ln.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        # Фильтр по имени
        result_fn = []  # Результат фильтра по имени
        if 'value' in filter['first_name']:
            for i in filter['first_name']['value']:
                async for row in conn.execute(peop.select().where(i == peop.c.first_name)):
                    result_fn.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'like' in filter['first_name']:
            fword = filter['first_name']['like']
            async for row in conn.execute(peop.select()):
                if fword in row.first_name:
                    result_fn.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        elif 'ilike' in filter['first_name']:
            fword = filter['first_name']['ilike']
            async for row in conn.execute(peop.select()):
                if fword.lower() in row.first_name.lower():
                    result_fn.append({"user_id": row.user_id, "first_name": row.first_name, "last_name": row.last_name})
        # Объедиинение результата result_fn и result_ln
        result = []
        i = 0
        while i < len(result_fn):
            j = 0
            while j < len(result_ln):
                if result_fn[i]['user_id'] == result_ln[j]['user_id']:
                    result.append(result_fn[int(i)])
                j += 1
            i += 1
        print(len(result))
        return len(result)