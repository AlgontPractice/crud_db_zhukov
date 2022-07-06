import asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa


metadata = sa.MetaData()



async def go():
    async with create_engine(user='postgres',
                             database='people_onion',
                             host='192.168.1.245',
                             password='postgres') as engine:
        async with engine.acquire() as conn:
            print(878787)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())