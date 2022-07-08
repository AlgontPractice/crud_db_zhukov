"""
Application factory module
"""
import asyncio
import logging
import logging.config
import socket
from asyncio import AbstractEventLoop
from typing import List

import aiohttp_cors
from aiohttp import web
from aiohttp_jsonrpc import handler
from aiopg.sa import engine, create_engine

from crud_db.db_people import init_db
from crud_db.db_people.init_db import get_all, get, add, set_m, delete, get_list, get_count

logger = logging.getLogger('app')


class JSONRPC_crud(handler.JSONRPCView):
    @property
    def _engine(self):
        return self.request.app['engine']
    async def rpc_add(self, person: dict) -> str:
        async with create_engine(user='postgres',
                                 database='people_onion',
                                 host='192.168.1.245',
                                 password='postgres') as engine:
            return await add(engine, person)

    async def rpc_set(self, person: dict):
        async with create_engine(user='postgres',
                                 database='people_onion',
                                 host='192.168.1.245',
                                 password='postgres') as engine:

            await set_m(engine, person)
        return "res"

    async def rpc_delete(self, person: dict):
        async with create_engine(user='postgres',
                                 database='people_onion',
                                 host='192.168.1.245',
                                 password='postgres') as engine:
            return await delete(engine, person), "s"

    async def rpc_get(self, id: str):
        async with create_engine(user='postgres',
                                 database='people_onion',
                                 host='192.168.1.245',
                                 password='postgres') as engine:
            return await get(engine, id)

    async def rpc_get_list(self, filter: dict, order: List[dict], limit: int, offset: int):
        async with create_engine(user='postgres',
                                 database='people_onion',
                                 host='192.168.1.245',
                                 password='postgres') as engine:
            return await get_list(engine, filter, order, limit, offset)

    async def rpc_get_count(self, filter: dict):
        async with create_engine(user='postgres',
                                 database='people_onion',
                                 host='192.168.1.245',
                                 password='postgres') as engine:
            return await get_count(engine, filter)
    async def rpc_get_all(self) -> List[dict]:
        async with create_engine(user='postgres',
                                 database='people_onion',
                                 host='192.168.1.245',
                                 password='postgres') as engine:
            return await get_all(engine)


async def on_app_start(app):
    """
    Service initialization on application start
    """
    assert 'config' in app

    app['localhost'] = socket.gethostbyname(socket.gethostname())
    # app['engine'] = create_engine(user='postgres',
    #                          database='people_onion',
    #                          host='127.0.0.1',
    #                          password='postgres')
    # async with create_engine(user='postgres',
    #                          database='people_onion',
    #                          host='192.168.1.245',
    #                          password='postgres') as engine:
    #     await get_all(engine)


async def on_app_stop(app):
    """
    Stop tasks on application destroy
    """


# pylint: disable = unused-argument
def create_app(loop: AbstractEventLoop = None, config: dict = None) -> web.Application:
    """
    Creates a web application
    Args:
        loop:
            loop is needed for pytest tests with pytest-aiohttp plugin.
            It is intended to be passed to web.Application, but
            it's deprecated there. So, it remains to avoid errors in tests.
        config:
            dictionary with configuration parameters
    Returns:
        application
    """
    app = web.Application()
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
    })
    app['config'] = config  # в этой переменной хранится конфиг приложения, с помощью неё при необходимости мы можем получить любые параметры из файла /etc/orbbec-mjpeg-streamer/orbbec-mjpeg-streamer.json
    app['frame'] = None  # в эту переменную мы будем складывать кадры, полученные с камеры в scanner.image_grabber
    logging.config.dictConfig(config['logging'])


    app.router.add_route('*', '/jsonrpc/peop_rpc',
                         JSONRPC_crud)  # endpoint, на котором мы можем посмотреть mjpeg-поток. Пример http://192.168.1.245:8080/

    app.on_startup.append(on_app_start)
    app.on_shutdown.append(on_app_stop)
    return app
