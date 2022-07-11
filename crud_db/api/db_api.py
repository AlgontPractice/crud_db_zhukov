import logging
from aiohttp_jsonrpc import handler
from typing import List

from crud_db.db_people.init_db import get_all, get, add, set_m, delete, get_list, get_count


class JSONRPC_crud(handler.JSONRPCView):
    @property
    def _engine(self):
        return self.request.app['engine']

    async def rpc_add(self, person: dict) -> str:
        return await add(self._engine, person)

    async def rpc_set(self, person: dict):
        await set_m(self._engine, person)

    async def rpc_delete(self, person: dict):
        return await delete(self._engine, person), "s"

    async def rpc_get(self, id: str):
        return await get(self._engine, id)

    async def rpc_get_list(self, filter: dict, order: List[dict], limit: int, offset: int):
        return await get_list(self._engine, filter, order, limit, offset)

    async def rpc_get_count(self, filter: dict):
        return await get_count(self._engine, filter)

    async def rpc_get_all(self) -> List[dict]:
        return await get_all(self._engine)


logger = logging.getLogger('crud_db')
