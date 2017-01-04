import os

import pytest
from asyncpg.exceptions import DuplicateTableError, UndefinedTableError

from trelliopg import get_db_adapter, PY_36, async_atomic

os.environ['CONFIG_FILE'] = './tests/test_config.json'


@pytest.fixture(scope='function')
def pg(event_loop):
    _pg = get_db_adapter()
    return _pg


@pytest.mark.asyncio
async def test_pool(pg):
    pool = await pg.get_pool()
    con = await pool.acquire()
    result = await con.fetch('SELECT * FROM sqrt(16)')
    assert result[0]['sqrt'] == 4.0
    await pool.close()


@pytest.mark.asyncio
async def test_pool_connection_transaction_context_manager(pg):
    pool = await pg.get_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            result = await con.fetchrow('SELECT * FROM sqrt(16)')
    assert result['sqrt'] == 4.0


@pytest.mark.asyncio
async def test_shared_pool():
    pg1 = get_db_adapter()
    p1 = await pg1.get_pool()
    c1 = await p1.acquire()

    pg2 = get_db_adapter()
    p2 = await pg2.get_pool()
    c2 = await p2.acquire()

    assert id(pg1.pool) == id(pg2.pool)
    # assert c1 == c2


if PY_36:
    @pytest.mark.asyncio
    async def test_iterate_query(pg):
        async for record in pg.iterate('SELECT * FROM sqrt(16)'):
            assert record['sqrt'] == 4.0


@pytest.mark.asyncio
async def test_async_atomic_decorators():
    async def create_table(conn, query):
        if 'CREATE' in query:
            try:
                await conn.execute(query)
                return True
            except DuplicateTableError:
                return False
        else:
            return False

    async def drop_table(conn, query):
        if 'DROP' in query:
            try:
                await conn.execute(query)
                return True
            except UndefinedTableError:
                return False
        else:
            return False

    class foo:

        @async_atomic
        async def setup(self, conn, *args, **kwargs):
            assert await drop_table(conn, 'DROP TABLE IF EXISTS test_async_atomic;')
            assert await create_table(conn, 'CREATE TABLE IF NOT EXISTS test_async_atomic (id serial primary key);')

        @async_atomic
        async def inner_query(self, conn, *args, **kwargs):
            assert await conn.fetch('INSERT INTO test_async_atomic DEFAULT VALUES RETURNING *;')

        @async_atomic
        async def outer_method_1(self, conn, *args, **kwargs):
            await self.inner_query(conn)
            assert await conn.fetch('INSERT INTO test_async_atomic DEFAULT VALUES RETURNING *;')
            raise Exception

        @async_atomic
        async def outer_method_2(self, conn, *args, **kwargs):
            await self.inner_query(conn)
            assert await conn.fetch('INSERT INTO test_async_atomic DEFAULT VALUES RETURNING *;')

        @async_atomic
        async def test_atomic_decorator(self, conn):
            await self.setup()

            try:
                await self.outer_method_1()
            except:
                rt = await conn.fetch('SELECT COUNT(*) FROM test_async_atomic;')
                print(rt)
                rt = rt[0]
                assert tuple(rt)[0] == 0
            await self.outer_method_2()
            rt = await conn.fetch('SELECT COUNT(*) FROM test_async_atomic;')
            print(rt)
            rt = rt[0]
            assert tuple(rt)[0] == 2

    await foo().test_atomic_decorator()
