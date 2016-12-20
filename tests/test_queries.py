import pytest

from trelliopg import get_db_adapter
from . import config


@pytest.fixture(scope='function')
def pg(event_loop):
    _pg = get_db_adapter(config)
    return _pg


@pytest.mark.asyncio
async def test_pool(pg):
    pool = await pg.create_pool()
    con = await pool.acquire()
    result = await con.fetch('SELECT * FROM sqrt(16)')
    assert result[0]['sqrt'] == 4.0
    await pool.close()


@pytest.mark.asyncio
async def test_pool_connection_transaction_context_manager(pg):
    pool = await pg.create_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            result = await con.fetchrow('SELECT * FROM sqrt(16)')
    assert result['sqrt'] == 4.0
