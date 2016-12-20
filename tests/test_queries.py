from trelliopg.sql import get_db_adapter

from . import config

pg = get_db_adapter(config)


async def test_pool():
    pool = await pg.create_pool()
    con = await pool.acquire()
    result = await con.fetch('SELECT * FROM sqrt(16)')
    assert next(result).sqrt == 4.0
    await pool.close()


async def test_pool_connection_transaction_context_manager():
    pool = await pg.create_pool()
    async with pool.transaction() as conn:
        result = await conn.fetch('SELECT * FROM sqrt(16)')

    assert next(result).sqrt == 4.0
