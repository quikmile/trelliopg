import functools
import itertools
import os
import sys

from asyncpg.connection import Connection
from asyncpg.pool import Pool, create_pool

PY_36 = sys.version_info >= (3, 6)

try:
    import ujson as json
except ImportError:
    import json


class AtomicExceptionHandler:
    def __init__(self, exp_coro, rt_dict):
        self.exp_coro = exp_coro
        self.rt_dict = rt_dict

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            return_dict = await self.exp_coro(exc_type, exc_val, exc_tb)
            for key, val in return_dict.items():
                self.rt_dict[key] = val
        return True


def get_db_settings(config_file=None):
    if not config_file:
        config_file = os.environ.get('CONFIG_FILE')

    if not config_file:
        config_file = './config.json'

    with open(config_file) as f:
        settings = json.load(f)

        if 'DATABASE_SETTINGS' not in settings.keys():
            raise KeyError('"DATABASE_SETTINGS" key not found in config file')

    return settings['DATABASE_SETTINGS']


def get_db_adapter(settings=None, config_file=None):
    if not settings:
        settings = get_db_settings(config_file)

    db_adapter = DBAdapter(**settings)
    return db_adapter


def async_atomic(on_exception=None, raise_exception=True, **kwargs):
    '''
    first argument will be a conn object
    :param func:
    :return:
    '''
    if not raise_exception and not on_exception:
        async def default_on_exception(exc):
            resp_dict = {}
            resp_dict['status'] = type(exc)
            resp_dict['message'] = str(exc)
            return resp_dict

        on_exception = default_on_exception
    elif raise_exception and not on_exception:
        async def raise_exception(exp_args):
            raise exp_args

        on_exception = raise_exception

    _db_adapter = get_db_adapter()

    def decorator(func):
        @functools.wraps(func)
        async def wrapped(self, *args, **kwargs):
            conn = None
            for i in itertools.chain(args, kwargs.values()):
                if type(i) is Connection:
                    conn = i
                    break
            if not conn:
                pool = await _db_adapter.get_pool()
                async with pool.acquire() as conn:
                    try:
                        async with conn.transaction():
                            kwargs['conn'] = conn
                            return await func(self, *args, **kwargs)
                    except Exception as e:
                        return await on_exception(e)
            else:
                try:
                    async with conn.transaction():
                        kwargs['conn'] = conn
                        return await func(self, *args, **kwargs)
                except Exception as e:
                    return await on_exception(e)

        return wrapped

    return decorator


def async_atomic_func(on_exception=None, raise_exception=True, **kwargs):
    '''
    first argument will be a conn object
    :param func:
    :return:
    '''
    if not raise_exception and not on_exception:
        async def default_on_exception(exc):
            resp_dict = {}
            resp_dict['status'] = type(exc)
            resp_dict['message'] = str(exc)
            return resp_dict

        on_exception = default_on_exception
    elif raise_exception and not on_exception:
        async def raise_exception(exp_args):
            raise exp_args

        on_exception = raise_exception

    _db_adapter = get_db_adapter()

    def decorator(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            conn = None
            for i in itertools.chain(args, kwargs.values()):
                if type(i) is Connection:
                    conn = i
                    break
            if not conn:
                pool = await _db_adapter.get_pool()
                try:
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            kwargs['conn'] = conn
                            return await func(*args, **kwargs)
                except Exception as e:
                    return await on_exception(e)
            else:
                try:
                    async with conn.transaction():
                        kwargs['conn'] = conn
                        return await func(*args, **kwargs)
                except Exception as e:
                    return await on_exception(e)

        return wrapped

    return decorator


class Borg:
    __shared_state = dict()

    def __init__(self):
        self.__dict__ = self.__shared_state


class DBAdapter(Borg):
    INSERT = """INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING *;"""
    SELECT = """SELECT {columns} FROM {table}"""
    UPDATE = """UPDATE {table} SET {values} {where} RETURNING *"""
    DELETE = """DELETE FROM {table} {where}"""
    WHERE = """ {key} = '{value}'"""

    def __init__(self, database: str = '', user: str = '', password: str = '', host: str = 'localhost',
                 port: int = 5432, min_size=5, max_size=10, max_queries=50000, setup=None, loop=None, **kwargs):

        super(DBAdapter, self).__init__()
        self._dsn = dict()
        self._dsn['database'] = database
        self._dsn['user'] = user
        self._dsn['password'] = password
        self._dsn['host'] = host
        self._dsn['port'] = port

        self._params = dict()
        self._params['dsn'] = 'postgres://{user}:{password}@{host}:{port}/{database}'.format(**self._dsn)
        self._params['min_size'] = min_size
        self._params['max_size'] = max_size
        self._params['max_queries'] = max_queries
        self._params['setup'] = setup
        self._params['loop'] = loop
        self._params.update(kwargs)

        self.pool = None

        if PY_36:
            self._compat()

    async def get_pool(self) -> Pool:
        if not self.pool:
            self.pool = await create_pool(**self._params)
        return self.pool

    async def insert(self, con: Connection = None, table: str = '', value_dict: dict = None):

        columns = ",".join(value_dict.keys())
        placeholder = ",".join(['${}'.format(i) for i in range(1, len(value_dict) + 1)])

        query = self.INSERT.format(table=table, columns=columns, values=placeholder)

        if not con:
            pool = await self.get_pool()
            async with pool.acquire() as con:
                async with con.transaction():
                    result = await con.fetchrow(query, *value_dict.values())
        else:
            async with con.transaction():
                result = await con.fetchrow(query, *value_dict.values())

        return result

    async def update(self, con: Connection = None, table: str = '', where_dict: dict = None,
                     **update_params: dict) -> list:

        values_list = ["{}='{}'".format(k, v) for k, v in update_params.items() if v is not None]
        values_list.extend(["{}=null".format(k, v) for k, v in update_params.items() if v is None])
        values = ','.join(values_list)

        where = ''
        if where_dict is not None:
            where = ' where '
            where += ' and '.join([self.WHERE.format(key=k, value=v) for k, v in where_dict.items()])
        query = self.UPDATE.format(table=table, values=values, where=where)

        if not con:
            pool = await self.get_pool()
            async with pool.acquire() as con:
                async with con.transaction():
                    results = await con.fetch(query)
        else:
            async with con.transaction():
                results = await con.fetch(query)

        return results

    async def delete(self, con: Connection = None, table: str = '', where_dict: dict = None):
        where = ' where '
        where += ' and '.join([self.WHERE.format(key=k, value=v) for k, v in where_dict.items()])
        query = self.DELETE.format(table=table, where=where)

        if not con:
            pool = await self.get_pool()
            async with pool.acquire() as con:
                async with con.transaction():
                    await con.execute(query)
        else:
            async with con.transaction():
                await con.execute(query)

    async def execute(self, con: Connection = None, query: str = ''):
        if not con:
            pool = await self.get_pool()
            async with pool.acquire() as con:
                async with con.transaction():
                    await con.execute(query)
        else:
            async with con.transaction():
                await con.execute(query)

    async def select(self, table: str, offset=0, limit='ALL', order_by='created desc', columns='*') -> list:
        if isinstance(columns, list):
            columns = ','.join(columns)
        query = self.SELECT.format(columns=columns, table=table)
        query += ' order by $1 offset $2 limit $3'

        pool = await self.get_pool()
        async with pool.acquire() as con:
            stmt = await con.prepare(query)
            results = await stmt.fetch(order_by, offset, limit)

        return results

    async def where(self, table: str, offset=None, limit=None, order_by=None, columns='*', **where_dict: dict) -> list:
        if isinstance(columns, list):
            columns = ','.join(columns)
        query = self.SELECT.format(columns=columns, table=table)
        query += self._where_query(where_dict, offset, limit, order_by)

        pool = await self.get_pool()
        async with pool.acquire() as con:
            results = await con.fetch(query)

        return results

    @staticmethod
    def _where_query(where_dict, offset=None, limit=None, order_by=None):
        query = ''

        if where_dict:
            query += ' where '

            where_query = None
            search_query = None
            if where_dict.get('search') and isinstance(where_dict.get('search'), dict):
                _search = where_dict.pop('search')
                search_columns = _search.get('columns', [])
                search_term = _search.get('term', '')
                search_list = []

                for key in search_columns:
                    column = key
                    operator = 'ilike'
                    value = "'%{}%'".format(search_term)

                    split_key = key.split('__')
                    if len(split_key) > 1:
                        column = split_key[0]
                        spliter = split_key[1]

                        if spliter == 'contains':
                            operator = 'like'
                            value = "'%{}%'".format(search_term)

                        if spliter == 'icontains':
                            operator = 'ilike'
                            value = "'%{}%'".format(search_term)

                        if spliter == 'startswith':
                            operator = 'like'
                            value = "'{}%'".format(search_term)

                        if spliter == 'istartswith':
                            operator = 'ilike'
                            value = "'{}%'".format(search_term)

                        if spliter == 'endswith':
                            operator = 'like'
                            value = "'%{}'".format(search_term)

                        if spliter == 'iendswith':
                            operator = 'ilike'
                            value = "'%{}'".format(search_term)

                    placeholder = "{} {} {}".format(column, operator, value)
                    search_list.append(placeholder)

                search_query = ' or '.join(search_list)

            where_list = []
            for key in where_dict.keys():
                operator = '='
                value = "'{}'".format(where_dict[key])

                split_key = key.split('__')
                if len(split_key) > 1:
                    column = split_key[0]
                    spliter = split_key[1]

                    if spliter == 'in':
                        operator = 'in'
                        value = "({})".format(','.join(["'{}'".format(v) for v in where_dict[key]]))

                    if spliter == 'not_in':
                        operator = 'not in'
                        value = "({})".format(','.join(["'{}'".format(v) for v in where_dict[key]]))

                    if spliter == 'lt':
                        operator = '<'

                    if spliter == 'lte':
                        operator = '<='

                    if spliter == 'gt':
                        operator = '>'

                    if spliter == 'gte':
                        operator = '>='

                    if spliter == 'contains':
                        operator = 'like'
                        value = "'%{}%'".format(search_term)

                    if spliter == 'icontains':
                        operator = 'ilike'
                        value = "'%{}%'".format(search_term)

                    if spliter == 'startswith':
                        operator = 'like'
                        value = "'{}%'".format(search_term)

                    if spliter == 'istartswith':
                        operator = 'ilike'
                        value = "'{}%'".format(search_term)

                    if spliter == 'endswith':
                        operator = 'like'
                        value = "'%{}'".format(search_term)

                    if spliter == 'iendswith':
                        operator = 'ilike'
                        value = "'%{}'".format(search_term)

                else:
                    column = key

                placeholder = "{} {} {}".format(column, operator, value)
                where_list.append(placeholder)
            if where_list:
                where_query = ' and '.join(where_list)

            if search_query and where_query:
                query += ' (' + search_query + ') and (' + where_query + ') '
            elif search_query and where_query is None:
                query += search_query
            elif where_query and search_query is None:
                query += where_query

        if order_by:
            query += ' order by {}'.format(order_by)
        if offset:
            query += ' offset {}'.format(offset)
        if limit:
            query += ' limit {}'.format(limit)

        return query

    def _compat(self):
        ld = {}
        s = '''async def iterate(self, query: str):
            pool = await self.get_pool()
            async with pool.acquire() as con:
                async with con.transaction():
                    async for record in con.cursor(query):
                        yield record'''

        exec(s, None, ld)
        for name, value in ld.items():
            setattr(DBAdapter, name, value)
