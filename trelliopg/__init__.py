import sys

from .sql import *

PY_36 = sys.version_info >= (3, 6)

__all__ = ['DBAdapter', 'get_db_adapter']
