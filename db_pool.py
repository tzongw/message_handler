import contextlib
from gevent.queue import Queue
from weakref import WeakKeyDictionary
from time import time


class AbstractDatabaseConnectionPool(object):
    def __init__(self, maxsize=64, timeout=5, check_time=3600, idle=None):
        self.maxsize = maxsize
        self.timeout = timeout
        self.pool = Queue()
        self.size = 0
        self.check_time = check_time
        self.check_dict = WeakKeyDictionary()
        if idle is None:
            idle = maxsize
        self.idle = idle

    def create_connection(self):
        raise NotImplementedError()

    def get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get(self.timeout)

        self.size += 1
        try:
            new_item = self.create_connection()
        except Exception:
            self.size -= 1
            raise
        return new_item

    def put(self, item):
        self.pool.put(item)

    def close_all(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass

    @contextlib.contextmanager
    def connection(self):
        conn = self.get()

        def close_conn():
            conn.close()
            self.size -= 1
        try:
            current = time()
            last_check = self.check_dict.get(conn, current)
            if current - last_check > self.check_time:
                conn.ping()
            yield conn
        except Exception:
            close_conn()
            raise
        else:
            if self.pool.qsize() < self.idle:
                self.put(conn)
                self.check_dict[conn] = current
            else:
                close_conn()

    @contextlib.contextmanager
    def cursor(self, transaction=False):
        with self.connection() as conn:
            if transaction and conn.get_autocommit():
                conn.begin()
            if transaction or not conn.get_autocommit():
                with conn as cursor:
                    with cursor:
                        yield cursor
            else:
                cursor = conn.cursor()
                with cursor:
                    yield cursor

    def execute(self, query, args=None):
        with self.cursor() as cursor:
            cursor.execute(query, args)
            return cursor

