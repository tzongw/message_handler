import contextlib
from gevent.queue import Queue


class AbstractDatabaseConnectionPool(object):
    def __init__(self, maxsize=32, timeout=5):
        self.maxsize = maxsize
        self.timeout = timeout
        self.pool = Queue()
        self.size = 0

    def create_connection(self):
        raise NotImplementedError()

    def get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get(self.timeout)

        self.size += 1
        try:
            new_item = self.create_connection()
        except:
            self.size -= 1
            raise
        return new_item

    def put(self, item):
        self.pool.put(item)

    def closeall(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
            except:
                pass

    @contextlib.contextmanager
    def connection(self):
        conn = self.get()
        try:
            yield conn
        except:
            conn.close()
            self.size -= 1
            raise
        else:
            self.put(conn)

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

