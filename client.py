import logging
from baseclient import BaseClient
from ping_handlers import PingHandlers


class Client(PingHandlers, BaseClient):
    _SOCKET_TIMEOUT = 10 * 60

    def __init__(self, socket):
        super(Client, self).__init__(socket)
        socket.settimeout(self._SOCKET_TIMEOUT)


def handle(socket, address):
    try:
        logging.info('>>> addr: %s', address)
        client = Client(socket)
        client.serve()
    except Exception as e:
        logging.warn('exception %s', e, exc_info=True)
    finally:
        logging.info('<<< addr: %s', address)



