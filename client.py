import logging
import traceback
from baseclient import BaseClient, handler
from ping_pb2 import Ping

class Client(BaseClient):
    _SOCKET_TIMEOUT = 10 * 60

    def __init__(self, socket):
        super(Client, self).__init__(socket)
        socket.settimeout(self._SOCKET_TIMEOUT)

    @handler(Ping)
    def onPing(self, req):
        logging.info('onPing: %s', req)
        self.writeMessage(req)
        self.close()


def handle(socket, address):
    try:
        logging.info('>>> addr: %s', address)
        client = Client(socket)
        client.serve()
    except Exception as e:
        logging.warn('exception %s, %s', e, traceback.format_exc())
    finally:
        logging.info('<<< addr: %s', address)



