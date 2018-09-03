import logging
from baseclient import handler
from ping_pb2 import Ping, Pong

class PingHandles(object):
    @handler(Ping)
    def onPing(self, req):
        logging.info('onPing: %s', req)
        res = Pong()
        self.writeMessage(res)

    @handler(Pong)
    def onPong(self, req):
        logging.info('onPong: %s', req)
        self.close()