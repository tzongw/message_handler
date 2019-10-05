import logging
from baseclient import message_handler, BaseClient
from ping_pb2 import Ping, Pong


class PingHandlers(BaseClient):
    @message_handler(Ping)
    def on_ping(self, req):
        logging.info('on ping: %s', req)
        res = Pong()
        self.write_message(res)

    @message_handler(Pong)
    def on_pong(self, req):
        logging.info('on pong: %s', req)
        self.close()
