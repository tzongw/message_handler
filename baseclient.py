from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from google.protobuf.message import Message


class BaseClient(object):
    _message_handlers = {}
    _LENGTH_LIMIT = 10 * 1024 * 1024

    def __init__(self, socket):
        tsocket = TSocket.TSocket()
        tsocket.setHandle(socket)
        trans = TTransport.TBufferedTransport(tsocket)
        self.proto = TBinaryProtocol.TBinaryProtocol(trans, string_length_limit=self._LENGTH_LIMIT)

    def __del__(self):
        self.close()

    def close(self):
        if self.proto:
            self.proto.trans.close()
            self.proto = None

    def write_message(self, message):
        self.proto.writeI32(message.uri)
        buf = message.SerializeToString()
        self.proto.writeBinary(buf)

    def flush(self):
        if self.proto:
            self.proto.trans.flush()

    def serve(self):
        while self.proto:
            uri = self.proto.readI32()
            buf = self.proto.readBinary()
            handler = self._message_handlers.get(uri)
            if handler:
                message_cls, func = handler
                message = message_cls()
                message.ParseFromString(buf)
                func(self, message)
                self.flush()

    @classmethod
    def message_handler(cls, message_cls):
        def wrapper(f):
            assert issubclass(message_cls, Message), 'message type wrong'
            uri = message_cls().uri
            handlers = cls._message_handlers
            assert uri not in handlers, 'duplicate uri'
            handlers[uri] = (message_cls, f)
            return f
        return wrapper


def message_handler(message_cls):
    return BaseClient.message_handler(message_cls)


