from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from google.protobuf import message

class BaseClient(object):
    _messageHandlers = {}
    _LENGTH_LIMIT = 10 * 1024 * 1024

    def __init__(self, socket):
        tsocket = TSocket.TSocket()
        tsocket.setHandle(socket)
        trans = TTransport.TBufferedTransport(tsocket)
        self.proto = TBinaryProtocol.TBinaryProtocol(trans, string_length_limit=self._LENGTH_LIMIT)

    def close(self):
        if self.proto:
            self.proto.trans.close()
            self.proto = None

    def writeMessage(self, message, flush=True):
        self.proto.writeI32(message.uri)
        buf = message.SerializeToString()
        self.proto.writeBinary(buf)
        if flush:
            self.proto.trans.flush()

    def serve(self):
        while self.proto:
            uri = self.proto.readI32()
            buf = self.proto.readBinary()
            handler = self._messageHandlers.get(uri)
            if handler:
                Message, func = handler
                message = Message()
                message.ParseFromString(buf)
                func(self, message)


def handler(Message):
    def wrapper(f):
        assert issubclass(Message, message.Message), 'Message type'
        uri = Message().uri
        handles = BaseClient._messageHandlers
        assert uri not in handles, 'Duplicate uri'
        handles[uri] = (Message, f)
        return f
    return wrapper



