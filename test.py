import logging
import socket
from client import Client
from ping_pb2 import Ping

def main():
    logging.getLogger().setLevel(logging.INFO)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 8888))
    c = Client(s)
    req = Ping()
    c.write_message(req)
    c.flush()
    c.serve()

if __name__ ==  '__main__':
    main()
