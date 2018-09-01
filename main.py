import logging
from gevent.server import StreamServer
from client import handle

def main():
    logging.getLogger().setLevel(logging.INFO)
    port = 8888
    logging.info('start port: %s', port)
    server = StreamServer(('', port), handle)
    server.serve_forever()

if __name__ ==  '__main__':
    main()