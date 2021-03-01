import socket
import selectors
import types
import random
import sys
from utils import recv_timeout
import json

SELF_HOST='127.0.0.1'
SELF_PORT=65432

NUM_DATA_BLOCKS = 1000
NUM_EDGE_NODES = 2

T = 2
K = 5

def accept_wrapper(sock, sel):
    conn, addr = sock.accept()  # Should be ready to read
    print('accepted connection from', addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_connection(key, mask, sel):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = recv_timeout(sock)  # Should be ready to read
        if recv_data:
            data.outb += recv_data
        else:
            print('closing connection to', data.addr)
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            str_data = data.outb.decode("utf-8")
            json_data = json.loads(str_data)
            print(json_data)
            # print('echoing', repr(data.outb), 'to', data.addr)
            # sent = sock.send(data.outb)  # Should be ready to write
            data.outb = bytearray()

def main():
    sel = selectors.DefaultSelector()
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.bind((SELF_HOST, SELF_PORT))
    lsock.listen()
    print('Central node listening on', (SELF_HOST, SELF_PORT))
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ, data=None)
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj, sel)
            else:
                service_connection(key, mask, sel)

if __name__ == "__main__":
    global BIN_ENCODING
    BIN_ENCODING = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(NUM_EDGE_NODES)]
    global DATA
    DATA = [random.randint(0,sys.maxsize) for x in range(NUM_DATA_BLOCKS)]
    main()
