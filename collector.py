from time import sleep
from threading import Thread
import selectors
import socket
import types
import json

from central import NUM_EDGE_NODES
from utils import recv_timeout

CENTRAL_HOST = '127.0.0.1'
CENTRAL_PORT = 65432

SELF_HOST = '127.0.0.1'
SELF_PORT = 65434

HEARTBEAT_PERIOD = 10

def check_failure():
    while True:
        sleep(HEARTBEAT_PERIOD)
        for i in range(NUM_EDGE_NODES):
            if NODE_FAILURE[i] == 0:
                print('Failure of edge node', i+1, 'detected')
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((CENTRAL_HOST, CENTRAL_PORT))
                    dict_id = {"failure_id": i+1}
                    json_id = json.dumps(dict_id)
                    sock.sendall(bytes(json_id, encoding="utf-8"))
        for i in range(NUM_EDGE_NODES):
            NODE_FAILURE[i] = 0

def accept_wrapper(sock, sel):
    conn, addr = sock.accept()  # Should be ready to read
    print('Edge node 1 accepted connection from', addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_connection(key, mask, sel):
    global NODE_FAILURE

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
            if "id" in json_data.keys():
                NODE_FAILURE[json_data['id']-1] = 1

            data.outb = bytearray()

def connect_to_nodes():
    sel = selectors.DefaultSelector()
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.bind((SELF_HOST, SELF_PORT))
    lsock.listen()
    print('Collector node listening on', (SELF_HOST, SELF_PORT))
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ, data=None)
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj, sel)
            else:
                service_connection(key, mask, sel)

def main():
    Thread(target = connect_to_nodes).start()
    Thread(target = check_failure).start()

if __name__ == "__main__":
    sleep(HEARTBEAT_PERIOD/2)
    global NODE_FAILURE
    NODE_FAILURE = [0 for x in range(NUM_EDGE_NODES)]
    main()
