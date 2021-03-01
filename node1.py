import socket
from multiprocessing import Process
import sys
import selectors
import types
import struct
from central import NUM_DATA_BLOCKS, T
import json
import time
from utils import recv_timeout
from time import sleep
import threading
from threading import Thread

CENTRAL_HOST='127.0.0.1'
CENTRAL_PORT=65432

SELF_HOST='127.0.0.1'
SELF_PORT=65433

def find_data_blocks(data_blocks):
    return_data = []
    for id in data_blocks:
        if DATA[id] != -1:
            return_data.append(DATA[id])
    return return_data

def send_info_central(sock):
    global NUM_ACCESS_DATA
    global RT_DATA
    dict_RT = {"RT_DATA": RT_DATA, "NUM_ACCESS_DATA": NUM_ACCESS_DATA, "id": 1}
    json_RT = json.dumps(dict_RT)
    sock.sendall(bytes(json_RT, encoding="utf-8"))

def connect_to_central():
    global lock
    global NUM_ACCESS_DATA
    global RT_DATA
    while True:
        sleep(T)
        lock.acquire()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((CENTRAL_HOST, CENTRAL_PORT))
            send_info_central(sock)
        for id in range(0,NUM_DATA_BLOCKS):
            NUM_ACCESS_DATA[id] = 0
            RT_DATA[id] = -1
        lock.release()

def accept_wrapper(sock, sel):
    conn, addr = sock.accept()  # Should be ready to read
    print('Edge node 1 accepted connection from', addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_connection(key, mask, sel):
    global lock
    global NUM_ACCESS_DATA
    global RT_DATA
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = recv_timeout(sock)  # Should be ready to read
        if recv_data:
            data.outb += recv_data
        else:
            print('Edge node 1 closing connection to', data.addr)
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            str_data = data.outb.decode("utf-8")
            json_data = json.loads(str_data)
            if "data_blocks" in json_data.keys():
                return_data = find_data_blocks(json_data["data_blocks"])
                return_data = {"data": return_data}
                return_data = json.dumps(return_data)
                sock.sendall(bytes(return_data, encoding="utf-8"))
                data.outb = bytearray()
            elif "RT" in json_data.keys():
                lock.acquire()
                for id in json_data["RT_data_blocks"]:
                    RT_DATA[id] = json_data["RT"]
                    NUM_ACCESS_DATA[id] += 1
                print(NUM_ACCESS_DATA)
                data.outb = bytearray()
                lock.release()

def connect_to_clients():
    sel = selectors.DefaultSelector()
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.bind((SELF_HOST, SELF_PORT))
    lsock.listen()
    print('Edge node 1 listening on', (SELF_HOST, SELF_PORT))
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
    Thread(target = connect_to_central).start()
    Thread(target = connect_to_clients).start()

if __name__ == "__main__":
    global DATA
    DATA = [-1 for x in range(NUM_DATA_BLOCKS)]
    global RT_DATA
    RT_DATA = [-1 for x in range(NUM_DATA_BLOCKS)]
    global NUM_ACCESS_DATA
    NUM_ACCESS_DATA = [0 for x in range(NUM_DATA_BLOCKS)]
    global lock
    lock = threading.Lock()
    main()
