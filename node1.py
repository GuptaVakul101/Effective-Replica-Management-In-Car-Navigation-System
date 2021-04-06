import socket
from multiprocessing import Process
import sys
import selectors
import types
import struct
from central import NUM_DATA_BLOCKS, T
from collector import HEARTBEAT_PERIOD
import json
import time
from utils import recv_timeout
from time import sleep
import threading
from threading import Thread
import psutil

CENTRAL_HOST = '127.0.0.1'
CENTRAL_PORT = 65432

COLLECTOR_HOST = '127.0.0.1'
COLLECTOR_PORT = 65434

SELF_HOST = '127.0.0.1'
SELF_PORT = 65433
ALPHA = 0.5

def send_heartbeat_packet():
    while True:
        sleep(HEARTBEAT_PERIOD)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((COLLECTOR_HOST, COLLECTOR_PORT))
            dict_id = {"id": 1}
            json_id = json.dumps(dict_id)
            sock.sendall(bytes(json_id, encoding="utf-8"))

def find_data_blocks(data_blocks):
    return_data = []
    for id in data_blocks:
        if DATA[id] != -1:
            return_data.append(DATA[id])
    return return_data

def send_info_central(sock):
    global NUM_ACCESS_DATA
    global RT_DATA

    uf = psutil.cpu_percent()/10**2
    nc = psutil.cpu_count(logical = False)
    fr = psutil.cpu_freq().max
    cpu_capacity = fr * nc * (1 - uf)

    disk_read_speed = psutil.disk_io_counters().read_time/10**3
    disk_write_speed = psutil.disk_io_counters().write_time/10**3
    disk_performance = disk_read_speed * ALPHA + disk_write_speed * (1 - ALPHA)

    mem_usage = psutil.virtual_memory().percent/10**2
    mem_size = psutil.virtual_memory().total/10**6

    load_capacity_memory = mem_size * (1 - mem_usage)
    load_capacity_disk = psutil.disk_usage('/').free/10**6
    sub_objective_1 = cpu_capacity + disk_performance + load_capacity_memory + load_capacity_disk

    f = 0
    for val in DATA:
        if val != 1:
            f += 1
    total_disk_space = psutil.disk_usage('/').total/10**6
    bsize = 1
    beta = (f * bsize) / total_disk_space

    net_dis_coeff = 1
    data_block_size = 1
    Ctr = 10

    sub_objective_3 = 10**5 / (net_dis_coeff * data_block_size * Ctr)

    print(sub_objective_1)
    print(beta)
    print(sub_objective_3)

    dict_RT = {"RT_DATA": RT_DATA, "NUM_ACCESS_DATA": NUM_ACCESS_DATA, "id": 1, "sub_objective_1": sub_objective_1, "beta": beta, "sub_objective_3": sub_objective_3}
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

def handleUpdateQuery(json_data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((CENTRAL_HOST, CENTRAL_PORT))
        json_help = json.dumps(json_data)
        sock.sendall(bytes(json_help, encoding="utf-8"))

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
    global DATA
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
                if json_data["type"] == 0:
                    return_data = find_data_blocks(json_data["data_blocks"])
                    return_data = {"data": return_data}
                    return_data = json.dumps(return_data)
                    sock.sendall(bytes(return_data, encoding="utf-8"))
                    # data.outb = bytearray()
                else:
                    handleUpdateQuery(json_data)
                    # data.outb = bytearray()
            elif "RT" in json_data.keys():
                lock.acquire()
                for id in json_data["RT_data_blocks"]:
                    RT_DATA[id] = json_data["RT"]
                    NUM_ACCESS_DATA[id] += 1
                # print(NUM_ACCESS_DATA)
                # data.outb = bytearray()
                lock.release()
            elif "new_data_blocks" in json_data.keys():
                print("New data blocks received:", json_data["new_data_blocks"])
                for key in json_data["new_data_blocks"].keys():
                    DATA[int(key)] = json_data["new_data_blocks"][key]
            data.outb = bytearray()

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
    Thread(target = send_heartbeat_packet).start()

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
