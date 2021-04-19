import socket
import time
import random
import json
import struct
import sys

from central import NUM_DATA_BLOCKS
from utils import recv_timeout, TIMEOUT

NODE_HOST='127.0.0.1'
NODE_PORT=65433

Q = 0

def query(sock):
    data_blocks = random.sample(range(0, NUM_DATA_BLOCKS), random.randint(1, NUM_DATA_BLOCKS))
    choiceList = [0, 1]
    distList = [0.9, 0.1]
    type = random.choices(choiceList, distList)
    data = {}
    values = [random.randint(0,sys.maxsize) for x in range(len(data_blocks))]
    if type[0] == 0:
        data = {"data_blocks": data_blocks, "type": type[0]}
        print("Query type: READ")
        print("Requested data blocks:", data_blocks)
    else:
        data = {"data_blocks": data_blocks, "type": type[0], "values": values, "id": 1}
        print("Query type: UPDATE")
        print("Updation data blocks:", data_blocks)
        print("Updation values:", values)
    data = json.dumps(data)
    sock.sendall(bytes(data, encoding="utf-8"))
    received = recv_timeout(sock)
    received = received.decode("utf-8")
    # print(received)
    return data_blocks

def send_RT(RT, data_blocks, sock):
    dict_RT = {"RT_data_blocks": data_blocks, "RT": RT}
    json_RT = json.dumps(dict_RT)
    sock.sendall(bytes(json_RT, encoding="utf-8"))

def connect_to_edge_node():
    global Q
    while True:
        print("====================================================================================================")
        print("                              QUERY", Q)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((NODE_HOST, NODE_PORT))
            queryStartTime = time.time()
            data_blocks = query(sock)   # List of queried data blocks
            queryEndTime = time.time()
            print("Query response time:", queryEndTime - queryStartTime - 2*TIMEOUT)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((NODE_HOST, NODE_PORT))
            send_RT(queryEndTime - queryStartTime - 2*TIMEOUT, data_blocks, sock)

        print("====================================================================================================")
        Q += 1

def main():
    connect_to_edge_node()

if __name__ == "__main__":
    main()
