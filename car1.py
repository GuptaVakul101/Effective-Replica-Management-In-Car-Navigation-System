import socket
import time
import random
import json
import struct

from central import NUM_DATA_BLOCKS

NODE_HOST='127.0.0.1'
NODE_PORT=65433

def recv_timeout(the_socket, timeout=1):
    #make socket non blocking
    the_socket.setblocking(0)

    #total data partwise in an array
    total_data = bytearray()

    #beginning time
    begin = time.time()
    while 1:
        #if you got some data, then break after timeout
        if total_data and time.time()-begin > timeout:
            break

        #if you got no data at all, wait a little longer, twice the timeout
        elif time.time()-begin > timeout*2:
            break

        #recv something
        try:
            data = the_socket.recv(8192)
            if data:
                total_data += data
                #change the beginning time for measurement
                begin=time.time()
            else:
                #sleep for sometime to indicate a gap
                time.sleep(0.1)
        except:
            pass

    #join all parts to make final string
    return total_data

def query(sock):
    data_blocks = random.sample(range(0, NUM_DATA_BLOCKS), random.randint(1, NUM_DATA_BLOCKS))
    data = {"data_blocks": data_blocks}
    data = json.dumps(data)
    sock.sendall(bytes(data, encoding="utf-8"))
    received = recv_timeout(sock)
    received = received.decode("utf-8")
    return data_blocks

def send_RT(data_blocks):
    pass

def connect_to_edge_node():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((NODE_HOST, NODE_PORT))
        queryStartTime = time.time()
        data_blocks = query(sock)   # List of queried data blocks
        queryEndTime = time.time()
        print(queryEndTime-queryStartTime)
        send_RT(data_blocks)

def main():
    connect_to_edge_node()

if __name__ == "__main__":
    main()
