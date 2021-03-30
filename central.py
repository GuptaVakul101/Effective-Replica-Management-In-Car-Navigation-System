import socket
import selectors
import types
import random
import sys
from utils import recv_timeout
import json

SELF_HOST='127.0.0.1'
SELF_PORT=65432

NUM_DATA_BLOCKS = 15
NUM_EDGE_NODES = 2

T = 2
K = 5

GLOBAL_CLOCK = 1
CLOCK = 0
CLOCK_HELPER = 0
STATIC_FACTOR = 1
ALPHA = 1
BETA_MEAN = 0
SUB_WEIGHT_PLACEMENT_1 = 0.4
SUB_WEIGHT_PLACEMENT_2 = 0.2
SUB_WEIGHT_PLACEMENT_3 = 0.4

def accept_wrapper(sock, sel):
    conn, addr = sock.accept()  # Should be ready to read
    print('accepted connection from', addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_connection(key, mask, sel):
    global CLOCK_HELPER
    global CLOCK
    global F
    global ART

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
            if "RT_DATA" in json_data.keys():
                for id in range(NUM_DATA_BLOCKS):
                    if json_data["RT_DATA"][id] != -1:
                        F[CLOCK][id] += json_data["NUM_ACCESS_DATA"][id]
                        ART[CLOCK][id] += json_data["RT_DATA"][id]
                SUB_OBJECTIVE_1[json_data["id"]-1] += json_data["sub_objective_1"]
                SUB_OBJECTIVE_2[json_data["id"]-1] += json_data["beta"]
                SUB_OBJECTIVE_3[json_data["id"]-1] += json_data["sub_objective_3"]

                CLOCK_HELPER += 1
                if CLOCK_HELPER == NUM_EDGE_NODES:
                    CLOCK += 1
                    CLOCK_HELPER = 0

def main():
    global CLOCK
    global ART
    global ART_FINAL
    global F
    global H
    global HEAT
    global H_PREV
    global DF
    global OPT_NUM_REPLICA
    global OPT_NUM_REPLICA_PREV
    global GLOBAL_CLOCK
    global ALPHA
    global BETA_MEAN
    global SUB_OBJECTIVE_1
    global SUB_OBJECTIVE_2
    global SUB_OBJECTIVE_3
    global OBJECTIVE

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
            if CLOCK == K:

                # Replica Creation
                # ART
                for j in range(NUM_DATA_BLOCKS):
                    non_zero_art = 0
                    for i in range(K):
                        if ART[i][j] > 0:
                            non_zero_art += 1
                            ART[i][j] /= F[i][j]
                            ART_FINAL[j] += ART[i][j]
                    if non_zero_art > 0:
                        ART_FINAL[j] /= non_zero_art

                # H
                for j in range(NUM_DATA_BLOCKS):
                    for i in range(K):
                        H[j] += F[i][j]
                    H[j] /= K

                # HEAT
                for j in range(NUM_DATA_BLOCKS):
                    HEAT[j] = H[j] + H_PREV[j]

                # Resetting H
                H_PREV = H

                # DF
                for j in range(NUM_DATA_BLOCKS):
                    DF[j] = HEAT[j]*ART_FINAL[j]
                print("DF:", DF)

                # Calculating optimal number of replicas
                for j in range(NUM_DATA_BLOCKS):
                    OPT_NUM_REPLICA[j] = ALPHA*OPT_NUM_REPLICA_PREV[j] + ((1 - ALPHA)*DF[j])/STATIC_FACTOR
                OPT_NUM_REPLICA_PREV = OPT_NUM_REPLICA
                print("Opt num replica:",OPT_NUM_REPLICA)


                # Replica Placement
                for id in range(NUM_EDGE_NODES):
                    SUB_OBJECTIVE_1[id] /= K
                    SUB_OBJECTIVE_2[id] /= K
                    SUB_OBJECTIVE_3[id] /= K
                    BETA_MEAN += SUB_OBJECTIVE_2[id]

                BETA_MEAN /= NUM_EDGE_NODES

                # Sub objective 2
                for id in range(NUM_EDGE_NODES):
                    SUB_OBJECTIVE_2[id] = NUM_EDGE_NODES/(SUB_OBJECTIVE_2[id]-BETA_MEAN)


                # Objective function
                for id in range(NUM_EDGE_NODES):
                    OBJECTIVE[id] = SUB_WEIGHT_PLACEMENT_1*SUB_OBJECTIVE_1[id] + SUB_WEIGHT_PLACEMENT_2*SUB_OBJECTIVE_2[id] + SUB_WEIGHT_PLACEMENT_3*SUB_OBJECTIVE_3[id]

                print("Objective function:", OBJECTIVE)

                OBJECTIVE = sorted(range(len(OBJECTIVE)), key=lambda k: OBJECTIVE[k])

                # Placement
                current_num_replicas = [0 for x in range(NUM_DATA_BLOCKS)]
                for i in range(NUM_DATA_BLOCKS):
                    num = 0
                    for j in range(NUM_EDGE_NODES):
                        current_num_replicas[i] += BIN_ENCODING[j][i]

                replicas_added = [-1 for x in range(NUM_DATA_BLOCKS)]

                for i in range(NUM_EDGE_NODES-1, 0, -1):
                    for j in range(NUM_DATA_BLOCKS):
                        if replicas_added[j] == -1 and OPT_NUM_REPLICA[j] > current_num_replicas[j] and BIN_ENCODING[i][j] == 0:
                            BIN_ENCODING[i][j] = 1
                            replicas_added[j] = i

                replicas_removed = [-1 for x in range(NUM_DATA_BLOCKS)]

                for i in range(NUM_EDGE_NODES):
                    for j in range(NUM_DATA_BLOCKS):
                        if replicas_removed[j] == -1 and OPT_NUM_REPLICA[j] < current_num_replicas[j] and BIN_ENCODING[i][j] == 1:
                            BIN_ENCODING[i][j] = 0
                            replicas_removed[j] = i

                print("Replicas added:", replicas_added)
                print("Replicas removed:", replicas_removed)

                # Resetting clock
                CLOCK = 0
                GLOBAL_CLOCK += 1
                ALPHA = GLOBAL_CLOCK/(2*GLOBAL_CLOCK-1)

                # Resetting
                ART = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(K)]
                ART_FINAL = [0 for x in range(NUM_DATA_BLOCKS)]
                H = [0 for x in range(NUM_DATA_BLOCKS)]
                F = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(K)]
                HEAT = [0 for x in range(NUM_DATA_BLOCKS)]
                DF = [0 for x in range(NUM_DATA_BLOCKS)]
                OPT_NUM_REPLICA = [0 for x in range(NUM_DATA_BLOCKS)]
                BETA_MEAN = 0
                SUB_OBJECTIVE_1 = [0 for x in range(NUM_EDGE_NODES)]
                SUB_OBJECTIVE_2 = [0 for x in range(NUM_EDGE_NODES)]
                SUB_OBJECTIVE_3 = [0 for x in range(NUM_EDGE_NODES)]
                OBJECTIVE = [0 for x in range(NUM_EDGE_NODES)]

            if key.data is None:
                accept_wrapper(key.fileobj, sel)
            else:
                service_connection(key, mask, sel)

if __name__ == "__main__":
    global BIN_ENCODING
    BIN_ENCODING = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(NUM_EDGE_NODES)]
    global DATA
    DATA = [random.randint(0,sys.maxsize) for x in range(NUM_DATA_BLOCKS)]
    global ART
    ART = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(K)]
    global ART_FINAL
    ART_FINAL = [0 for x in range(NUM_DATA_BLOCKS)]
    global H
    H = [0 for x in range(NUM_DATA_BLOCKS)]
    global F
    F = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(K)]
    global H_PREV
    H_PREV = [0 for x in range(NUM_DATA_BLOCKS)]
    global HEAT
    HEAT = [0 for x in range(NUM_DATA_BLOCKS)]
    global DF
    DF = [0 for x in range(NUM_DATA_BLOCKS)]
    global OPT_NUM_REPLICA
    OPT_NUM_REPLICA = [0 for x in range(NUM_DATA_BLOCKS)]
    global OPT_NUM_REPLICA_PREV
    OPT_NUM_REPLICA_PREV = [0 for x in range(NUM_DATA_BLOCKS)]
    global SUB_OBJECTIVE_1
    SUB_OBJECTIVE_1 = [0 for x in range(NUM_EDGE_NODES)]
    global SUB_OBJECTIVE_2
    SUB_OBJECTIVE_2 = [0 for x in range(NUM_EDGE_NODES)]
    global SUB_OBJECTIVE_3
    SUB_OBJECTIVE_3 = [0 for x in range(NUM_EDGE_NODES)]
    global OBJECTIVE
    OBJECTIVE = [0 for x in range(NUM_EDGE_NODES)]
    main()
