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
# SYNC_W = (NUM_EDGE_NODES // 2) + 1
SYNC_W = 1

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

NODE_HOSTS = ['127.0.0.1']
NODE_PORTS = [65433]

def handle_node_failure():
    global NODE_FAILURE

    potential_data_blocks = {}
    for i in range(NUM_EDGE_NODES):
        if NODE_FAILURE[i] == 1:
            for j in range(NUM_DATA_BLOCKS):
                if BIN_ENCODING[i][j] == 1:
                    potential_data_blocks.add(j)

    if len(potential_data_blocks) == 0:
        for i in range(NUM_EDGE_NODES):
            NODE_FAILURE[i] = 0
        return

    sco = 0
    asel = 0
    for i in range(NUM_DATA_BLOCKS):
        sco += H[i]

    cost = [1 for x in range(NUM_DATA_BLOCKS)]

    sel = [0 for x in range(NUM_DATA_BLOCKS)]
    for id in potential_data_blocks:
        sel[id] = H[id]/sco
        sel[id] /= cost[id]
        asel += sel[id]

    asel /= len(potential_data_blocks)

    recoverable_data_blocks = {}
    for id in potential_data_blocks:
        if sel[id] > asel:
            recoverable_data_blocks.add(id)

    for i in range(NUM_EDGE_NODES-1, -1, -1):
        edge_id = OBJECTIVE[i]
        if ACTIVE_NODES[edge_id] == 0:
            print("Failed node", i+1, "skipped")
            continue
        recovery_data_blocks = {}
        for j in recoverable_data_blocks:
            if BIN_ENCODING[edge_id][j] == 0:
                recovery_data_blocks[j] = DATA[j]
        for j in new_data_blocks.keys():
            recoverable_data_blocks.remove(j)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((NODE_HOSTS[edge_id], NODE_PORTS[edge_id]))
            dict = {"new_data_blocks": recovery_data_blocks}
            json_recovery_blocks = json.dumps(dict)
            sock.sendall(bytes(json_recovery_blocks, encoding="utf-8"))

    for i in range(NUM_EDGE_NODES):
        NODE_FAILURE[i] = 0


def triggerUnsynchUpdates():
    global H
    global SUB_OBJECTIVE_1
    global RST

    hah = 0
    for i in range(len(H)):
        hah += H[i]
    hah /= NUM_DATA_BLOCKS

    alc = 0
    for i in range(len(SUB_OBJECTIVE_1)):
        alc += SUB_OBJECTIVE_1[i]
    alc /= NUM_EDGE_NODES

    print("hah: ", hah)
    print("alc: ", alc)

    # handle CASE-2 (alc)
    for id in range(NUM_EDGE_NODES):
        if SUB_OBJECTIVE_1[id] > alc:
            update_data_blocks = {}
            for j in range(NUM_DATA_BLOCKS):
                if RST[id][j] == 1:
                    update_data_blocks[j] = DATA[j]
                    RST[id][j] = 0
            if len(update_data_blocks.keys()) != 0:
                if ACTIVE_NODES[id] == 1:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.connect((NODE_HOSTS[id], NODE_PORTS[id]))
                        dict = {"new_data_blocks": update_data_blocks}
                        json_update_blocks = json.dumps(dict)
                        sock.sendall(bytes(json_update_blocks, encoding="utf-8"))

    # handle CASE-1 (hah)
    for id in range(NUM_DATA_BLOCKS):
        if H[id] > hah:
            for j in range(NUM_EDGE_NODES):
                if RST[j][id] == 1:
                    update_data_blocks = {id: DATA[id]}
                    if ACTIVE_NODES[j] == 1:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                            sock.connect((NODE_HOSTS[j], NODE_PORTS[j]))
                            dict = {"new_data_blocks": update_data_blocks}
                            json_update_blocks = json.dumps(dict)
                            sock.sendall(bytes(json_update_blocks, encoding="utf-8"))
                        RST[j][id] = 0

def sendUpdateData(send_write_updates):
    for id in range(NUM_EDGE_NODES):
        update_data_blocks = {}
        for j in range(NUM_DATA_BLOCKS):
            if send_write_updates[id][j] == 1:
                update_data_blocks[j] = DATA[j]
        if len(update_data_blocks.keys()) != 0:
            if ACTIVE_NODES[id] == 1:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((NODE_HOSTS[id], NODE_PORTS[id]))
                    dict = {"new_data_blocks": update_data_blocks}
                    json_update_blocks = json.dumps(dict)
                    sock.sendall(bytes(json_update_blocks, encoding="utf-8"))

def replica_synchronization():
    send_write_updates = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(NUM_EDGE_NODES)]
    for i in range(NUM_EDGE_NODES):
        for j in range(NUM_DATA_BLOCKS):
            if WRITE_REQUESTS[i][j] == 1:
                for k in range(0, NUM_EDGE_NODES):
                    if k < max(0, i - SYNC_W // 2) or k >= min(NUM_EDGE_NODES, i + SYNC_W // 2 + 1):
                        RST[i][j] = 1
                    else:
                        send_write_updates[k][j] = 1
                        RST[i][j] = 0
    sendUpdateData(send_write_updates)

def sendReplicas(replicas_added, replicas_removed):
    for id in range(len(NODE_HOSTS)):
        print('Active nodes', ACTIVE_NODES)
        if ACTIVE_NODES[id] == 0:
            continue

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((NODE_HOSTS[id], NODE_PORTS[id]))

            new_data_blocks = {}
            for i in range(len(replicas_added)):
                if replicas_added[i] == id:
                    new_data_blocks[i] = DATA[i]

            for i in range(len(replicas_removed)):
                if replicas_removed[i] == id:
                    new_data_blocks[i] = -1

            dict = {"new_data_blocks": new_data_blocks}
            json_new_blocks = json.dumps(dict)
            print(json_new_blocks)

            sock.sendall(bytes(json_new_blocks, encoding="utf-8"))
            print("done")

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
    global DATA
    global WRITE_REQUESTS
    global NODE_FAILURE
    global ACTIVE_NODES

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
            elif "data_blocks" in json_data.keys():
                print(json_data)
                for id in range(len(json_data["data_blocks"])):
                    WRITE_REQUESTS[json_data["id"] - 1][json_data["data_blocks"][id]] = 1
                    DATA[json_data["data_blocks"][id]] = json_data["values"][id]
            elif "failure_id" in json_data.keys():
                NODE_FAILURE[json_data["failure_id"]-1] = 1
                ACTIVE_NODES[json_data["failure_id"]-1] = 0

            data.outb = bytearray()

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
                print("Sorted objective indexes:", OBJECTIVE)

                # Placement
                current_num_replicas = [0 for x in range(NUM_DATA_BLOCKS)]
                for i in range(NUM_DATA_BLOCKS):
                    num = 0
                    for j in range(NUM_EDGE_NODES):
                        current_num_replicas[i] += BIN_ENCODING[j][i]

                replicas_added = [-1 for x in range(NUM_DATA_BLOCKS)]

                for i in range(NUM_EDGE_NODES-1, -1, -1):
                    edge_id = OBJECTIVE[i]
                    for j in range(NUM_DATA_BLOCKS):
                        if replicas_added[j] == -1 and OPT_NUM_REPLICA[j] > current_num_replicas[j] and BIN_ENCODING[edge_id][j] == 0:
                            BIN_ENCODING[edge_id][j] = 1
                            replicas_added[j] = edge_id

                replicas_removed = [-1 for x in range(NUM_DATA_BLOCKS)]

                for i in range(NUM_EDGE_NODES):
                    edge_id = OBJECTIVE[i]
                    for j in range(NUM_DATA_BLOCKS):
                        if replicas_removed[j] == -1 and OPT_NUM_REPLICA[j] < current_num_replicas[j] and BIN_ENCODING[edge_id][j] == 1:
                            BIN_ENCODING[edge_id][j] = 0
                            replicas_removed[j] = edge_id

                print("Replicas added:", replicas_added)
                print("Replicas removed:", replicas_removed)

                sendReplicas(replicas_added, replicas_removed)

                # Replica Synchronization
                replica_synchronization()

                # trigger unsynchronized updates
                triggerUnsynchUpdates()

                # Handle node failure
                handle_node_failure()

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
    global WRITE_REQUESTS
    WRITE_REQUESTS = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(NUM_EDGE_NODES)]
    global RST
    RST = [[0 for x in range(NUM_DATA_BLOCKS)] for y in range(NUM_EDGE_NODES)]
    global NODE_FAILURE
    NODE_FAILURE = [0 for x in range(NUM_EDGE_NODES)]
    global ACTIVE_NODES
    ACTIVE_NODES = [1 for x in range(NUM_EDGE_NODES)]
    main()
