import socket

NODE_HOST='127.0.0.1'
NODE_PORT=65434

def connect_to_edge_node():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((NODE_HOST, NODE_PORT))
        s.sendall(b'Hello, world')
        data = s.recv(1024)
    print('Received', repr(data))

def main():
    connect_to_edge_node()

if __name__ == "__main__":
    main()
