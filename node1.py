import socket

CENTRAL_HOST='127.0.0.1'
CENTRAL_PORT=65432

def connect_to_central():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((CENTRAL_HOST, CENTRAL_PORT))
        s.sendall(b'Hello, world')
        data = s.recv(1024)
    print('Received', repr(data))

def main():
    connect_to_central()

if __name__ == "__main__":
    main()
