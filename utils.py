import time
TIMEOUT = 1

def recv_timeout(the_socket, timeout=TIMEOUT):
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
