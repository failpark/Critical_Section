import socket
import time
import random

COORD_IP = '192.168.1.101'
PORT = 50000

def run_node():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    my_id = f"Node_1"

    while True:
        time.sleep(random.uniform(2, 5))
        sock.sendto(f"REQ {my_id}".encode(), (COORD_IP, PORT))
        sock.recv(1024)

        time.sleep(random.uniform(2, 4))

        sock.sendto(f"REL {my_id}".encode(), (COORD_IP, PORT))

if __name__ == "__main__":
    run_node()