import socket
import time
import random

COORD_IP = '192.168.1.101'  #anpassen an Server IP
PORT = 5000


def run_smart_node():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)

    my_id = f"Node_{random.randint(10, 99)}"
    print(f"--- Node {my_id} gestartet ---")

    while True:
        #Idle
        print(f"[{my_id}] Arbeite lokal (kein Zugriff auf Critical Section nötig)...")
        time.sleep(random.uniform(2, 4))

        #REQuest
        granted = False
        while not granted:
            try:
                print(f"[{my_id}] Fordere Token an (REQ)...")
                sock.sendto(f"REQ {my_id}".encode(), (COORD_IP, PORT))
                msg, _ = sock.recvfrom(1024)
                if msg.decode() == 'GRANT':
                    granted = True
            except socket.timeout:
                print(f"[{my_id}] Keine Antwort, versuche erneut...")
                time.sleep(1)

        print(f"[{my_id}] >>> BETRITT CRITICAL SECTION <<<")

        # simulierte Arbeit
        work_duration = random.randint(3, 12)

        start_time = time.time()
        next_heartbeat = start_time

        while time.time() - start_time < work_duration:
            time.sleep(0.5)

            #heartbeat (2 Sekunden)
            if time.time() > next_heartbeat:
                print(f"[{my_id}] Sende Heartbeat (HB)...")
                sock.sendto(f"HB {my_id}".encode(), (COORD_IP, PORT))
                next_heartbeat = time.time() + 2.0

        #RELease
        print(f"[{my_id}] <<< VERLÄSST CRITICAL SECTION (REL) nach {work_duration}s")
        sock.sendto(f"REL {my_id}".encode(), (COORD_IP, PORT))

if __name__ == "__main__":
    run_smart_node()