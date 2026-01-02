import socket
import time
import random
from typing import Optional
from protocol import REQ, GRANT, REL, HB, ACK, NACK, ReliableSender, serialize, deserialize, Message

COORD_IP = '192.168.1.101'
PORT = 50000


def run_smart_node():
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.settimeout(2.0)
	
	my_id = f"Node_{random.randint(10, 99)}"
	term = 0
	reliable_sender = ReliableSender()
	
	print(f"--- Node {my_id} gestartet ---")
	
	while True:
		print(f"[{my_id}] Arbeite lokal (kein Zugriff auf Critical Section nötig)...")
		time.sleep(random.uniform(2, 4))
		
		granted = False
		while not granted:
			print(f"[{my_id}] Fordere Token an (REQ)...")
			req_msg = REQ(node_id=my_id, seq=0, term=term)
			
			ack_response = reliable_sender.send_reliable(sock, req_msg, (COORD_IP, PORT))
			if ack_response is None:
				print(f"[{my_id}] Keine ACK erhalten, versuche erneut...")
				time.sleep(1)
				continue
				
			if isinstance(ack_response, NACK):
				print(f"[{my_id}] NACK erhalten: {ack_response.reason}, versuche erneut...")
				time.sleep(1)
				continue
				
			if isinstance(ack_response, ACK) and ack_response.msg_type == "REQ":
				print(f"[{my_id}] REQ ACK erhalten, warte auf GRANT...")
				term = max(term, ack_response.term)
				
				try:
					while True:
						data, _ = sock.recvfrom(1024)
						grant_msg = deserialize(data)
						if grant_msg and isinstance(grant_msg, GRANT) and grant_msg.node_id == my_id:
							print(f"[{my_id}] GRANT erhalten mit lease_duration={grant_msg.lease_duration}")
							term = max(term, grant_msg.term)
							granted = True
							break
				except socket.timeout:
					print(f"[{my_id}] Timeout beim Warten auf GRANT, versuche REQ erneut...")
					continue
		
		print(f"[{my_id}] >>> BETRITT CRITICAL SECTION <<<")
		
		work_duration = random.randint(3, 12)
		start_time = time.time()
		next_heartbeat = start_time
		
		while time.time() - start_time < work_duration:
			time.sleep(0.5)
			
			if time.time() > next_heartbeat:
				print(f"[{my_id}] Sende Heartbeat (HB)...")
				hb_msg = HB(node_id=my_id, seq=0, term=term)
				
				hb_response = reliable_sender.send_reliable(sock, hb_msg, (COORD_IP, PORT))
				if hb_response and isinstance(hb_response, ACK) and hb_response.msg_type == "HB":
					print(f"[{my_id}] HB ACK erhalten")
					term = max(term, hb_response.term)
				elif hb_response and isinstance(hb_response, NACK):
					print(f"[{my_id}] HB NACK erhalten: {hb_response.reason}")
					
				next_heartbeat = time.time() + 2.0
		
		print(f"[{my_id}] <<< VERLÄSST CRITICAL SECTION (REL) nach {work_duration}s")
		rel_msg = REL(node_id=my_id, seq=0, term=term)
		
		rel_response = reliable_sender.send_reliable(sock, rel_msg, (COORD_IP, PORT))
		if rel_response and isinstance(rel_response, ACK) and rel_response.msg_type == "REL":
			print(f"[{my_id}] REL ACK erhalten")
			term = max(term, rel_response.term)
		elif rel_response and isinstance(rel_response, NACK):
			print(f"[{my_id}] REL NACK erhalten: {rel_response.reason}")

if __name__ == "__main__":
    run_smart_node()