import socket
import time
import random
from typing import Optional
from protocol import REQ, GRANT, REL, HB, ACK, NACK, COORDINATOR, ReliableSender, serialize, deserialize, Message

COORD_IP = '127.0.0.1'
PORT = 50000
STATE_NORMAL = 'NORMAL'
STATE_WAITING_FOR_COORDINATOR = 'WAITING_FOR_COORDINATOR'

def run_smart_node():
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.settimeout(2.0)
	
	my_id = f"Node_{random.randint(10, 99)}"
	term = 0
	reliable_sender = ReliableSender()
	node_state = STATE_NORMAL
	coord_addr = (COORD_IP, PORT)
	
	print(f"--- Node {my_id} gestartet ---")
	
	while True:
		if node_state == STATE_WAITING_FOR_COORDINATOR:
			print(f"[{my_id}] Waiting for new coordinator...")
			coord_addr = wait_for_coordinator(sock, my_id)
			if coord_addr:
				node_state = STATE_NORMAL
				print(f"[{my_id}] New coordinator found at {coord_addr}")
			else:
				time.sleep(1)
				continue
		
		print(f"[{my_id}] Arbeite lokal (kein Zugriff auf Critical Section nötig)...")
		time.sleep(random.uniform(2, 4))
		
		granted = False
		retry_count = 0
		max_retries = 3
		
		while not granted and retry_count < max_retries:
			print(f"[{my_id}] Fordere Token an (REQ)...")
			req_msg = REQ(node_id=my_id, seq=0, term=term)
			
			grant_response = reliable_sender.send_reliable(sock, req_msg, coord_addr)
			if grant_response is None:
				retry_count += 1
				print(f"[{my_id}] Keine Antwort erhalten, Versuch {retry_count}/{max_retries}")
				if retry_count >= max_retries:
					print(f"[{my_id}] Coordinator failure detected after {max_retries} failed attempts")
					node_state = STATE_WAITING_FOR_COORDINATOR
					break
				time.sleep(1)
				continue
				
			retry_count = 0
			
			if isinstance(grant_response, NACK):
				if "redirect_to_" in grant_response.reason:
					new_coord = grant_response.reason.replace("redirect_to_", "")
					if ":" in new_coord:
						host, port = new_coord.split(":")
						coord_addr = (host, int(port))
						print(f"[{my_id}] Redirected to coordinator at {coord_addr}")
					continue
				else:
					print(f"[{my_id}] NACK erhalten: {grant_response.reason}, versuche erneut...")
					time.sleep(1)
					continue
				
			if isinstance(grant_response, GRANT):
				print(f"[{my_id}] GRANT erhalten mit lease_duration={grant_response.lease_duration}")
				term = max(term, grant_response.term)
				granted = True
		
		if not granted and node_state == STATE_WAITING_FOR_COORDINATOR:
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
				
				hb_response = reliable_sender.send_reliable(sock, hb_msg, coord_addr)
				if hb_response and isinstance(hb_response, ACK) and hb_response.msg_type == "HB":
					print(f"[{my_id}] HB ACK erhalten")
					term = max(term, hb_response.term)
				elif hb_response and isinstance(hb_response, NACK):
					print(f"[{my_id}] HB NACK erhalten: {hb_response.reason}")
				elif hb_response is None:
					print(f"[{my_id}] HB failed - coordinator may be down")
					
				next_heartbeat = time.time() + 2.0
		
		print(f"[{my_id}] <<< VERLÄSST CRITICAL SECTION (REL) nach {work_duration}s")
		rel_msg = REL(node_id=my_id, seq=0, term=term)
		
		rel_response = reliable_sender.send_reliable(sock, rel_msg, coord_addr)
		if rel_response and isinstance(rel_response, ACK) and rel_response.msg_type == "REL":
			print(f"[{my_id}] REL ACK erhalten")
			term = max(term, rel_response.term)
		elif rel_response and isinstance(rel_response, NACK):
			print(f"[{my_id}] REL NACK erhalten: {rel_response.reason}")
		elif rel_response is None:
			print(f"[{my_id}] REL failed - coordinator may be down")


def wait_for_coordinator(sock: socket.socket, node_id: str) -> Optional[tuple]:
	"""Wait for COORDINATOR broadcast message and return new coordinator address."""
	sock.settimeout(1.0)
	
	try:
		data, addr = sock.recvfrom(1024)
		msg = deserialize(data)
		
		if isinstance(msg, COORDINATOR):
			print(f"[{node_id}] Received COORDINATOR broadcast: {msg.coord_addr}")
			return msg.coord_addr
			
	except socket.timeout:
		pass
	except Exception as e:
		print(f"[{node_id}] Error waiting for coordinator: {e}")
	
	finally:
		sock.settimeout(2.0)
	
	return None


if __name__ == "__main__":
    run_smart_node()