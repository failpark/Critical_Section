import socket
import time
import random
import argparse
import os
from typing import Optional
from protocol import REQ, GRANT, REL, HB, ACK, NACK, COORDINATOR, STEP_DOWN, ReliableSender, serialize, deserialize, Message

STATE_NORMAL = 'NORMAL'
STATE_WAITING_FOR_COORDINATOR = 'WAITING_FOR_COORDINATOR'

# Backoff configuration
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 16.0
BACKOFF_MULTIPLIER = 2.0
JITTER_FACTOR = 0.25

def calculate_backoff_delay(current_backoff: float) -> tuple[float, float]:
	"""
	Calculate actual delay with jitter and next backoff value.

	Returns:
		(delay_with_jitter, next_backoff_value)
	"""
	jitter = random.uniform(1 - JITTER_FACTOR, 1 + JITTER_FACTOR)
	delay = current_backoff * jitter
	next_backoff = min(current_backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
	return delay, next_backoff

def run_smart_node(node_id: Optional[str] = None, coord_ip: str = '127.0.0.1', coord_port: int = 50000):
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.settimeout(2.0)

	my_id = node_id if node_id else f"Node_{random.randint(10, 99)}"
	term = 0
	reliable_sender = ReliableSender()
	node_state = STATE_NORMAL
	coord_addr = (coord_ip, coord_port)
	backoff_delay = INITIAL_BACKOFF
	
	print(f"--- Node {my_id} started ---")
	
	while True:
		if node_state == STATE_WAITING_FOR_COORDINATOR:
			print(f"[{my_id}] Waiting for new coordinator...")
			coord_addr = wait_for_coordinator(sock, my_id)
			if coord_addr:
				node_state = STATE_NORMAL
				print(f"[{my_id}] New coordinator found at {coord_addr}")
				backoff_delay = INITIAL_BACKOFF
			else:
				time.sleep(1)
				continue
		
		time.sleep(random.uniform(2, 4))
		
		granted = False
		retry_count = 0
		max_retries = 3
		
		while not granted and retry_count < max_retries:
			print(f"[{my_id}] Requesting token (REQ)...")
			req_msg = REQ(node_id=my_id, seq=0, term=term)
			
			grant_response = reliable_sender.send_reliable(sock, req_msg, coord_addr)
			if grant_response is None:
				retry_count += 1
				print(f"[{my_id}] No response received, attempt {retry_count}/{max_retries}")
				if retry_count >= max_retries:
					print(f"[{my_id}] Coordinator failure detected after {max_retries} failed attempts")
					node_state = STATE_WAITING_FOR_COORDINATOR
					break
				delay, backoff_delay = calculate_backoff_delay(backoff_delay)
				time.sleep(delay)
				continue
				
			retry_count = 0
			
			if isinstance(grant_response, NACK):
				if "redirect_to_" in grant_response.reason:
					new_coord = grant_response.reason.replace("redirect_to_", "")
					if ":" in new_coord:
						host, port = new_coord.split(":")
						coord_addr = (host, int(port))
						print(f"[{my_id}] Redirected to coordinator at {coord_addr}")
					backoff_delay = INITIAL_BACKOFF
					continue
				else:
					print(f"[{my_id}] NACK received: {grant_response.reason}, retrying...")
					term = max(term, grant_response.term)
					time.sleep(1)
					continue

			# Handle ACK without GRANT (request queued)
			if isinstance(grant_response, ACK) and grant_response.msg_type == "REQ":
				print(f"[{my_id}] REQ acknowledged but no GRANT - request queued")
				delay, backoff_delay = calculate_backoff_delay(backoff_delay)
				time.sleep(delay)
				continue

			# Handle GRANT - exit loop immediately
			if isinstance(grant_response, GRANT):
				print(f"[{my_id}] GRANT received with lease_duration={grant_response.lease_duration}")
				term = max(term, grant_response.term)
				backoff_delay = INITIAL_BACKOFF
				granted = True
				break

			# Fallback backoff for unexpected responses
			delay, backoff_delay = calculate_backoff_delay(backoff_delay)
			time.sleep(delay)

		if not granted and node_state == STATE_WAITING_FOR_COORDINATOR:
			continue

		if granted:
			print(f"[{my_id}] >>> ENTERING CRITICAL SECTION <<<")
		
		work_duration = random.randint(3, 12)
		start_time = time.time()
		next_heartbeat = start_time
		
		while time.time() - start_time < work_duration:
			time.sleep(0.5)
			
			if time.time() > next_heartbeat:
				print(f"[{my_id}] Sending heartbeat (HB)...")
				hb_msg = HB(node_id=my_id, seq=0, term=term)
				
				hb_response = reliable_sender.send_reliable(sock, hb_msg, coord_addr)
				if hb_response and isinstance(hb_response, ACK) and hb_response.msg_type == "HB":
					print(f"[{my_id}] HB ACK received")
					term = max(term, hb_response.term)
				elif hb_response and isinstance(hb_response, NACK):
					print(f"[{my_id}] HB NACK received: {hb_response.reason}")
					term = max(term, hb_response.term)
				elif hb_response is None:
					print(f"[{my_id}] HB failed - coordinator may be down")
					# Check for coordinator updates during failed heartbeat
					new_coord_addr = check_for_coordinator_updates(sock, my_id)
					if new_coord_addr:
						coord_addr = new_coord_addr
						print(f"[{my_id}] Updated coordinator to {coord_addr} during heartbeat")
					
				next_heartbeat = time.time() + 2.0
		
		print(f"[{my_id}] <<< LEAVING CRITICAL SECTION (REL) after {work_duration}s")
		rel_msg = REL(node_id=my_id, seq=0, term=term)
		
		rel_response = reliable_sender.send_reliable(sock, rel_msg, coord_addr)
		if rel_response and isinstance(rel_response, ACK) and rel_response.msg_type == "REL":
			print(f"[{my_id}] REL ACK received")
			term = max(term, rel_response.term)
		elif rel_response and isinstance(rel_response, NACK):
			print(f"[{my_id}] REL NACK received: {rel_response.reason}")
			term = max(term, rel_response.term)
		elif rel_response is None:
			print(f"[{my_id}] REL failed - coordinator may be down")


def check_for_coordinator_updates(sock: socket.socket, node_id: str) -> Optional[tuple]:
	"""Check for immediate COORDINATOR or STEP_DOWN messages without blocking."""
	original_timeout = sock.gettimeout()
	sock.settimeout(0.1)  # Very short timeout for non-blocking check
	
	try:
		data, addr = sock.recvfrom(1024)
		msg = deserialize(data)
		
		if isinstance(msg, COORDINATOR):
			print(f"[{node_id}] Updated coordinator: {msg.coord_addr}")
			return msg.coord_addr
		elif isinstance(msg, STEP_DOWN):
			print(f"[{node_id}] Coordinator {msg.node_id} stepped down")
			return None  # Will trigger coordinator wait
			
	except socket.timeout:
		pass  # No immediate messages
	except Exception as e:
		print(f"[{node_id}] Error checking coordinator updates: {e}")
	finally:
		sock.settimeout(original_timeout)
	
	return None

def wait_for_coordinator(sock: socket.socket, node_id: str) -> Optional[tuple]:
	"""Wait for COORDINATOR broadcast message and return new coordinator address."""
	sock.settimeout(1.0)
	
	try:
		data, addr = sock.recvfrom(1024)
		msg = deserialize(data)
		
		if isinstance(msg, COORDINATOR):
			print(f"[{node_id}] Received COORDINATOR broadcast: {msg.coord_addr}")
			return msg.coord_addr
		elif isinstance(msg, STEP_DOWN):
			print(f"[{node_id}] Received STEP_DOWN from {msg.node_id}, continuing to wait")
			
	except socket.timeout:
		pass
	except Exception as e:
		print(f"[{node_id}] Error waiting for coordinator: {e}")
	
	finally:
		sock.settimeout(2.0)
	
	return None


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Smart node for distributed critical section')
	parser.add_argument('--id', type=str, help='Node identifier (e.g., Node_1)')
	parser.add_argument('--coord-ip', type=str, default=os.environ.get('COORD_IP', '127.0.0.1'), help='Coordinator IP address')
	parser.add_argument('--coord-port', type=int, default=int(os.environ.get('COORD_PORT', '50000')), help='Coordinator port')
	args = parser.parse_args()

	run_smart_node(node_id=args.id, coord_ip=args.coord_ip, coord_port=args.coord_port)
