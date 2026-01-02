from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, Tuple
import json
import socket
import time

@dataclass
class Message(ABC):
	node_id: str
	seq: int
	term: int
	type: str = field(init=False)

@dataclass
class REQ(Message):
	def __post_init__(self):
		self.type = "REQ"

@dataclass  
class GRANT(Message):
	lease_duration: float
	
	def __post_init__(self):
		self.type = "GRANT"

@dataclass
class REL(Message):
	def __post_init__(self):
		self.type = "REL"

@dataclass
class HB(Message):
	def __post_init__(self):
		self.type = "HB"

@dataclass
class ACK(Message):
	msg_type: str
	
	def __post_init__(self):
		self.type = "ACK"

@dataclass
class NACK(Message):
	msg_type: str
	reason: str
	
	def __post_init__(self):
		self.type = "NACK"

@dataclass
class SYNC(Message):
	state_snapshot: Dict[str, Any]
	
	def __post_init__(self):
		self.type = "SYNC"

@dataclass
class SYNC_ACK(Message):
	def __post_init__(self):
		self.type = "SYNC_ACK"

@dataclass
class COORD_HB(Message):
	def __post_init__(self):
		self.type = "COORD_HB"

@dataclass
class COORD_HB_ACK(Message):
	def __post_init__(self):
		self.type = "COORD_HB_ACK"

@dataclass
class ELECTION(Message):
	candidate_id: str
	proposed_term: int
	
	def __post_init__(self):
		self.type = "ELECTION"

@dataclass
class OK(Message):
	responder_id: str
	
	def __post_init__(self):
		self.type = "OK"

@dataclass
class COORDINATOR(Message):
	coord_id: str
	coord_addr: Tuple[str, int]
	
	def __post_init__(self):
		self.type = "COORDINATOR"

@dataclass
class STEP_DOWN(Message):
	def __post_init__(self):
		self.type = "STEP_DOWN"

MESSAGE_TYPES = {
	"REQ": REQ,
	"GRANT": GRANT,
	"REL": REL,
	"HB": HB,
	"ACK": ACK,
	"NACK": NACK,
	"SYNC": SYNC,
	"SYNC_ACK": SYNC_ACK,
	"COORD_HB": COORD_HB,
	"COORD_HB_ACK": COORD_HB_ACK,
	"ELECTION": ELECTION,
	"OK": OK,
	"COORDINATOR": COORDINATOR,
	"STEP_DOWN": STEP_DOWN
}

def serialize(msg: Message) -> bytes:
	try:
		data = {
			"type": msg.type,
			"node_id": msg.node_id,
			"seq": msg.seq,
			"term": msg.term
		}
		
		if hasattr(msg, 'lease_duration'):
			data["lease_duration"] = msg.lease_duration
		if hasattr(msg, 'msg_type'):
			data["msg_type"] = msg.msg_type
		if hasattr(msg, 'reason'):
			data["reason"] = msg.reason
		if hasattr(msg, 'state_snapshot'):
			data["state_snapshot"] = msg.state_snapshot
		if hasattr(msg, 'candidate_id'):
			data["candidate_id"] = msg.candidate_id
		if hasattr(msg, 'proposed_term'):
			data["proposed_term"] = msg.proposed_term
		if hasattr(msg, 'responder_id'):
			data["responder_id"] = msg.responder_id
		if hasattr(msg, 'coord_id'):
			data["coord_id"] = msg.coord_id
		if hasattr(msg, 'coord_addr'):
			data["coord_addr"] = msg.coord_addr
			
		return json.dumps(data).encode('utf-8')
	except Exception:
		return b''

def deserialize(data: bytes) -> Optional[Message]:
	try:
		obj = json.loads(data.decode('utf-8'))
		msg_type = obj.get("type")
		
		if msg_type not in MESSAGE_TYPES:
			return None
			
		msg_class = MESSAGE_TYPES[msg_type]
		
		kwargs = {
			"node_id": obj["node_id"],
			"seq": obj["seq"],
			"term": obj["term"]
		}
		
		if "lease_duration" in obj:
			kwargs["lease_duration"] = obj["lease_duration"]
		if "msg_type" in obj:
			kwargs["msg_type"] = obj["msg_type"]
		if "reason" in obj:
			kwargs["reason"] = obj["reason"]
		if "state_snapshot" in obj:
			kwargs["state_snapshot"] = obj["state_snapshot"]
		if "candidate_id" in obj:
			kwargs["candidate_id"] = obj["candidate_id"]
		if "proposed_term" in obj:
			kwargs["proposed_term"] = obj["proposed_term"]
		if "responder_id" in obj:
			kwargs["responder_id"] = obj["responder_id"]
		if "coord_id" in obj:
			kwargs["coord_id"] = obj["coord_id"]
		if "coord_addr" in obj:
			kwargs["coord_addr"] = tuple(obj["coord_addr"])
			
		return msg_class(**kwargs)
	except Exception:
		return None

class ReliableSender:
	def __init__(self):
		self.seq_counters: Dict[str, int] = {}
		self.last_seen_seq: Dict[str, int] = {}
	
	def send_reliable(self, sock: socket.socket, msg: Message, addr: Tuple[str, int], timeout: float = 0.5, retries: int = 3) -> Optional[Message]:
		dest_key = f"{addr[0]}:{addr[1]}"
		
		if dest_key not in self.seq_counters:
			self.seq_counters[dest_key] = 0
		
		self.seq_counters[dest_key] += 1
		msg.seq = self.seq_counters[dest_key]
		
		data = serialize(msg)
		if not data:
			print(f"DEBUG: Failed to serialize message {msg}")
			return None
		
		print(f"DEBUG: Sending {msg.type} to {addr} with seq={msg.seq}")
		
		orig_timeout = sock.gettimeout()
		sock.settimeout(timeout)
		
		try:
			for attempt in range(retries):
				try:
					sock.sendto(data, addr)
					print(f"DEBUG: Attempt {attempt+1}, sent message")
					
					while True:
						response_data, response_addr = sock.recvfrom(1024)
						print(f"DEBUG: Received response from {response_addr}")
						response = deserialize(response_data)
						
						if not response:
							print(f"DEBUG: Failed to deserialize response")
							continue
						
						print(f"DEBUG: Deserialized {response.type} with seq={response.seq}")
						
						if response_addr != addr:
							print(f"DEBUG: Response from wrong address {response_addr} != {addr}")
							continue
						
						sender_key = f"{response_addr[0]}:{response_addr[1]}"
						
						if sender_key in self.last_seen_seq:
							if response.seq <= self.last_seen_seq[sender_key]:
								print(f"DEBUG: Duplicate response seq={response.seq} <= {self.last_seen_seq[sender_key]}")
								continue
						
						self.last_seen_seq[sender_key] = response.seq
						
						if isinstance(response, (ACK, NACK, GRANT)) and response.seq == msg.seq:
							print(f"DEBUG: Got matching {response.type} response")
							return response
						else:
							print(f"DEBUG: Response doesn't match: type={type(response)}, seq={response.seq} vs {msg.seq}")
							
				except socket.timeout:
					print(f"DEBUG: Timeout on attempt {attempt+1}")
					if attempt == retries - 1:
						break
					continue
				except Exception as e:
					print(f"DEBUG: Exception in send_reliable: {e}")
					break
		finally:
			sock.settimeout(orig_timeout)
		
		print(f"DEBUG: send_reliable returning None after all attempts")
		return None

if __name__ == "__main__":
	print("Testing protocol serialization...")
	
	test_messages = [
		REQ(node_id="Node_1", seq=1, term=1),
		GRANT(node_id="coord", seq=1, term=1, lease_duration=5.0),
		REL(node_id="Node_1", seq=2, term=1),
		HB(node_id="Node_1", seq=3, term=1),
		ACK(node_id="coord", seq=1, term=1, msg_type="REQ"),
		NACK(node_id="coord", seq=1, term=1, msg_type="REQ", reason="invalid"),
		SYNC(node_id="coord", seq=1, term=1, state_snapshot={"holder": "Node_1", "queue": []}),
		SYNC_ACK(node_id="backup", seq=1, term=1),
		COORD_HB(node_id="coord", seq=1, term=1),
		COORD_HB_ACK(node_id="backup", seq=1, term=1),
		ELECTION(node_id="Node_99", seq=1, term=1, candidate_id="Node_99", proposed_term=2),
		OK(node_id="Node_100", seq=1, term=2, responder_id="Node_100"),
		COORDINATOR(node_id="Node_100", seq=1, term=2, coord_id="Node_100", coord_addr=("192.168.1.100", 50000)),
		STEP_DOWN(node_id="coord", seq=1, term=1)
	]
	
	success_count = 0
	for i, msg in enumerate(test_messages):
		data = serialize(msg)
		if not data:
			print(f"Test {i+1}: Serialization failed for {type(msg).__name__}")
			continue
			
		deserialized = deserialize(data)
		if not deserialized:
			print(f"Test {i+1}: Deserialization failed for {type(msg).__name__}")
			continue
			
		if type(deserialized) != type(msg):
			print(f"Test {i+1}: Type mismatch for {type(msg).__name__}")
			continue
			
		success_count += 1
		print(f"Test {i+1}: {type(msg).__name__} - OK")
	
	print(f"\nPassed {success_count}/{len(test_messages)} tests")