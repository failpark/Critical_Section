import socket
import threading
import time
import argparse
import os
from collections import deque
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from protocol import REQ, GRANT, REL, HB, ACK, NACK, SYNC, SYNC_ACK, COORD_HB, COORD_HB_ACK, COORDINATOR, ELECTION, OK, STEP_DOWN, serialize, deserialize, Message
from failures import MessageDropWrapper, NodeFailureSimulator


@dataclass
class CoordinatorState:
	term: int
	role: str
	current_holder: Optional[str]
	lease_expiry: float
	queue: deque = field(default_factory=deque)
	known_nodes: set = field(default_factory=set)
	known_node_addrs: Dict[str, Tuple[str, int]] = field(default_factory=dict)  # Track client addresses for broadcast
	last_seen_seq: Dict[str, int] = field(default_factory=dict)
	backup_addrs: List[Tuple[str, int]] = field(default_factory=list)
	backup_status: Dict[str, str] = field(default_factory=dict)
	suspect_count: Dict[str, int] = field(default_factory=dict)  # Track consecutive failures
	dead_peers: set = field(default_factory=set)  # Peers excluded from quorum
	sync_seq_counter: int = 0
	last_primary_hb: float = 0.0
	primary_suspected_failed: bool = False
	heartbeat_seq_counter: int = 0
	election_in_progress: bool = False
	election_start_time: float = 0.0
	awaiting_coordinator: bool = False
	awaiting_coordinator_since: float = 0.0
	candidate_peers: List[str] = field(default_factory=list)
	election_seq_counter: int = 0
	pending_requests: deque = field(default_factory=deque)
	quorum_ack_count: int = 0
	last_quorum_check: float = 0.0
	quorum_lost_start: Optional[float] = None
	current_primary_addr: Optional[Tuple[str, int]] = None
	last_coordinator_announcement: float = 0.0  # Track last coordinator broadcast time


@dataclass
class StateSnapshot:
	term: int
	current_holder: Optional[str]
	lease_expiry: float
	queue_list: List[Tuple[str, Tuple[str, int], int]]
	known_nodes_list: List[str]
	last_seen_seq: Dict[str, int]
	
	def to_dict(self) -> Dict[str, Any]:
		return {
			'term': self.term,
			'current_holder': self.current_holder,
			'lease_expiry': self.lease_expiry,
			'queue_list': self.queue_list,
			'known_nodes_list': self.known_nodes_list,
			'last_seen_seq': self.last_seen_seq
		}
	
	@classmethod
	def from_dict(cls, data: Dict[str, Any]) -> 'StateSnapshot':
		# Convert queue_list entries: [node_id, [host, port], seq] -> (node_id, (host, port), seq)
		# JSON deserialization converts tuples to lists
		queue_list = [
			(entry[0], tuple(entry[1]), entry[2])
			for entry in data['queue_list']
		]
		return cls(
			term=data['term'],
			current_holder=data['current_holder'],
			lease_expiry=data['lease_expiry'],
			queue_list=queue_list,
			known_nodes_list=data['known_nodes_list'],
			last_seen_seq=data['last_seen_seq']
		)


class Coordinator:
    """
    Core coordinator implementation for centralized mutual exclusion.
    Extracted from Jupyter notebook for better modularity.
    """
    
    def __init__(self, host='0.0.0.0', client_port=50000, coord_port=50001, lease_duration=5.0,
                 role='PRIMARY', node_id=100, peers=None, drop_rate=0.0, failure_prob=0.0, permanent_failure_prob=0.10):
        self.host = host
        self.client_port = client_port
        self.coord_port = coord_port
        self.lease_duration = lease_duration

        # Failure simulation parameters
        self.drop_rate = drop_rate
        self.failure_prob = failure_prob
        self.permanent_failure_prob = permanent_failure_prob

        # State variables (protected by state_lock)
        self.state_lock = threading.Lock()
        self.running = True
        self.state = CoordinatorState(
            term=0,
            role=role,
            current_holder=None,
            lease_expiry=0.0,
            queue=deque(),
            known_nodes=set(),
            last_seen_seq={},
            backup_addrs=peers or []
        )
        self.node_id = f"Node_{node_id}"

        # Initialize failure simulator if configured
        self.failure_sim = None
        if failure_prob > 0:
            self.failure_sim = NodeFailureSimulator(failure_prob, permanent_failure_prob, self.node_id)
            print(f"--- {self.node_id} failure probability: {failure_prob*100:.2f}% per check ---")

        # Network
        self.client_sock = None
        self.coord_sock = None
        self.hb_sock = None  # Separate socket for heartbeat sending to avoid race conditions
        self.client_thread = None
        self.coord_thread = None
        self.heartbeat_thread = None
    
    def start(self):
        """Start the coordinator server."""
        self.running = True
        with self.state_lock:
            self.state.queue.clear()
            self.state.current_holder = None
            self.state.known_nodes.clear()
        
        self.client_thread = threading.Thread(target=self._client_server, daemon=True)
        self.coord_thread = threading.Thread(target=self._coord_server, daemon=True)
        self.client_thread.start()
        self.coord_thread.start()
        
        if self.state.role == 'PRIMARY' and self.state.backup_addrs:
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_sender, daemon=True)
            self.heartbeat_thread.start()
        
        print(f"running as {self.state.role}")
        print(f"Client UDP Server running on {self.client_port}")
        print(f"Coordinator UDP Server running on {self.coord_port}")

    def _get_resolvable_hostname(self) -> str:
        """Get a hostname that can be resolved by other containers."""
        # Try HOSTNAME environment variable first
        hostname = os.environ.get('HOSTNAME')
        if hostname:
            return hostname

        # Derive from node_id (Docker Compose naming convention)
        # 100 -> primary, 99 -> backup1, 98 -> backup2
        if self.node_id == 100:
            return 'primary'
        elif self.node_id == 99:
            return 'backup1'
        elif self.node_id == 98:
            return 'backup2'

        # Fallback to socket.gethostname()
        return socket.gethostname()
    
    def stop(self):
        """Stop the coordinator server."""
        if not self.running:
            return
            
        print("Shutting down system...")
        self.running = False
        
        # Send internal stop signals to wake up server threads
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.sendto(b"INTERNAL_STOP Node_sys", ('127.0.0.1', self.client_port))
            temp_sock.sendto(b"INTERNAL_STOP Node_sys", ('127.0.0.1', self.coord_port))
            temp_sock.close()
        except Exception as e:
            print(f"Failed to send wake-up packet: {e}")
        
        time.sleep(0.1)
        for sock in [self.client_sock, self.coord_sock]:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    def get_state(self):
        """Get current coordinator state (thread-safe)."""
        with self.state_lock:
            current_time = time.time()
            return {
                'current_holder': self.state.current_holder,
                'lease_expiry': self.state.lease_expiry,
                'lease_remaining': max(0, self.state.lease_expiry - current_time) if self.state.current_holder else 0,
                'queue': list(self.state.queue),
                'queue_length': len(self.state.queue),
                'known_nodes': set(self.state.known_nodes),
                'running': self.running,
                'role': self.state.role,
                'backup_status': self.state.backup_status.copy(),
                'dead_peers': set(self.state.dead_peers),
                'term': self.state.term,
                'primary_suspected_failed': self.state.primary_suspected_failed,
                'last_primary_hb': self.state.last_primary_hb
            }
    
    def _client_server(self):
        """Client UDP server loop - handles REQ/REL/HB from nodes."""
        self.client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.client_sock.bind((self.host, self.client_port))
        except OSError:
            print(f"ERROR: Client Port {self.client_port} still in use! Please restart kernel.")
            self.running = False
            return

        # Wrap socket for message dropping if configured
        if self.drop_rate > 0:
            self.client_sock = MessageDropWrapper(self.client_sock, self.drop_rate)
            print(f"--- {self.node_id} client socket message drop rate: {self.drop_rate*100:.0f}% ---")

        while self.running:
            # Check for node failure simulation
            if self.failure_sim:
                if self.failure_sim.is_failed():
                    self.failure_sim.maybe_recover()
                    time.sleep(0.5)
                    continue  # Skip all processing while failed

                # Check if failure should be triggered
                self.failure_sim.check_for_failure()
            try:
                # Automatic grants on lease expiry (PRIMARY only)
                if self.state.role == 'PRIMARY':
                    with self.state_lock:
                        if self.state.current_holder and time.time() > self.state.lease_expiry and self.state.queue:
                            next_id, next_addr, next_seq = self.state.queue.popleft()
                            self.state.current_holder = next_id
                            self.state.lease_expiry = time.time() + self.lease_duration
                            ack_count = self._sync_to_backups()
                            grant_msg = GRANT(node_id=next_id, seq=next_seq, term=self.state.term, lease_duration=self.lease_duration)
                            self.client_sock.sendto(serialize(grant_msg), next_addr)
                
                try:
                    data, addr = self.client_sock.recvfrom(1024)
                except socket.timeout:
                    continue
                except OSError:
                    break
                
                if data.startswith(b"INTERNAL_STOP"):
                    print("Client stop signal received.")
                    break
                
                msg = deserialize(data)
                if not msg or not isinstance(msg, Message):
                    continue
                
                with self.state_lock:
                    self.state.known_nodes.add(msg.node_id)
                    self.state.known_node_addrs[msg.node_id] = addr

                    # Validate term first
                    if not self._validate_term(msg, addr, self.client_sock):
                        continue
                    
                    # Check for duplicates
                    if msg.node_id in self.state.last_seen_seq and msg.seq <= self.state.last_seen_seq[msg.node_id]:
                        nack_msg = NACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type=msg.type, reason="duplicate")
                        self.client_sock.sendto(serialize(nack_msg), addr)
                        continue
                    
                    self.state.last_seen_seq[msg.node_id] = msg.seq
                    
                    # Handle client messages based on role
                    if self.state.role == 'PRIMARY':
                        if isinstance(msg, REQ):
                            self._handle_request(msg.node_id, addr, msg.seq)
                        elif isinstance(msg, HB):
                            self._handle_heartbeat(msg.node_id)
                            ack_msg = ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type="HB")
                            self.client_sock.sendto(serialize(ack_msg), addr)
                        elif isinstance(msg, REL):
                            self._handle_release(msg.node_id)
                            ack_msg = ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type="REL")
                            self.client_sock.sendto(serialize(ack_msg), addr)
                    elif self.state.role == 'CANDIDATE':
                        # Election freeze: buffer REQ messages, allow HB/REL
                        if isinstance(msg, REQ):
                            self.state.pending_requests.append((msg.node_id, addr, msg.seq, msg.term))
                            ack_msg = ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type="REQ")
                            self.client_sock.sendto(serialize(ack_msg), addr)
                            print(f"Buffered REQ from {msg.node_id} during election")
                        elif isinstance(msg, HB):
                            ack_msg = ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type="HB")
                            self.client_sock.sendto(serialize(ack_msg), addr)
                        elif isinstance(msg, REL):
                            ack_msg = ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type="REL")
                            self.client_sock.sendto(serialize(ack_msg), addr)
                    else:  # BACKUP
                        # Send NACK redirect for all client messages
                        if self.state.current_primary_addr:
                            primary_addr = self.state.current_primary_addr
                            # Try to resolve hostname to IP for better connectivity
                            try:
                                primary_ip = socket.gethostbyname(primary_addr[0])
                                reason = f"redirect_to_{primary_ip}:{primary_addr[1]}"
                            except socket.gaierror:
                                # Fallback to hostname if DNS resolution fails
                                reason = f"redirect_to_{primary_addr[0]}:{primary_addr[1]}"
                            nack_msg = NACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type=msg.type, reason=reason)
                        elif self.state.election_in_progress or self.state.awaiting_coordinator:
                            # Election in progress - tell client to retry later
                            nack_msg = NACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type=msg.type, reason="election_in_progress")
                        else:
                            # No primary known and no election - shouldn't happen but handle gracefully
                            nack_msg = NACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type=msg.type, reason="no_primary_available")
                        self.client_sock.sendto(serialize(nack_msg), addr)
                        
            except Exception as e:
                if self.running:
                    print(f"Client Server Error: {e}")
        
        if self.client_sock:
            self.client_sock.close()
        print("Client Server Thread ended.")
    
    def _coord_server(self):
        """Coordinator UDP server loop - handles SYNC/COORD_HB between coordinators."""
        self.coord_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.coord_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.coord_sock.bind((self.host, self.coord_port))
        except OSError:
            print(f"ERROR: Coordinator Port {self.coord_port} still in use! Please restart kernel.")
            self.running = False
            return

        # Wrap socket for message dropping if configured
        if self.drop_rate > 0:
            self.coord_sock = MessageDropWrapper(self.coord_sock, self.drop_rate)
            print(f"--- {self.node_id} coord socket message drop rate: {self.drop_rate*100:.0f}% ---")

        # Set timeout to allow periodic checks (election timeout, primary failure, etc.)
        self.coord_sock.settimeout(0.5)

        while self.running:
            # Check for node failure simulation
            if self.failure_sim:
                if self.failure_sim.is_failed():
                    self.failure_sim.maybe_recover()
                    time.sleep(0.5)
                    continue  # Skip all processing while failed

                # Check if failure should be triggered
                self.failure_sim.check_for_failure()
            try:
                # Check for primary failure (BACKUP only)
                if self.state.role == 'BACKUP':
                    self._check_primary_failure()
                
                # Check for election timeout (CANDIDATE only)
                if self.state.role == 'CANDIDATE':
                    with self.state_lock:
                        self._check_election_timeout()

                # Check if awaiting COORDINATOR but it never arrived
                if self.state.awaiting_coordinator:
                    with self.state_lock:
                        if time.time() - self.state.awaiting_coordinator_since > 3.0:
                            print("COORDINATOR timeout - higher-ID node may have failed, restarting election")
                            self.state.awaiting_coordinator = False
                            self._start_election()

                try:
                    data, addr = self.coord_sock.recvfrom(1024)
                except socket.timeout:
                    continue
                except OSError:
                    break
                
                if data.startswith(b"INTERNAL_STOP"):
                    print("Coordinator stop signal received.")
                    break
                
                msg = deserialize(data)
                if not msg or not isinstance(msg, Message):
                    continue
                
                with self.state_lock:
                    if not self._validate_term(msg, addr, self.coord_sock):
                        continue
                    
                    if isinstance(msg, SYNC):
                        if self.state.role == 'BACKUP':
                            self._apply_state_snapshot(msg.state_snapshot)
                            ack_msg = SYNC_ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term)
                            self.coord_sock.sendto(serialize(ack_msg), addr)
                            print(f"Applied SYNC: holder={self.state.current_holder}, queue_len={len(self.state.queue)}, term={self.state.term}")
                    
                    elif isinstance(msg, COORD_HB):
                        self.state.last_primary_hb = time.time()
                        self.state.primary_suspected_failed = False
                        
                        ack_msg = COORD_HB_ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term)
                        self.coord_sock.sendto(serialize(ack_msg), addr)
                    
                    elif isinstance(msg, ELECTION):
                        my_id = int(self.node_id.split('_')[1]) if '_' in self.node_id else 0
                        candidate_id = int(msg.candidate_id.split('_')[1]) if '_' in msg.candidate_id else 0
                        
                        if candidate_id < my_id:
                            ok_msg = OK(node_id=self.node_id, seq=msg.seq, term=self.state.term, responder_id=self.node_id)
                            self.coord_sock.sendto(serialize(ok_msg), addr)
                            print(f"Sent OK to {msg.candidate_id} from {addr}")
                            
                            if not self.state.election_in_progress:
                                print(f"Starting election triggered by {msg.candidate_id}")
                                self._start_election()
                    
                    elif isinstance(msg, OK):
                        if self.state.role == 'CANDIDATE':
                            self.state.role = 'BACKUP'
                            self.state.election_in_progress = False
                            self.state.awaiting_coordinator = True
                            self.state.awaiting_coordinator_since = time.time()
                            print(f"Received OK from {msg.responder_id}, stepping down to await COORDINATOR")
                    
                    elif isinstance(msg, COORDINATOR):
                        if msg.term >= self.state.term:
                            if self.state.role in ['CANDIDATE', 'PRIMARY']:
                                print(f"Stepping down to BACKUP: received COORDINATOR from {msg.coord_id}")

                            self.state.role = 'BACKUP'
                            self.state.election_in_progress = False
                            self.state.awaiting_coordinator = False
                            self.state.primary_suspected_failed = False
                            self.state.last_primary_hb = time.time()
                            self.state.current_primary_addr = msg.coord_addr

                            # Discard pending requests when becoming BACKUP
                            if self.state.pending_requests:
                                discarded_count = len(self.state.pending_requests)
                                self.state.pending_requests.clear()
                                print(f"Discarded {discarded_count} pending requests, new primary will handle")

                            print(f"New coordinator: {msg.coord_id} at {msg.coord_addr}")
                        else:
                            print(f"Rejected COORDINATOR with stale term {msg.term} < {self.state.term}")
                    
                    elif isinstance(msg, STEP_DOWN):
                        print(f"Received STEP_DOWN from {msg.node_id}, term={msg.term}")
                        # Primary stepped down, trigger election if we are BACKUP
                        if self.state.role == 'BACKUP' and msg.term >= self.state.term:
                            if not self.state.election_in_progress:
                                print("Primary stepped down, starting election")
                                self._start_election()
                        
            except Exception as e:
                if self.running:
                    print(f"Coordinator Server Error: {e}")
        
        if self.coord_sock:
            self.coord_sock.close()
        print("Coordinator Server Thread ended.")
    
    def _handle_request(self, node_id, addr, seq):
        """Handle REQ message - must be called within state_lock."""
        if self.state.current_holder is None:
            # Grant immediately
            self.state.current_holder = node_id
            self.state.lease_expiry = time.time() + self.lease_duration
            ack_count = self._sync_to_backups()
            grant_msg = GRANT(node_id=node_id, seq=seq, term=self.state.term, lease_duration=self.lease_duration)
            self.client_sock.sendto(serialize(grant_msg), addr)
        elif self.state.current_holder == node_id:
            # Renew lease for same node
            self.state.lease_expiry = time.time() + self.lease_duration
            ack_count = self._sync_to_backups()
            grant_msg = GRANT(node_id=node_id, seq=seq, term=self.state.term, lease_duration=self.lease_duration)
            self.client_sock.sendto(serialize(grant_msg), addr)
        else:
            # Queue the request if not already queued
            if node_id not in [x[0] for x in self.state.queue]:
                self.state.queue.append((node_id, addr, seq))
            # Send ACK only for queued requests
            ack_msg = ACK(node_id=self.node_id, seq=seq, term=self.state.term, msg_type="REQ")
            self.client_sock.sendto(serialize(ack_msg), addr)
    
    def _handle_heartbeat(self, node_id):
        """Handle HB message - must be called within state_lock."""
        if node_id == self.state.current_holder:
            self.state.lease_expiry = time.time() + self.lease_duration
            ack_count = self._sync_to_backups()
    
    def _handle_release(self, node_id):
        """Handle REL message - must be called within state_lock."""
        if node_id == self.state.current_holder:
            if self.state.queue:
                # Grant to next in queue
                next_id, next_addr, next_seq = self.state.queue.popleft()
                self.state.current_holder = next_id
                self.state.lease_expiry = time.time() + self.lease_duration
                ack_count = self._sync_to_backups()
                grant_msg = GRANT(node_id=next_id, seq=next_seq, term=self.state.term, lease_duration=self.lease_duration)
                self.client_sock.sendto(serialize(grant_msg), next_addr)
            else:
                # No one waiting
                self.state.current_holder = None
                ack_count = self._sync_to_backups()
    
    def _sync_to_backups(self) -> int:
        """Send state synchronization to backup coordinators - must be called within state_lock."""
        if not self.state.backup_addrs or self.state.role != 'PRIMARY':
            return 0
            
        self.state.sync_seq_counter += 1
        
        snapshot = StateSnapshot(
            term=self.state.term,
            current_holder=self.state.current_holder,
            lease_expiry=self.state.lease_expiry,
            queue_list=list(self.state.queue),
            known_nodes_list=list(self.state.known_nodes),
            last_seen_seq=self.state.last_seen_seq.copy()
        )
        
        sync_msg = SYNC(node_id=self.node_id, seq=self.state.sync_seq_counter, term=self.state.term, state_snapshot=snapshot.to_dict())
        sync_data = serialize(sync_msg)
        
        ack_count = 0
        for backup_addr in self.state.backup_addrs:
            backup_key = f"{backup_addr[0]}:{backup_addr[1]}"

            # Skip dead peers - no point trying to sync
            if backup_key in self.state.dead_peers:
                continue

            try:
                coord_addr = backup_addr  # Port already correct from config

                self.coord_sock.settimeout(1.0)
                self.coord_sock.sendto(sync_data, coord_addr)
                
                try:
                    response_data, response_addr = self.coord_sock.recvfrom(1024)
                    response = deserialize(response_data)

                    # Resolve hostname to IP for comparison
                    try:
                        expected_ip = socket.gethostbyname(backup_addr[0])
                    except socket.gaierror:
                        expected_ip = backup_addr[0]

                    if (isinstance(response, SYNC_ACK) and
                        response.seq == self.state.sync_seq_counter and
                        response_addr[0] == expected_ip):
                        ack_count += 1
                        self.state.backup_status[backup_key] = "healthy"
                        print(f"SYNC_ACK received from backup {backup_addr}")
                    else:
                        self.state.backup_status[backup_key] = "suspect"
                        print(f"Invalid SYNC_ACK from backup {backup_addr}")
                        
                except socket.timeout:
                    self.state.backup_status[backup_key] = "suspect"
                    print(f"SYNC timeout for backup {backup_addr}")
                    
            except Exception as e:
                backup_key = f"{backup_addr[0]}:{backup_addr[1]}"
                self.state.backup_status[backup_key] = "suspect"
                print(f"Failed to sync to backup {backup_addr}: {e}")
        
        self.coord_sock.settimeout(None)

        # Calculate quorum excluding dead peers (same as heartbeat quorum)
        alive_peers = sum(1 for addr in self.state.backup_addrs
                         if f"{addr[0]}:{addr[1]}" not in self.state.dead_peers)
        total_alive = alive_peers + 1  # +1 for self
        majority_needed = (total_alive + 1) // 2
        if ack_count < majority_needed:
            print(f"WARNING: SYNC majority not reached ({ack_count}/{alive_peers}, need {majority_needed})")

        return ack_count
    
    def _apply_state_snapshot(self, snapshot_dict: Dict[str, Any]):
        """Apply state snapshot from primary - must be called within state_lock."""
        snapshot = StateSnapshot.from_dict(snapshot_dict)
        
        self.state.term = snapshot.term
        self.state.current_holder = snapshot.current_holder
        self.state.lease_expiry = snapshot.lease_expiry
        self.state.queue = deque(snapshot.queue_list)
        self.state.known_nodes = set(snapshot.known_nodes_list)
        self.state.last_seen_seq = snapshot.last_seen_seq.copy()

    def _heartbeat_sender(self):
        """Send COORD_HB to all backup coordinators every 1 second (PRIMARY only)."""
        print("Heartbeat sender thread started")

        # Create separate socket for heartbeat communication
        if not self.hb_sock:
            self.hb_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.hb_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Wrap for message dropping if configured
            if self.drop_rate > 0:
                self.hb_sock = MessageDropWrapper(self.hb_sock, self.drop_rate)

        while self.running and self.state.role == 'PRIMARY':
            # Check for node failure simulation
            if self.failure_sim:
                if self.failure_sim.is_failed():
                    self.failure_sim.maybe_recover()
                    time.sleep(0.5)
                    continue  # Skip all processing while failed

                # Check if failure should be triggered
                self.failure_sim.check_for_failure()

            time.sleep(1.0)

            if not self.running:
                break
                
            with self.state_lock:
                if not self.state.backup_addrs:
                    continue
                    
                self.state.heartbeat_seq_counter += 1
                self.state.quorum_ack_count = 0  # Reset counter for this heartbeat round
                self.state.last_quorum_check = time.time()
                hb_msg = COORD_HB(node_id=self.node_id, seq=self.state.heartbeat_seq_counter, term=self.state.term)
                hb_data = serialize(hb_msg)
                
                if not hb_data:
                    continue
                
                for backup_addr in self.state.backup_addrs:
                    try:
                        coord_addr = backup_addr  # Port already correct from config
                        backup_key = f"{backup_addr[0]}:{backup_addr[1]}"

                        self.hb_sock.settimeout(0.5)
                        self.hb_sock.sendto(hb_data, coord_addr)

                        try:
                            response_data, response_addr = self.hb_sock.recvfrom(1024)
                            response = deserialize(response_data)

                            # Resolve hostname to IP for comparison
                            try:
                                expected_ip = socket.gethostbyname(backup_addr[0])
                            except socket.gaierror:
                                expected_ip = backup_addr[0]

                            if (isinstance(response, COORD_HB_ACK) and
                                response.seq == self.state.heartbeat_seq_counter and
                                response_addr[0] == expected_ip):
                                self.state.backup_status[backup_key] = "healthy"
                                self.state.suspect_count[backup_key] = 0  # Reset on success
                                self.state.quorum_ack_count += 1
                            else:
                                self.state.backup_status[backup_key] = "suspect"
                                self.state.suspect_count[backup_key] = self.state.suspect_count.get(backup_key, 0) + 1
                                
                        except socket.timeout:
                            self.state.backup_status[backup_key] = "suspect"
                            self.state.suspect_count[backup_key] = self.state.suspect_count.get(backup_key, 0) + 1

                    except socket.gaierror as e:
                        # DNS resolution failed - peer is unreachable
                        backup_key = f"{backup_addr[0]}:{backup_addr[1]}"
                        self.state.backup_status[backup_key] = "unreachable"
                        self.state.suspect_count[backup_key] = self.state.suspect_count.get(backup_key, 0) + 1
                    except Exception as e:
                        backup_key = f"{backup_addr[0]}:{backup_addr[1]}"
                        self.state.backup_status[backup_key] = "suspect"
                        self.state.suspect_count[backup_key] = self.state.suspect_count.get(backup_key, 0) + 1

                    # Check if peer should be marked as dead (3+ consecutive failures)
                    if self.state.suspect_count.get(backup_key, 0) >= 3 and backup_key not in self.state.dead_peers:
                        self.state.dead_peers.add(backup_key)
                        print(f"[{self.node_id}] Marking {backup_key} as DEAD after {self.state.suspect_count[backup_key]} failures")

                self.hb_sock.settimeout(None)
                
                # Check quorum after heartbeat round (including self)
                # Count only alive peers for quorum (exclude unreachable and dead)
                alive_peers = sum(1 for addr in self.state.backup_addrs
                                 if f"{addr[0]}:{addr[1]}" not in self.state.dead_peers and
                                    self.state.backup_status.get(f"{addr[0]}:{addr[1]}") != "unreachable")
                total_alive = alive_peers + 1  # +1 for self
                majority_needed = (total_alive + 1) // 2
                quorum_achieved = (self.state.quorum_ack_count + 1) >= majority_needed  # +1 for self
                
                if not quorum_achieved:
                    current_time = time.time()
                    if self.state.quorum_lost_start is None:
                        self.state.quorum_lost_start = current_time
                        print(f"Quorum lost: {self.state.quorum_ack_count + 1}/{total_alive} (need {majority_needed})")
                    elif current_time - self.state.quorum_lost_start > 3.0:
                        print(f"Quorum lost for 3+ seconds, stepping down")
                        self._step_down()
                        break
                else:
                    if self.state.quorum_lost_start is not None:
                        print("Quorum restored")
                    self.state.quorum_lost_start = None

                # Periodic coordinator re-announcement (every 10 seconds)
                current_time = time.time()
                if current_time - self.state.last_coordinator_announcement >= 10.0:
                    self._broadcast_coordinator_announcement()

        # Cleanup heartbeat socket
        if self.hb_sock:
            try:
                self.hb_sock.close()
            except:
                pass
            self.hb_sock = None

        print("Heartbeat sender thread ended")

    def _step_down(self):
        """Step down from PRIMARY role and broadcast STEP_DOWN - must be called within state_lock."""
        if self.state.role != 'PRIMARY':
            return
            
        print(f"Stepping down from PRIMARY role, term={self.state.term}")
        
        # Broadcast STEP_DOWN message
        step_down_msg = STEP_DOWN(node_id=self.node_id, seq=self.state.heartbeat_seq_counter + 1, term=self.state.term)
        step_down_data = serialize(step_down_msg)
        
        if step_down_data:
            try:
                broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                
                # Broadcast to all coordinators and nodes
                for peer_addr in self.state.backup_addrs:
                    broadcast_sock.sendto(step_down_data, peer_addr)
                
                broadcast_sock.sendto(step_down_data, ('255.255.255.255', self.client_port))
                broadcast_sock.sendto(step_down_data, ('127.0.0.1', self.client_port))
                broadcast_sock.close()
                print("Broadcast STEP_DOWN message")
                
            except Exception as e:
                print(f"Failed to broadcast STEP_DOWN: {e}")
        
        # Transition to BACKUP role
        self.state.role = 'BACKUP'
        self.state.quorum_lost_start = None
        
        # Clear pending requests
        if self.state.pending_requests:
            discarded_count = len(self.state.pending_requests)
            self.state.pending_requests.clear()
            print(f"Discarded {discarded_count} pending requests during step-down")

    def _validate_term(self, msg: Message, addr: Tuple[str, int], sock: socket.socket) -> bool:
        """Validate message term and handle term updates - must be called within state_lock."""
        if msg.term < self.state.term:
            nack_msg = NACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type=msg.type, reason="STALE_TERM")
            sock.sendto(serialize(nack_msg), addr)
            return False
        elif msg.term > self.state.term:
            old_term = self.state.term
            self.state.term = msg.term
            if self.state.role == 'PRIMARY':
                print(f"Stepping down: received term {msg.term} > local {old_term}")
                self.state.role = 'BACKUP'
        return True

    def _get_higher_id_peers(self) -> List[Tuple[str, int]]:
        """Get list of peer coordinators with higher IDs than this node."""
        my_id = int(self.node_id.split('_')[1]) if '_' in self.node_id else 0
        higher_peers = []
        
        for peer_addr in self.state.backup_addrs:
            for i in range(98, 105):
                if i > my_id:
                    higher_peers.append(peer_addr)
        return higher_peers

    def _check_primary_failure(self):
        """Check for primary failure and trigger election (BACKUP only) - must be called within coord_server loop."""
        if self.state.role != 'BACKUP':
            return
            
        current_time = time.time()
        
        with self.state_lock:
            if self.state.last_primary_hb > 0 and current_time - self.state.last_primary_hb > 2.5:
                if not self.state.primary_suspected_failed and not self.state.election_in_progress:
                    self.state.primary_suspected_failed = True
                    print("primary failure detected")
                    self._start_election()

    def _start_election(self):
        """Start Bully election - must be called within state_lock."""
        self.state.role = 'CANDIDATE'
        self.state.term += 1
        self.state.election_in_progress = True
        self.state.election_start_time = time.time()
        self.state.election_seq_counter += 1
        
        my_id = int(self.node_id.split('_')[1]) if '_' in self.node_id else 0
        higher_peers = []
        
        # In this implementation, we need to know which peer addresses correspond to higher IDs
        # For the test case with ID 99 trying to contact ID 100:
        # - If backup_addrs contains the primary address, we try to contact it
        # - If no response, we win the election
        for peer_addr in self.state.backup_addrs:
            try:
                # Port already correct from config
                coord_addr = peer_addr

                # For now, assume any peer might have a higher ID
                # In the test scenario, this will be the primary coordinator
                higher_peers.append(coord_addr)
                    
            except Exception as e:
                print(f"Error parsing peer {peer_addr}: {e}")
        
        if not higher_peers:
            print(f"No higher peers found, becoming PRIMARY, term={self.state.term}")
            self._become_primary()
            return
        
        election_msg = ELECTION(
            node_id=self.node_id,
            seq=self.state.election_seq_counter,
            term=self.state.term,
            candidate_id=self.node_id,
            proposed_term=self.state.term
        )
        
        election_data = serialize(election_msg)
        if election_data:
            for coord_addr in higher_peers:
                try:
                    self.coord_sock.sendto(election_data, coord_addr)
                    print(f"Sent ELECTION to {coord_addr}")
                except Exception as e:
                    print(f"Failed to send ELECTION to {coord_addr}: {e}")
        
        print(f"Started election with term {self.state.term}")

    def _check_election_timeout(self):
        """Check if election timeout expired - must be called within state_lock."""
        if not self.state.election_in_progress or self.state.role != 'CANDIDATE':
            return
            
        current_time = time.time()
        if current_time - self.state.election_start_time > 2.0:
            print(f"Election timeout - became PRIMARY, term={self.state.term}")
            self._become_primary()

    def _become_primary(self):
        """Transition to PRIMARY role with state takeover and pending request processing - must be called within state_lock."""
        self.state.role = 'PRIMARY'
        self.state.election_in_progress = False
        
        # Check for expired leases and auto-grant to queue head
        current_time = time.time()
        if self.state.current_holder and current_time > self.state.lease_expiry:
            print(f"Clearing expired lease for {self.state.current_holder}")
            self.state.current_holder = None
            if self.state.queue:
                next_id, next_addr, next_seq = self.state.queue.popleft()
                self.state.current_holder = next_id
                self.state.lease_expiry = current_time + self.lease_duration
                grant_msg = GRANT(node_id=next_id, seq=next_seq, term=self.state.term, lease_duration=self.lease_duration)
                self.client_sock.sendto(serialize(grant_msg), next_addr)
                print(f"Auto-granted to {next_id} from queue after takeover")
        
        # Process pending requests in FIFO order
        while self.state.pending_requests:
            pending_node_id, pending_addr, pending_seq, pending_term = self.state.pending_requests.popleft()
            # Only process if term is still valid
            if pending_term <= self.state.term:
                self._handle_request(pending_node_id, pending_addr, pending_seq)
                print(f"Processed pending REQ from {pending_node_id}")
            else:
                print(f"Discarded stale pending REQ from {pending_node_id} (term {pending_term} > {self.state.term})")
        
        self._broadcast_coordinator_announcement()
        
        # Start heartbeat thread if we have backup coordinators
        if self.state.backup_addrs and (not self.heartbeat_thread or not self.heartbeat_thread.is_alive()):
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_sender, daemon=True)
            self.heartbeat_thread.start()
            print("Started new heartbeat sender thread after becoming PRIMARY")

    def _broadcast_coordinator_announcement(self):
        """Broadcast COORDINATOR message after winning election - must be called within state_lock."""
        self.state.last_coordinator_announcement = time.time()

        coord_msg = COORDINATOR(
            node_id=self.node_id,
            seq=self.state.election_seq_counter,
            term=self.state.term,
            coord_id=self.node_id,
            coord_addr=(self._get_resolvable_hostname(), self.client_port)
        )
        
        broadcast_data = serialize(coord_msg)
        if broadcast_data:
            try:
                broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                
                broadcast_sock.sendto(broadcast_data, ('255.255.255.255', self.client_port))
                broadcast_sock.sendto(broadcast_data, ('127.0.0.1', self.client_port))

                # Unicast to all known client nodes (cross-subnet support)
                for node_id, node_addr in self.state.known_node_addrs.items():
                    try:
                        broadcast_sock.sendto(broadcast_data, node_addr)
                        print(f"[{self.node_id}] Sent COORDINATOR to known node {node_id} at {node_addr}")
                    except Exception as e:
                        print(f"[{self.node_id}] Failed to send to {node_id}: {e}")

                for peer_addr in self.state.backup_addrs:
                    try:
                        broadcast_sock.sendto(broadcast_data, peer_addr)
                    except socket.gaierror as e:
                        print(f"Skipping unreachable peer {peer_addr}: {e}")
                
                broadcast_sock.close()
                print(f"Broadcast COORDINATOR: {coord_msg.coord_addr}")
                
            except Exception as e:
                print(f"Failed to broadcast COORDINATOR: {e}")

    def _broadcast_new_coordinator(self):
        """Broadcast COORDINATOR message to inform nodes of new coordinator - must be called within state_lock."""
        if self.state.role != 'BACKUP':
            return

        try:
            broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            coord_msg = COORDINATOR(
                node_id=self.node_id,
                seq=1,
                term=self.state.term,
                coord_id=self.node_id,
                coord_addr=(socket.gethostname(), self.client_port)
            )
            
            broadcast_data = serialize(coord_msg)
            if broadcast_data:
                broadcast_sock.sendto(broadcast_data, ('255.255.255.255', self.client_port))
                broadcast_sock.sendto(broadcast_data, ('127.0.0.1', self.client_port))
                print(f"Broadcast COORDINATOR message: {coord_msg.coord_addr}")
            
            broadcast_sock.close()
            
        except Exception as e:
            print(f"Failed to broadcast coordinator: {e}")


def parse_peers(peers_str: str) -> List[Tuple[str, int]]:
    """Parse comma-separated peer addresses into list of (host, port) tuples."""
    if not peers_str:
        return []
    
    peers = []
    for peer in peers_str.split(','):
        peer = peer.strip()
        if ':' in peer:
            host, port = peer.split(':', 1)
            peers.append((host, int(port)))
        else:
            peers.append((peer, 50000))  # Default client port
    return peers


def main():
    """Main function with CLI argument parsing for PRIMARY/BACKUP roles."""
    # Default failure simulation parameters for presentation
    # P(at least 1 failure in 30s) = 0.9 with ~6 nodes
    # λ = -ln(0.1)/30 ≈ 0.077 failures/second total
    # Per node: 0.077/6 ≈ 0.013 per second
    DEFAULT_DROP_RATE = 0.3  # 30% message loss
    DEFAULT_FAILURE_PROB = 0.013  # ~1.3% per check
    DEFAULT_PERMANENT_PROB = 0.10  # 10% chance of permanent failure

    parser = argparse.ArgumentParser(description='Distributed Coordinator with PRIMARY/BACKUP roles')
    parser.add_argument('--role', choices=['primary', 'backup'], required=True,
                      help='Coordinator role: primary or backup')
    parser.add_argument('--id', type=int, required=True,
                      help='Coordinator ID (98,99,100 for backups; 100 for primary)')
    parser.add_argument('--peers', type=str, default='',
                      help='Comma-separated list of peer coordinator addresses (ip:port,ip:port)')
    parser.add_argument('--host', default='0.0.0.0',
                      help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--client-port', type=int, default=50000,
                      help='Client port (default: 50000)')
    parser.add_argument('--coord-port', type=int, default=50001,
                      help='Coordinator port (default: 50001)')
    parser.add_argument('--drop-rate', type=float,
                      default=float(os.environ.get('DROP_RATE', DEFAULT_DROP_RATE)),
                      help=f'Message drop probability (default: {DEFAULT_DROP_RATE*100:.0f}%% from env)')
    parser.add_argument('--failure-prob', type=float,
                      default=float(os.environ.get('FAILURE_PROB', DEFAULT_FAILURE_PROB)),
                      help=f'Node failure probability per check (default: {DEFAULT_FAILURE_PROB*100:.2f}%% from env)')
    parser.add_argument('--permanent-failure-prob', type=float,
                      default=float(os.environ.get('PERMANENT_FAILURE_PROB', DEFAULT_PERMANENT_PROB)),
                      help=f'Probability failure is permanent (default: {DEFAULT_PERMANENT_PROB*100:.0f}%%)')

    args = parser.parse_args()

    peers = parse_peers(args.peers)
    role = args.role.upper()

    coordinator = Coordinator(
        host=args.host,
        client_port=args.client_port,
        coord_port=args.coord_port,
        role=role,
        node_id=args.id,
        peers=peers,
        drop_rate=args.drop_rate,
        failure_prob=args.failure_prob,
        permanent_failure_prob=args.permanent_failure_prob
    )
    
    try:
        coordinator.start()
        
        print("Coordinator running. Press Ctrl+C to stop.")
        while coordinator.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping Coordinator...")
    finally:
        coordinator.stop()


if __name__ == "__main__":
    main()