import socket
import threading
import time
import argparse
from collections import deque
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from protocol import REQ, GRANT, REL, HB, ACK, NACK, SYNC, SYNC_ACK, serialize, deserialize, Message


@dataclass
class CoordinatorState:
	term: int
	role: str
	current_holder: Optional[str]
	lease_expiry: float
	queue: deque = field(default_factory=deque)
	known_nodes: set = field(default_factory=set)
	last_seen_seq: Dict[str, int] = field(default_factory=dict)
	backup_addrs: List[Tuple[str, int]] = field(default_factory=list)


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
		return cls(
			term=data['term'],
			current_holder=data['current_holder'],
			lease_expiry=data['lease_expiry'],
			queue_list=data['queue_list'],
			known_nodes_list=data['known_nodes_list'],
			last_seen_seq=data['last_seen_seq']
		)


class Coordinator:
    """
    Core coordinator implementation for centralized mutual exclusion.
    Extracted from Jupyter notebook for better modularity.
    """
    
    def __init__(self, host='0.0.0.0', client_port=50000, coord_port=50001, lease_duration=5.0, 
                 role='PRIMARY', node_id=100, peers=None):
        self.host = host
        self.client_port = client_port
        self.coord_port = coord_port
        self.lease_duration = lease_duration
        
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
        
        # Network
        self.client_sock = None
        self.coord_sock = None
        self.client_thread = None
        self.coord_thread = None
    
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
        
        print(f"running as {self.state.role}")
        print(f"Client UDP Server läuft auf {self.client_port}")
        print(f"Coordinator UDP Server läuft auf {self.coord_port}")
    
    def stop(self):
        """Stop the coordinator server."""
        if not self.running:
            return
            
        print("Beende System...")
        self.running = False
        
        # Send internal stop signals to wake up server threads
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.sendto(b"INTERNAL_STOP Node_sys", ('127.0.0.1', self.client_port))
            temp_sock.sendto(b"INTERNAL_STOP Node_sys", ('127.0.0.1', self.coord_port))
            temp_sock.close()
        except Exception as e:
            print(f"Konnte Wake-Up Packet nicht senden: {e}")
        
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
                'role': self.state.role
            }
    
    def _client_server(self):
        """Client UDP server loop - handles REQ/REL/HB from nodes."""
        self.client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.client_sock.bind((self.host, self.client_port))
        except OSError:
            print(f"FEHLER: Client Port {self.client_port} ist noch belegt! Bitte Kernel neustarten.")
            self.running = False
            return
        
        while self.running:
            try:
                # Automatic grants on lease expiry (PRIMARY only)
                if self.state.role == 'PRIMARY':
                    with self.state_lock:
                        if self.state.current_holder and time.time() > self.state.lease_expiry and self.state.queue:
                            next_id, next_addr, next_seq = self.state.queue.popleft()
                            self.state.current_holder = next_id
                            self.state.lease_expiry = time.time() + self.lease_duration
                            grant_msg = GRANT(node_id=next_id, seq=next_seq, term=self.state.term, lease_duration=self.lease_duration)
                            self.client_sock.sendto(serialize(grant_msg), next_addr)
                            self._sync_to_backups()
                
                try:
                    data, addr = self.client_sock.recvfrom(1024)
                except socket.timeout:
                    continue
                except OSError:
                    break
                
                if data.startswith(b"INTERNAL_STOP"):
                    print("Client Stop-Signal empfangen.")
                    break
                
                msg = deserialize(data)
                if not msg or not isinstance(msg, Message):
                    continue
                
                with self.state_lock:
                    self.state.known_nodes.add(msg.node_id)
                    
                    # Check for duplicates
                    if msg.node_id in self.state.last_seen_seq and msg.seq <= self.state.last_seen_seq[msg.node_id]:
                        nack_msg = NACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type=msg.type, reason="duplicate")
                        self.client_sock.sendto(serialize(nack_msg), addr)
                        continue
                    
                    self.state.last_seen_seq[msg.node_id] = msg.seq
                    self.state.term = max(self.state.term, msg.term)
                    
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
                    else:  # BACKUP
                        # Send NACK redirect for all client messages
                        primary_addr = ('192.168.1.101', 50000)  # Default primary address
                        if self.state.backup_addrs:
                            primary_addr = self.state.backup_addrs[0]
                        nack_msg = NACK(node_id=self.node_id, seq=msg.seq, term=self.state.term, msg_type=msg.type, reason=f"redirect_to_{primary_addr[0]}:{primary_addr[1]}")
                        self.client_sock.sendto(serialize(nack_msg), addr)
                        
            except Exception as e:
                if self.running:
                    print(f"Client Server Error: {e}")
        
        if self.client_sock:
            self.client_sock.close()
        print("Client Server Thread beendet.")
    
    def _coord_server(self):
        """Coordinator UDP server loop - handles SYNC/COORD_HB between coordinators."""
        self.coord_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.coord_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.coord_sock.bind((self.host, self.coord_port))
        except OSError:
            print(f"FEHLER: Coordinator Port {self.coord_port} ist noch belegt! Bitte Kernel neustarten.")
            self.running = False
            return
        
        while self.running:
            try:
                try:
                    data, addr = self.coord_sock.recvfrom(1024)
                except socket.timeout:
                    continue
                except OSError:
                    break
                
                if data.startswith(b"INTERNAL_STOP"):
                    print("Coordinator Stop-Signal empfangen.")
                    break
                
                msg = deserialize(data)
                if not msg or not isinstance(msg, Message):
                    continue
                
                with self.state_lock:
                    if isinstance(msg, SYNC):
                        # Apply state update from primary
                        self._apply_state_snapshot(msg.state_snapshot)
                        ack_msg = SYNC_ACK(node_id=self.node_id, seq=msg.seq, term=self.state.term)
                        self.coord_sock.sendto(serialize(ack_msg), addr)
                        
            except Exception as e:
                if self.running:
                    print(f"Coordinator Server Error: {e}")
        
        if self.coord_sock:
            self.coord_sock.close()
        print("Coordinator Server Thread beendet.")
    
    def _handle_request(self, node_id, addr, seq):
        """Handle REQ message - must be called within state_lock."""
        if self.state.current_holder is None:
            # Grant immediately
            self.state.current_holder = node_id
            self.state.lease_expiry = time.time() + self.lease_duration
            grant_msg = GRANT(node_id=node_id, seq=seq, term=self.state.term, lease_duration=self.lease_duration)
            self.client_sock.sendto(serialize(grant_msg), addr)
            self._sync_to_backups()
        elif self.state.current_holder == node_id:
            # Renew lease for same node
            self.state.lease_expiry = time.time() + self.lease_duration
            grant_msg = GRANT(node_id=node_id, seq=seq, term=self.state.term, lease_duration=self.lease_duration)
            self.client_sock.sendto(serialize(grant_msg), addr)
            self._sync_to_backups()
        else:
            # Queue the request if not already queued
            if node_id not in [x[0] for x in self.state.queue]:
                self.state.queue.append((node_id, addr, seq))
    
    def _handle_heartbeat(self, node_id):
        """Handle HB message - must be called within state_lock."""
        if node_id == self.state.current_holder:
            self.state.lease_expiry = time.time() + self.lease_duration
            self._sync_to_backups()
    
    def _handle_release(self, node_id):
        """Handle REL message - must be called within state_lock."""
        if node_id == self.state.current_holder:
            if self.state.queue:
                # Grant to next in queue
                next_id, next_addr, next_seq = self.state.queue.popleft()
                self.state.current_holder = next_id
                self.state.lease_expiry = time.time() + self.lease_duration
                grant_msg = GRANT(node_id=next_id, seq=next_seq, term=self.state.term, lease_duration=self.lease_duration)
                self.client_sock.sendto(serialize(grant_msg), next_addr)
            else:
                # No one waiting
                self.state.current_holder = None
            self._sync_to_backups()
    
    def _sync_to_backups(self):
        """Send state synchronization to backup coordinators - must be called within state_lock."""
        if not self.state.backup_addrs or self.state.role != 'PRIMARY':
            return
            
        snapshot = StateSnapshot(
            term=self.state.term,
            current_holder=self.state.current_holder,
            lease_expiry=self.state.lease_expiry,
            queue_list=list(self.state.queue),
            known_nodes_list=list(self.state.known_nodes),
            last_seen_seq=self.state.last_seen_seq.copy()
        )
        
        sync_msg = SYNC(node_id=self.node_id, seq=0, term=self.state.term, state_snapshot=snapshot.to_dict())
        
        for backup_addr in self.state.backup_addrs:
            try:
                coord_addr = (backup_addr[0], backup_addr[1] + 1)  # coord_port = client_port + 1
                self.coord_sock.sendto(serialize(sync_msg), coord_addr)
            except Exception as e:
                print(f"Failed to sync to backup {backup_addr}: {e}")
    
    def _apply_state_snapshot(self, snapshot_dict: Dict[str, Any]):
        """Apply state snapshot from primary - must be called within state_lock."""
        snapshot = StateSnapshot.from_dict(snapshot_dict)
        
        self.state.term = snapshot.term
        self.state.current_holder = snapshot.current_holder
        self.state.lease_expiry = snapshot.lease_expiry
        self.state.queue = deque(snapshot.queue_list)
        self.state.known_nodes = set(snapshot.known_nodes_list)
        self.state.last_seen_seq = snapshot.last_seen_seq.copy()


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
    
    args = parser.parse_args()
    
    peers = parse_peers(args.peers)
    role = args.role.upper()
    
    coordinator = Coordinator(
        host=args.host,
        client_port=args.client_port,
        coord_port=args.coord_port,
        role=role,
        node_id=args.id,
        peers=peers
    )
    
    try:
        coordinator.start()
        
        print("Coordinator läuft. Drücke Ctrl+C zum Beenden.")
        while coordinator.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nBeende Coordinator...")
    finally:
        coordinator.stop()


if __name__ == "__main__":
    main()