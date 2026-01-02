import socket
import threading
import time
from collections import deque
from typing import Optional, Dict
from protocol import REQ, GRANT, REL, HB, ACK, NACK, serialize, deserialize, Message


class Coordinator:
    """
    Core coordinator implementation for centralized mutual exclusion.
    Extracted from Jupyter notebook for better modularity.
    """
    
    def __init__(self, host='0.0.0.0', port=50000, lease_duration=5.0):
        self.host = host
        self.port = port
        self.lease_duration = lease_duration
        
        # State variables (protected by state_lock)
        self.state_lock = threading.Lock()
        self.running = True
        self.queue = deque()
        self.current_holder = None
        self.lease_expiry = 0.0
        self.known_nodes = set()
        self.term = 0
        self.last_seen_seq: Dict[str, int] = {}
        
        # Network
        self.server_sock = None
        self.server_thread = None
    
    def start(self):
        """Start the coordinator server."""
        self.running = True
        self.queue.clear()
        self.current_holder = None
        self.known_nodes.clear()
        
        self.server_thread = threading.Thread(target=self._udp_server, daemon=True)
        self.server_thread.start()
        
        print(f"UDP Server läuft auf {self.port}")
    
    def stop(self):
        """Stop the coordinator server."""
        if not self.running:
            return
            
        print("Beende System...")
        self.running = False
        
        # Send internal stop signal to wake up server thread
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.sendto(b"INTERNAL_STOP Node_sys", ('127.0.0.1', self.port))
            temp_sock.close()
        except Exception as e:
            print(f"Konnte Wake-Up Packet nicht senden: {e}")
        
        time.sleep(0.1)
        if self.server_sock:
            try:
                self.server_sock.close()
            except:
                pass
    
    def get_state(self):
        """Get current coordinator state (thread-safe)."""
        with self.state_lock:
            current_time = time.time()
            return {
                'current_holder': self.current_holder,
                'lease_expiry': self.lease_expiry,
                'lease_remaining': max(0, self.lease_expiry - current_time) if self.current_holder else 0,
                'queue': list(self.queue),
                'queue_length': len(self.queue),
                'known_nodes': set(self.known_nodes),
                'running': self.running
            }
    
    def _udp_server(self):
        """Main UDP server loop - extracted from notebook."""
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_sock.bind((self.host, self.port))
        except OSError:
            print(f"FEHLER: Port {self.port} ist noch belegt! Bitte Kernel neustarten.")
            self.running = False
            return
        
        while self.running:
            try:
                # Automatic grants on lease expiry
                with self.state_lock:
                    if self.current_holder and time.time() > self.lease_expiry and self.queue:
                        next_id, next_addr, next_seq = self.queue.popleft()
                        self.current_holder = next_id
                        self.lease_expiry = time.time() + self.lease_duration
                        grant_msg = GRANT(node_id=next_id, seq=next_seq, term=self.term, lease_duration=self.lease_duration)
                        self.server_sock.sendto(serialize(grant_msg), next_addr)
                
                try:
                    data, addr = self.server_sock.recvfrom(1024)
                except socket.timeout:
                    continue
                except OSError:
                    break  # Socket closed
                
                if data.startswith(b"INTERNAL_STOP"):
                    print("Stop-Signal empfangen.")
                    break
                
                msg = deserialize(data)
                if not msg or not isinstance(msg, Message):
                    continue
                
                with self.state_lock:
                    self.known_nodes.add(msg.node_id)
                    
                    # Check for duplicates
                    if msg.node_id in self.last_seen_seq and msg.seq <= self.last_seen_seq[msg.node_id]:
                        nack_msg = NACK(node_id="coordinator", seq=msg.seq, term=self.term, msg_type=msg.type, reason="duplicate")
                        self.server_sock.sendto(serialize(nack_msg), addr)
                        continue
                    
                    self.last_seen_seq[msg.node_id] = msg.seq
                    self.term = max(self.term, msg.term)
                    
                    if isinstance(msg, REQ):
                        self._handle_request(msg.node_id, addr, msg.seq)
                    elif isinstance(msg, HB):
                        self._handle_heartbeat(msg.node_id)
                        ack_msg = ACK(node_id="coordinator", seq=msg.seq, term=self.term, msg_type="HB")
                        self.server_sock.sendto(serialize(ack_msg), addr)
                    elif isinstance(msg, REL):
                        self._handle_release(msg.node_id)
                        ack_msg = ACK(node_id="coordinator", seq=msg.seq, term=self.term, msg_type="REL")
                        self.server_sock.sendto(serialize(ack_msg), addr)
                        
            except Exception as e:
                if self.running:
                    print(f"Server Error: {e}")
        
        if self.server_sock:
            self.server_sock.close()
        print("Server Thread beendet.")
    
    def _handle_request(self, node_id, addr, seq):
        """Handle REQ message - must be called within state_lock."""
        if self.current_holder is None:
            # Grant immediately
            self.current_holder = node_id
            self.lease_expiry = time.time() + self.lease_duration
            grant_msg = GRANT(node_id=node_id, seq=seq, term=self.term, lease_duration=self.lease_duration)
            self.server_sock.sendto(serialize(grant_msg), addr)
        elif self.current_holder == node_id:
            # Renew lease for same node
            self.lease_expiry = time.time() + self.lease_duration
            grant_msg = GRANT(node_id=node_id, seq=seq, term=self.term, lease_duration=self.lease_duration)
            self.server_sock.sendto(serialize(grant_msg), addr)
        else:
            # Queue the request if not already queued
            if node_id not in [x[0] for x in self.queue]:
                self.queue.append((node_id, addr, seq))
    
    def _handle_heartbeat(self, node_id):
        """Handle HB message - must be called within state_lock."""
        if node_id == self.current_holder:
            self.lease_expiry = time.time() + self.lease_duration
    
    def _handle_release(self, node_id):
        """Handle REL message - must be called within state_lock."""
        if node_id == self.current_holder:
            if self.queue:
                # Grant to next in queue
                next_id, next_addr, next_seq = self.queue.popleft()
                self.current_holder = next_id
                self.lease_expiry = time.time() + self.lease_duration
                grant_msg = GRANT(node_id=next_id, seq=next_seq, term=self.term, lease_duration=self.lease_duration)
                self.server_sock.sendto(serialize(grant_msg), next_addr)
            else:
                # No one waiting
                self.current_holder = None


def main():
    """Simple main function to run coordinator standalone."""
    coordinator = Coordinator()
    
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