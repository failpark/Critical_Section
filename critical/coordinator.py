import socket
import threading
import time
from collections import deque


class Coordinator:
    """
    Core coordinator implementation for centralized mutual exclusion.
    Extracted from Jupyter notebook for better modularity.
    """
    
    def __init__(self, host='0.0.0.0', port=5000, lease_duration=5.0):
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
                        next_id, next_addr = self.queue.popleft()
                        self.current_holder = next_id
                        self.lease_expiry = time.time() + self.lease_duration
                        self.server_sock.sendto(b'GRANT', next_addr)
                
                try:
                    data, addr = self.server_sock.recvfrom(1024)
                except socket.timeout:
                    continue
                except OSError:
                    break  # Socket closed
                
                msg = data.decode()
                
                if msg == "INTERNAL_STOP":
                    print("Stop-Signal empfangen.")
                    break
                
                parts = msg.split()
                if len(parts) < 2:
                    continue
                
                cmd, node_id = parts[0], parts[1]
                
                with self.state_lock:
                    self.known_nodes.add(node_id)
                    
                    if cmd == 'REQ':
                        self._handle_request(node_id, addr)
                    elif cmd == 'HB':
                        self._handle_heartbeat(node_id)
                    elif cmd == 'REL':
                        self._handle_release(node_id)
                        
            except Exception as e:
                if self.running:
                    print(f"Server Error: {e}")
        
        if self.server_sock:
            self.server_sock.close()
        print("Server Thread beendet.")
    
    def _handle_request(self, node_id, addr):
        """Handle REQ message - must be called within state_lock."""
        if self.current_holder is None:
            # Grant immediately
            self.current_holder = node_id
            self.lease_expiry = time.time() + self.lease_duration
            self.server_sock.sendto(b'GRANT', addr)
        elif self.current_holder == node_id:
            # Renew lease for same node
            self.lease_expiry = time.time() + self.lease_duration
            self.server_sock.sendto(b'GRANT', addr)
        else:
            # Queue the request if not already queued
            if node_id not in [x[0] for x in self.queue]:
                self.queue.append((node_id, addr))
    
    def _handle_heartbeat(self, node_id):
        """Handle HB message - must be called within state_lock."""
        if node_id == self.current_holder:
            self.lease_expiry = time.time() + self.lease_duration
    
    def _handle_release(self, node_id):
        """Handle REL message - must be called within state_lock."""
        if node_id == self.current_holder:
            if self.queue:
                # Grant to next in queue
                next_id, next_addr = self.queue.popleft()
                self.current_holder = next_id
                self.lease_expiry = time.time() + self.lease_duration
                self.server_sock.sendto(b'GRANT', next_addr)
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