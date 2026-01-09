import time
import threading
import sys
import signal
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from tests.test_env import setup_test_environment, standard_cluster_startup, ProcessManager, NetworkConfig, TimestampLogger, ProcessInfo

@dataclass
class NodeState:
	node_id: str
	status: str
	last_seen: float
	grants_received: int
	color: str

@dataclass
class SystemState:
	primary_id: Optional[str]
	current_holder: Optional[str]
	queue_size: int
	total_grants: int
	uptime: float
	nodes: Dict[str, NodeState]

class VisualDisplay:
	def __init__(self):
		self.colors = {
			'IDLE': '\033[37m',      # White
			'REQUESTING': '\033[33m', # Yellow
			'HOLDING': '\033[32m',   # Green
			'RELEASING': '\033[36m', # Cyan
			'ERROR': '\033[31m',     # Red
			'PRIMARY': '\033[35m',   # Magenta
			'BACKUP': '\033[34m',    # Blue
			'RESET': '\033[0m'       # Reset
		}
	
	def clear_screen(self):
		print("\033[2J\033[H", end='')
	
	def print_header(self, title: str):
		print("=" * 80)
		print(f"{title:^80}")
		print("=" * 80)
	
	def print_system_status(self, state: SystemState):
		print(f"System Status:")
		print(f"  Primary: {self.colors['PRIMARY']}{state.primary_id or 'Unknown'}{self.colors['RESET']}")
		print(f"  Current Holder: {self.colors['HOLDING']}{state.current_holder or 'None'}{self.colors['RESET']}")
		print(f"  Queue Size: {state.queue_size}")
		print(f"  Total Grants: {state.total_grants}")
		print(f"  Uptime: {state.uptime:.1f}s")
		print()
	
	def print_node_status(self, nodes: Dict[str, NodeState]):
		print("Node Status:")
		print(f"{'Node ID':<15} {'Status':<12} {'Grants':<8} {'Last Seen':<12}")
		print("-" * 55)
		
		for node_id, node_state in nodes.items():
			color = self.colors.get(node_state.status, self.colors['RESET'])
			time_since = time.time() - node_state.last_seen
			print(f"{node_id:<15} {color}{node_state.status:<12}{self.colors['RESET']} {node_state.grants_received:<8} {time_since:<12.1f}s")
		print()
	
	def print_event_log(self, events: List[str], max_events: int = 10):
		print("Recent Events:")
		print("-" * 40)
		
		recent_events = events[-max_events:] if len(events) > max_events else events
		for event in recent_events:
			print(f"  {event}")
		print()

class DemoMonitor:
	def __init__(self, processes: Dict[str, ProcessInfo]):
		self.processes = processes
		self.system_state = SystemState(
			primary_id=None,
			current_holder=None,
			queue_size=0,
			total_grants=0,
			uptime=0.0,
			nodes={}
		)
		self.event_log: List[str] = []
		self.start_time = time.time()
		self.lock = threading.Lock()
	
	def add_event(self, event: str):
		with self.lock:
			timestamp = f"{time.time() - self.start_time:6.1f}s"
			self.event_log.append(f"[{timestamp}] {event}")
	
	def update_node_state(self, node_id: str, status: str):
		with self.lock:
			if node_id not in self.system_state.nodes:
				self.system_state.nodes[node_id] = NodeState(
					node_id=node_id,
					status=status,
					last_seen=time.time(),
					grants_received=0,
					color='IDLE'
				)
			else:
				self.system_state.nodes[node_id].status = status
				self.system_state.nodes[node_id].last_seen = time.time()
			
			if status == 'HOLDING':
				self.system_state.current_holder = node_id
				self.system_state.nodes[node_id].grants_received += 1
				self.system_state.total_grants += 1
				self.add_event(f"{node_id} acquired critical section")
			elif status == 'REQUESTING':
				self.add_event(f"{node_id} requesting access")
			elif status == 'RELEASING':
				if self.system_state.current_holder == node_id:
					self.system_state.current_holder = None
				self.add_event(f"{node_id} released critical section")
	
	def monitor_processes(self):
		while True:
			with self.lock:
				self.system_state.uptime = time.time() - self.start_time
			
			for proc_name, proc_info in self.processes.items():
				if proc_info.process.poll() is None:
					try:
						line = proc_info.process.stdout.readline()
						if line:
							line = line.strip()
							self.parse_process_output(proc_name, proc_info, line)
					except:
						pass
			
			time.sleep(0.1)
	
	def parse_process_output(self, proc_name: str, proc_info: ProcessInfo, line: str):
		node_id = proc_info.node_id
		
		if 'REQ' in line and ('sending' in line.lower() or 'requesting' in line.lower()):
			self.update_node_state(node_id, 'REQUESTING')
		elif 'GRANT' in line or 'granted' in line.lower():
			self.update_node_state(node_id, 'HOLDING')
		elif 'REL' in line and ('sending' in line.lower() or 'release' in line.lower()):
			self.update_node_state(node_id, 'RELEASING')
		elif 'PRIMARY' in line and 'becoming' in line.lower():
			with self.lock:
				self.system_state.primary_id = node_id
			self.add_event(f"{node_id} became PRIMARY coordinator")
		elif 'ELECTION' in line:
			self.add_event(f"Election started by {node_id}")
		elif 'ERROR' in line or 'failed' in line.lower():
			self.update_node_state(node_id, 'ERROR')
			self.add_event(f"Error in {node_id}: {line[:50]}")
		elif proc_info.role == 'CLIENT' and 'idle' in line.lower():
			self.update_node_state(node_id, 'IDLE')

def happy_path_demo():
	print("Starting Happy Path Demo...")
	print("This demo shows normal operation with 3 nodes cycling through the critical section")
	print("Note: Built-in failure simulation is active (30% message drop, node failures)")
	print("To disable failures, set environment variables: DROP_RATE=0 FAILURE_PROB=0")

	manager, config, logger = setup_test_environment()
	display = VisualDisplay()
	
	try:
		processes = {}
		
		processes['primary'] = manager.start_coordinator('primary', 100, 50000)
		time.sleep(2)
		
		processes['node1'] = manager.start_node('Node_1')
		processes['node2'] = manager.start_node('Node_2')
		processes['node3'] = manager.start_node('Node_3')
		
		time.sleep(3)
		
		monitor = DemoMonitor(processes)
		monitor_thread = threading.Thread(target=monitor.monitor_processes, daemon=True)
		monitor_thread.start()
		
		demo_duration = 60.0
		end_time = time.time() + demo_duration
		
		print(f"\nRunning happy path demo for {demo_duration} seconds...")
		print("Press Ctrl+C to stop early\n")
		
		while time.time() < end_time:
			try:
				display.clear_screen()
				display.print_header("Critical Section Demo - Happy Path")
				
				with monitor.lock:
					display.print_system_status(monitor.system_state)
					display.print_node_status(monitor.system_state.nodes)
					display.print_event_log(monitor.event_log)
				
				print(f"Demo will run for {end_time - time.time():.1f} more seconds...")
				time.sleep(2)
				
			except KeyboardInterrupt:
				print("\nDemo interrupted by user")
				break
		
		print("\nHappy path demo completed successfully!")
		
	except Exception as e:
		print(f"Demo failed: {e}")
	finally:
		manager.cleanup_all()

def failure_injection_demo():
	print("Starting Failure Injection Demo...")
	print("This demo shows coordinator failover and system recovery")
	print("Note: Built-in failure simulation is active (30% message drop, node failures)")
	print("Additionally, we will manually kill the primary coordinator to demonstrate failover")

	manager, config, logger = setup_test_environment()
	display = VisualDisplay()
	
	try:
		processes = standard_cluster_startup(manager, logger)
		
		monitor = DemoMonitor(processes)
		monitor_thread = threading.Thread(target=monitor.monitor_processes, daemon=True)
		monitor_thread.start()
		
		print(f"\nSystem starting up...")
		time.sleep(5)
		
		phase_duration = 15.0
		
		print("\nPhase 1: Normal operation")
		end_time = time.time() + phase_duration
		while time.time() < end_time:
			try:
				display.clear_screen()
				display.print_header("Critical Section Demo - Phase 1: Normal Operation")
				
				with monitor.lock:
					display.print_system_status(monitor.system_state)
					display.print_node_status(monitor.system_state.nodes)
					display.print_event_log(monitor.event_log)
				
				print(f"Phase 1 time remaining: {end_time - time.time():.1f}s")
				time.sleep(2)
				
			except KeyboardInterrupt:
				print("\nDemo interrupted by user")
				return
		
		print("\nPhase 2: Injecting coordinator failure...")
		monitor.add_event("INJECTING PRIMARY COORDINATOR FAILURE")
		
		primary_killed = manager.kill_process(processes['primary'])
		if primary_killed:
			monitor.add_event("Primary coordinator terminated")
		else:
			monitor.add_event("Failed to terminate primary coordinator")
		
		end_time = time.time() + phase_duration
		while time.time() < end_time:
			try:
				display.clear_screen()
				display.print_header("Critical Section Demo - Phase 2: Coordinator Failure")
				
				with monitor.lock:
					display.print_system_status(monitor.system_state)
					display.print_node_status(monitor.system_state.nodes)
					display.print_event_log(monitor.event_log)
				
				print(f"Phase 2 time remaining: {end_time - time.time():.1f}s")
				time.sleep(2)
				
			except KeyboardInterrupt:
				print("\nDemo interrupted by user")
				return
		
		print("\nPhase 3: System recovery")
		end_time = time.time() + phase_duration
		while time.time() < end_time:
			try:
				display.clear_screen()
				display.print_header("Critical Section Demo - Phase 3: System Recovery")
				
				with monitor.lock:
					display.print_system_status(monitor.system_state)
					display.print_node_status(monitor.system_state.nodes)
					display.print_event_log(monitor.event_log)
				
				print(f"Phase 3 time remaining: {end_time - time.time():.1f}s")
				time.sleep(2)
				
			except KeyboardInterrupt:
				print("\nDemo interrupted by user")
				return
		
		print("\nFailure injection demo completed!")
		
	except Exception as e:
		print(f"Demo failed: {e}")
	finally:
		manager.cleanup_all()

def interactive_demo():
	print("Starting Interactive Demo...")
	print("Commands: 'happy' for happy path, 'failure' for failure injection, 'quit' to exit")
	
	while True:
		try:
			command = input("\nDemo> ").strip().lower()
			
			if command == 'quit' or command == 'q':
				print("Exiting demo...")
				break
			elif command == 'happy' or command == 'h':
				happy_path_demo()
			elif command == 'failure' or command == 'f':
				failure_injection_demo()
			else:
				print("Available commands:")
				print("  happy (h)   - Run happy path demo")
				print("  failure (f) - Run failure injection demo")
				print("  quit (q)    - Exit")
		
		except KeyboardInterrupt:
			print("\nExiting demo...")
			break
		except EOFError:
			print("\nExiting demo...")
			break

def signal_handler(signum, frame):
	print("\nDemo interrupted, cleaning up...")
	sys.exit(0)

if __name__ == "__main__":
	signal.signal(signal.SIGINT, signal_handler)
	
	if len(sys.argv) > 1:
		demo_type = sys.argv[1].lower()
		
		if demo_type == 'happy':
			happy_path_demo()
		elif demo_type == 'failure':
			failure_injection_demo()
		elif demo_type == 'interactive':
			interactive_demo()
		else:
			print(f"Unknown demo type: {demo_type}")
			print("Available demos: happy, failure, interactive")
			sys.exit(1)
	else:
		interactive_demo()