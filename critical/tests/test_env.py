import subprocess
import socket
import time
import random
import signal
import os
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ProcessInfo:
	process: subprocess.Popen
	role: str
	node_id: str
	address: Tuple[str, int]

@dataclass
class NetworkConfig:
	primary_addr: Tuple[str, int] = ('127.0.0.1', 50000)
	backup1_addr: Tuple[str, int] = ('127.0.0.1', 50002)
	backup2_addr: Tuple[str, int] = ('127.0.0.1', 50004)
	coord_port_offset: int = 1
	
	def get_coord_port(self, client_port: int) -> int:
		return client_port + self.coord_port_offset
	
	def get_peer_string(self, exclude_port: int) -> str:
		addrs = [self.primary_addr, self.backup1_addr, self.backup2_addr]
		peers = [f"{addr[0]}:{addr[1]}" for addr in addrs if addr[1] != exclude_port]
		return ','.join(peers)

class ProcessManager:
	def __init__(self, config: NetworkConfig):
		self.config = config
		self.processes: List[ProcessInfo] = []
	
	def start_coordinator(self, role: str, node_id: int, port: int) -> ProcessInfo:
		peers = self.config.get_peer_string(port)
		cmd = [
			'uv', 'run', 'critical/coordinator.py',
			'--role', role,
			'--id', str(node_id),
			'--peers', peers
		]
		
		env = os.environ.copy()
		env['PYTHONPATH'] = '/Users/phedias/code/sem3/algo_dist_sys/Critical_Section'
		
		process = subprocess.Popen(
			cmd,
			cwd='/Users/phedias/code/sem3/algo_dist_sys/Critical_Section',
			env=env,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			text=True
		)
		
		info = ProcessInfo(
			process=process,
			role=role,
			node_id=f"Node_{node_id}",
			address=('127.0.0.1', port)
		)
		self.processes.append(info)
		return info
	
	def start_node(self, node_id: str) -> ProcessInfo:
		cmd = ['uv', 'run', 'critical/node.py', '--id', node_id]

		env = os.environ.copy()
		env['PYTHONPATH'] = '/Users/phedias/code/sem3/algo_dist_sys/Critical_Section'

		process = subprocess.Popen(
			cmd,
			cwd='/Users/phedias/code/sem3/algo_dist_sys/Critical_Section',
			env=env,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			text=True
		)

		info = ProcessInfo(
			process=process,
			role='CLIENT',
			node_id=node_id,
			address=('127.0.0.1', 0)
		)
		self.processes.append(info)
		return info
	
	
	def kill_process(self, process_info: ProcessInfo) -> bool:
		try:
			process_info.process.terminate()
			process_info.process.wait(timeout=3)
			return True
		except subprocess.TimeoutExpired:
			process_info.process.kill()
			process_info.process.wait()
			return True
		except Exception:
			return False
	
	def cleanup_all(self):
		for process_info in self.processes:
			try:
				if process_info.process.poll() is None:
					process_info.process.terminate()
					process_info.process.wait(timeout=2)
			except:
				try:
					process_info.process.kill()
					process_info.process.wait()
				except:
					pass
		self.processes.clear()
	
	def wait_for_startup(self, timeout: float = 10.0) -> bool:
		start_time = time.time()
		while time.time() - start_time < timeout:
			all_responsive = True
			for proc_info in self.processes:
				if proc_info.role in ['primary', 'backup']:
					if not self._check_coordinator_responsive(proc_info.address):
						all_responsive = False
						break
			if all_responsive:
				return True
			time.sleep(0.5)
		return False
	
	def _check_coordinator_responsive(self, addr: Tuple[str, int]) -> bool:
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.settimeout(1.0)
			test_data = b'{"type": "HB", "node_id": "test", "seq": 1, "term": 0}'
			sock.sendto(test_data, addr)
			response, _ = sock.recvfrom(1024)
			sock.close()
			return len(response) > 0
		except:
			return False

class MessageDropWrapper:
	def __init__(self, original_socket: socket.socket, drop_rate: float = 0.0):
		self.original_socket = original_socket
		self.drop_rate = drop_rate
		self.dropped_count = 0
		self.sent_count = 0
	
	def __getattr__(self, name):
		return getattr(self.original_socket, name)
	
	def sendto(self, data: bytes, address: Tuple[str, int]) -> int:
		self.sent_count += 1
		if random.random() < self.drop_rate:
			self.dropped_count += 1
			return len(data)
		return self.original_socket.sendto(data, address)
	
	def get_stats(self) -> Dict[str, int]:
		return {
			'sent': self.sent_count,
			'dropped': self.dropped_count,
			'delivered': self.sent_count - self.dropped_count
		}

class TimestampLogger:
	def __init__(self):
		self.logs: List[Dict[str, Any]] = []
	
	def log_event(self, event_type: str, message: str, **kwargs):
		log_entry = {
			'timestamp': datetime.now().isoformat(),
			'type': event_type,
			'message': message,
			**kwargs
		}
		self.logs.append(log_entry)
		print(f"[{log_entry['timestamp']}] {event_type}: {message}")
	
	def log_test_start(self, test_name: str):
		self.log_event('TEST_START', f"Starting {test_name}")
	
	def log_test_end(self, test_name: str, success: bool, duration: float):
		status = 'PASS' if success else 'FAIL'
		self.log_event('TEST_END', f"{test_name} - {status} ({duration:.2f}s)")
	
	def log_process_event(self, event: str, process_info: ProcessInfo):
		self.log_event('PROCESS', f"{event}: {process_info.role} {process_info.node_id}")
	
	def log_network_event(self, event: str, details: str):
		self.log_event('NETWORK', f"{event}: {details}")
	
	def get_logs_by_type(self, event_type: str) -> List[Dict[str, Any]]:
		return [log for log in self.logs if log['type'] == event_type]
	
	def clear_logs(self):
		self.logs.clear()

def setup_test_environment(drop_rate: float = 0.0) -> Tuple[ProcessManager, NetworkConfig, TimestampLogger]:
	config = NetworkConfig()
	manager = ProcessManager(config)
	logger = TimestampLogger()
	
	logger.log_event('SETUP', f"Test environment initialized with {drop_rate*100}% message drop rate")
	
	return manager, config, logger

def standard_cluster_startup(manager: ProcessManager, logger: TimestampLogger) -> Dict[str, ProcessInfo]:
	logger.log_event('SETUP', "Starting standard cluster: 1 primary + 2 backups + 3 nodes")
	
	processes = {}
	
	processes['primary'] = manager.start_coordinator('primary', 100, 50000)
	logger.log_process_event('STARTED', processes['primary'])
	
	processes['backup1'] = manager.start_coordinator('backup', 98, 50002)
	logger.log_process_event('STARTED', processes['backup1'])
	
	processes['backup2'] = manager.start_coordinator('backup', 99, 50004)
	logger.log_process_event('STARTED', processes['backup2'])
	
	time.sleep(2)
	
	processes['node1'] = manager.start_node('Node_1')
	logger.log_process_event('STARTED', processes['node1'])

	processes['node2'] = manager.start_node('Node_2')
	logger.log_process_event('STARTED', processes['node2'])

	processes['node3'] = manager.start_node('Node_3')
	logger.log_process_event('STARTED', processes['node3'])
	
	logger.log_event('SETUP', "Waiting for cluster startup...")
	if manager.wait_for_startup():
		logger.log_event('SETUP', "Cluster startup complete")
	else:
		logger.log_event('SETUP', "Cluster startup timeout - proceeding anyway")
	
	return processes

if __name__ == "__main__":
	manager, config, logger = setup_test_environment()
	
	try:
		processes = standard_cluster_startup(manager, logger)
		
		logger.log_event('TEST', "Environment test running for 10 seconds...")
		time.sleep(10)
		
		logger.log_event('TEST', "Test complete, cleaning up...")
		
	finally:
		manager.cleanup_all()
		logger.log_event('CLEANUP', "All processes terminated")