import time
import statistics
import threading
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from test_env import setup_test_environment, standard_cluster_startup, ProcessManager, NetworkConfig, TimestampLogger

@dataclass
class PerformanceMetrics:
	grants_per_second: float
	avg_grant_latency: float
	total_grants: int
	test_duration: float
	election_time: Optional[float] = None
	replication_overhead: Optional[float] = None

@dataclass
class BenchmarkResults:
	baseline: PerformanceMetrics
	with_replication: PerformanceMetrics
	election_timing: PerformanceMetrics
	comparison_table: str

class PerformanceCollector:
	def __init__(self):
		self.grant_times: List[float] = []
		self.request_times: Dict[str, float] = {}
		self.election_start_time: Optional[float] = None
		self.election_end_time: Optional[float] = None
		self.lock = threading.Lock()
	
	def record_request(self, node_id: str, timestamp: float):
		with self.lock:
			self.request_times[node_id] = timestamp
	
	def record_grant(self, node_id: str, timestamp: float):
		with self.lock:
			if node_id in self.request_times:
				latency = timestamp - self.request_times[node_id]
				self.grant_times.append(latency)
				del self.request_times[node_id]
	
	def record_election_start(self, timestamp: float):
		with self.lock:
			self.election_start_time = timestamp
	
	def record_election_end(self, timestamp: float):
		with self.lock:
			self.election_end_time = timestamp
	
	def get_metrics(self, test_duration: float) -> PerformanceMetrics:
		with self.lock:
			total_grants = len(self.grant_times)
			grants_per_second = total_grants / test_duration if test_duration > 0 else 0
			avg_latency = statistics.mean(self.grant_times) if self.grant_times else 0
			
			election_time = None
			if self.election_start_time and self.election_end_time:
				election_time = self.election_end_time - self.election_start_time
			
			return PerformanceMetrics(
				grants_per_second=grants_per_second,
				avg_grant_latency=avg_latency,
				total_grants=total_grants,
				test_duration=test_duration,
				election_time=election_time
			)

def baseline_throughput_test(duration: float = 60.0) -> PerformanceMetrics:
	print("=" * 60)
	print("BENCHMARK: Baseline Throughput (Single Coordinator)")
	print("=" * 60)
	
	manager, config, logger = setup_test_environment()
	collector = PerformanceCollector()
	
	try:
		logger.log_event('BENCHMARK', f"Starting baseline throughput test for {duration}s")
		
		primary = manager.start_coordinator('primary', 100, 50000)
		logger.log_process_event('STARTED', primary)
		
		time.sleep(2)
		
		nodes = []
		for i in range(5):
			node = manager.start_node(f'node{i+1}.py')
			nodes.append(node)
			logger.log_process_event('STARTED', node)
		
		time.sleep(3)
		
		start_time = time.time()
		monitor_thread = threading.Thread(
			target=monitor_performance,
			args=(nodes + [primary], collector, duration),
			daemon=True
		)
		monitor_thread.start()
		
		time.sleep(duration)
		
		actual_duration = time.time() - start_time
		metrics = collector.get_metrics(actual_duration)
		
		logger.log_event('BENCHMARK', f"Baseline completed: {metrics.grants_per_second:.2f} grants/sec")
		return metrics
		
	except Exception as e:
		logger.log_event('ERROR', f"Baseline test failed: {e}")
		return PerformanceMetrics(0, 0, 0, duration)
	finally:
		manager.cleanup_all()

def replication_overhead_test(duration: float = 60.0) -> PerformanceMetrics:
	print("\n" + "=" * 60)
	print("BENCHMARK: Replication Overhead (1 Primary + 2 Backups)")
	print("=" * 60)
	
	manager, config, logger = setup_test_environment()
	collector = PerformanceCollector()
	
	try:
		logger.log_event('BENCHMARK', f"Starting replication overhead test for {duration}s")
		
		processes = standard_cluster_startup(manager, logger)
		
		start_time = time.time()
		monitor_thread = threading.Thread(
			target=monitor_performance,
			args=(list(processes.values()), collector, duration),
			daemon=True
		)
		monitor_thread.start()
		
		time.sleep(duration)
		
		actual_duration = time.time() - start_time
		metrics = collector.get_metrics(actual_duration)
		
		logger.log_event('BENCHMARK', f"Replication completed: {metrics.grants_per_second:.2f} grants/sec")
		return metrics
		
	except Exception as e:
		logger.log_event('ERROR', f"Replication test failed: {e}")
		return PerformanceMetrics(0, 0, 0, duration)
	finally:
		manager.cleanup_all()

def election_timing_test() -> PerformanceMetrics:
	print("\n" + "=" * 60)
	print("BENCHMARK: Election Timing")
	print("=" * 60)
	
	manager, config, logger = setup_test_environment()
	collector = PerformanceCollector()
	
	try:
		logger.log_event('BENCHMARK', "Starting election timing test")
		
		processes = standard_cluster_startup(manager, logger)
		
		logger.log_event('BENCHMARK', "System stabilized, triggering election...")
		time.sleep(5)
		
		election_start = time.time()
		collector.record_election_start(election_start)
		
		primary_killed = manager.kill_process(processes['primary'])
		if not primary_killed:
			logger.log_event('ERROR', "Failed to kill primary for election test")
			return PerformanceMetrics(0, 0, 0, 0)
		
		logger.log_process_event('KILLED', processes['primary'])
		
		election_detected = False
		new_primary_found = False
		election_end = None
		
		timeout = 30.0
		check_start = time.time()
		
		while time.time() - check_start < timeout and not new_primary_found:
			for proc_name, proc_info in processes.items():
				if proc_info.role == 'backup' and proc_info.process.poll() is None:
					try:
						line = proc_info.process.stdout.readline()
						if line:
							if 'ELECTION' in line and not election_detected:
								election_detected = True
								logger.log_event('ELECTION', f"Election started by {proc_name}")
							
							if 'PRIMARY' in line and 'becoming' in line.lower():
								election_end = time.time()
								collector.record_election_end(election_end)
								new_primary_found = True
								logger.log_event('PROMOTION', f"{proc_name} became PRIMARY")
								break
					except:
						pass
			
			if not election_detected and not new_primary_found:
				time.sleep(0.1)
		
		if election_end:
			election_duration = election_end - election_start
			metrics = PerformanceMetrics(
				grants_per_second=0,
				avg_grant_latency=0,
				total_grants=0,
				test_duration=election_duration,
				election_time=election_duration
			)
			
			logger.log_event('BENCHMARK', f"Election completed in {election_duration:.3f} seconds")
			return metrics
		else:
			logger.log_event('ERROR', "Election did not complete within timeout")
			return PerformanceMetrics(0, 0, 0, 30.0)
		
	except Exception as e:
		logger.log_event('ERROR', f"Election timing test failed: {e}")
		return PerformanceMetrics(0, 0, 0, 0)
	finally:
		manager.cleanup_all()

def monitor_performance(processes: List, collector: PerformanceCollector, duration: float):
	end_time = time.time() + duration
	
	while time.time() < end_time:
		for proc_info in processes:
			if proc_info.process.poll() is None:
				try:
					line = proc_info.process.stdout.readline()
					if line:
						current_time = time.time()
						
						if 'REQ' in line and 'sending' in line.lower():
							collector.record_request(proc_info.node_id, current_time)
						elif 'GRANT' in line or 'granted' in line.lower():
							collector.record_grant(proc_info.node_id, current_time)
				except:
					pass
		
		time.sleep(0.01)

def create_comparison_table(baseline: PerformanceMetrics, replication: PerformanceMetrics, election: PerformanceMetrics) -> str:
	table = []
	table.append("=" * 80)
	table.append("PERFORMANCE COMPARISON TABLE")
	table.append("=" * 80)
	table.append(f"{'Metric':<30} {'Baseline':<15} {'Replication':<15} {'Overhead':<15}")
	table.append("-" * 80)
	
	table.append(f"{'Grants/Second':<30} {baseline.grants_per_second:<15.2f} {replication.grants_per_second:<15.2f} {(baseline.grants_per_second - replication.grants_per_second):<15.2f}")
	
	if baseline.grants_per_second > 0 and replication.grants_per_second > 0:
		overhead_pct = ((baseline.grants_per_second - replication.grants_per_second) / baseline.grants_per_second) * 100
		table.append(f"{'Overhead Percentage':<30} {'':<15} {'':<15} {overhead_pct:<15.1f}%")
	
	table.append(f"{'Avg Grant Latency (s)':<30} {baseline.avg_grant_latency:<15.3f} {replication.avg_grant_latency:<15.3f} {(replication.avg_grant_latency - baseline.avg_grant_latency):<15.3f}")
	
	table.append(f"{'Total Grants':<30} {baseline.total_grants:<15} {replication.total_grants:<15} {(replication.total_grants - baseline.total_grants):<15}")
	
	if election.election_time:
		table.append("-" * 80)
		table.append(f"{'Election Time (s)':<30} {'N/A':<15} {election.election_time:<15.3f} {'N/A':<15}")
	
	table.append("=" * 80)
	table.append("")
	
	table.append("ANALYSIS:")
	if baseline.grants_per_second > 0 and replication.grants_per_second > 0:
		overhead_pct = ((baseline.grants_per_second - replication.grants_per_second) / baseline.grants_per_second) * 100
		table.append(f"• Replication overhead: {overhead_pct:.1f}% reduction in throughput")
		table.append(f"• Latency increase: {(replication.avg_grant_latency - baseline.avg_grant_latency)*1000:.1f}ms per grant")
	
	if election.election_time:
		table.append(f"• Election time: {election.election_time:.3f} seconds for coordinator failover")
		if replication.grants_per_second > 0:
			lost_grants = election.election_time * replication.grants_per_second
			table.append(f"• Estimated grants lost during election: {lost_grants:.1f}")
	
	table.append("")
	
	return "\n".join(table)

def run_full_benchmark() -> BenchmarkResults:
	print("Starting Distributed Critical Section Performance Benchmark")
	print("=" * 60)
	
	print("Running baseline throughput test...")
	baseline = baseline_throughput_test(30.0)
	
	print("Running replication overhead test...")
	replication = replication_overhead_test(30.0)
	
	print("Running election timing test...")
	election = election_timing_test()
	
	comparison_table = create_comparison_table(baseline, replication, election)
	
	results = BenchmarkResults(
		baseline=baseline,
		with_replication=replication,
		election_timing=election,
		comparison_table=comparison_table
	)
	
	print(comparison_table)
	
	return results

if __name__ == "__main__":
	import sys
	
	if len(sys.argv) > 1:
		test_type = sys.argv[1]
		
		if test_type == 'baseline':
			metrics = baseline_throughput_test(60.0)
			print(f"Baseline: {metrics.grants_per_second:.2f} grants/sec, {metrics.avg_grant_latency:.3f}s latency")
		elif test_type == 'replication':
			metrics = replication_overhead_test(60.0)
			print(f"Replication: {metrics.grants_per_second:.2f} grants/sec, {metrics.avg_grant_latency:.3f}s latency")
		elif test_type == 'election':
			metrics = election_timing_test()
			if metrics.election_time:
				print(f"Election: {metrics.election_time:.3f} seconds")
			else:
				print("Election: Failed to complete")
		elif test_type == 'full':
			results = run_full_benchmark()
		else:
			print(f"Unknown benchmark: {test_type}")
			print("Available benchmarks: baseline, replication, election, full")
			sys.exit(1)
	else:
		results = run_full_benchmark()