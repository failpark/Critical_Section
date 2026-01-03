import time
import socket
import threading
import random
from typing import Dict, List, Tuple, Optional
from test_env import setup_test_environment, standard_cluster_startup, ProcessInfo, MessageDropWrapper, NetworkConfig

def test_message_loss() -> bool:
	print("=" * 60)
	print("TEST: Message Loss Tolerance (30% drop rate)")
	print("=" * 60)
	
	manager, config, logger = setup_test_environment(drop_rate=0.3)
	
	try:
		logger.log_test_start('message_loss')
		start_time = time.time()
		
		processes = standard_cluster_startup(manager, logger)
		
		logger.log_event('TEST', "Applying 30% message drop rate to all network traffic")
		
		test_duration = 30.0
		logger.log_event('TEST', f"Running message loss test for {test_duration} seconds")
		
		grants_observed = 0
		last_check_time = time.time()
		
		end_time = time.time() + test_duration
		while time.time() < end_time:
			time.sleep(2)
			current_time = time.time()
			
			for proc_name, proc_info in processes.items():
				if proc_info.role == 'CLIENT':
					if proc_info.process.poll() is None:
						try:
							stdout_line = proc_info.process.stdout.readline()
							if stdout_line and 'GRANT' in stdout_line:
								grants_observed += 1
								logger.log_event('GRANT', f"Node {proc_name} received grant")
						except:
							pass
			
			if current_time - last_check_time >= 5:
				logger.log_event('PROGRESS', f"Grants observed: {grants_observed}, Time remaining: {end_time - current_time:.1f}s")
				last_check_time = current_time
		
		success = grants_observed >= 3
		duration = time.time() - start_time
		
		logger.log_test_end('message_loss', success, duration)
		logger.log_event('RESULT', f"Total grants observed: {grants_observed} (minimum required: 3)")
		
		return success
		
	except Exception as e:
		logger.log_event('ERROR', f"Test failed with exception: {e}")
		return False
	finally:
		manager.cleanup_all()
		logger.log_event('CLEANUP', "Message loss test cleanup complete")

def test_coordinator_failover() -> bool:
	print("\n" + "=" * 60)
	print("TEST: Coordinator Failover")
	print("=" * 60)
	
	manager, config, logger = setup_test_environment()
	
	try:
		logger.log_test_start('coordinator_failover')
		start_time = time.time()
		
		processes = standard_cluster_startup(manager, logger)
		
		logger.log_event('TEST', "Waiting for initial system stabilization...")
		time.sleep(5)
		
		logger.log_event('TEST', "Killing primary coordinator...")
		primary_killed = manager.kill_process(processes['primary'])
		if not primary_killed:
			logger.log_event('ERROR', "Failed to kill primary coordinator")
			return False
		
		logger.log_process_event('KILLED', processes['primary'])
		
		logger.log_event('TEST', "Waiting for backup promotion...")
		time.sleep(10)
		
		election_detected = False
		new_primary_detected = False
		
		for proc_name, proc_info in processes.items():
			if proc_info.role == 'backup' and proc_info.process.poll() is None:
				try:
					for _ in range(10):
						line = proc_info.process.stdout.readline()
						if line:
							if 'ELECTION' in line or 'becoming PRIMARY' in line:
								election_detected = True
								logger.log_event('ELECTION', f"Election detected in {proc_name}")
							if 'PRIMARY' in line and 'role' in line:
								new_primary_detected = True
								logger.log_event('PROMOTION', f"{proc_name} promoted to PRIMARY")
								break
				except:
					pass
		
		logger.log_event('TEST', "Testing queue preservation after failover...")
		time.sleep(10)
		
		queue_preserved = True
		for proc_name, proc_info in processes.items():
			if proc_info.role == 'CLIENT' and proc_info.process.poll() is None:
				try:
					line = proc_info.process.stdout.readline()
					if line and ('ERROR' in line or 'failed' in line.lower()):
						queue_preserved = False
						logger.log_event('QUEUE_ERROR', f"Queue error in {proc_name}: {line.strip()}")
				except:
					pass
		
		success = election_detected and new_primary_detected and queue_preserved
		duration = time.time() - start_time
		
		logger.log_test_end('coordinator_failover', success, duration)
		logger.log_event('RESULT', f"Election: {election_detected}, New Primary: {new_primary_detected}, Queue Preserved: {queue_preserved}")
		
		return success
		
	except Exception as e:
		logger.log_event('ERROR', f"Failover test failed: {e}")
		return False
	finally:
		manager.cleanup_all()
		logger.log_event('CLEANUP', "Coordinator failover test cleanup complete")

def test_split_brain() -> bool:
	print("\n" + "=" * 60)
	print("TEST: Split Brain Prevention")
	print("=" * 60)
	
	manager, config, logger = setup_test_environment()
	
	try:
		logger.log_test_start('split_brain')
		start_time = time.time()
		
		processes = standard_cluster_startup(manager, logger)
		
		logger.log_event('TEST', "Waiting for initial cluster formation...")
		time.sleep(5)
		
		logger.log_event('TEST', "Simulating network partition by killing backup1...")
		partition_created = manager.kill_process(processes['backup1'])
		if not partition_created:
			logger.log_event('ERROR', "Failed to create network partition")
			return False
		
		logger.log_process_event('PARTITIONED', processes['backup1'])
		
		logger.log_event('TEST', "Waiting for partition detection...")
		time.sleep(8)
		
		step_down_detected = False
		term_validation_detected = False
		
		for proc_name, proc_info in processes.items():
			if proc_info.process.poll() is None:
				try:
					for _ in range(20):
						line = proc_info.process.stdout.readline()
						if line:
							if 'STEP_DOWN' in line or 'step down' in line.lower():
								step_down_detected = True
								logger.log_event('STEP_DOWN', f"Step down detected in {proc_name}")
							if 'STALE_TERM' in line or 'term' in line and 'reject' in line.lower():
								term_validation_detected = True
								logger.log_event('TERM_VALIDATION', f"Term validation in {proc_name}")
				except:
					pass
		
		logger.log_event('TEST', "Testing system recovery...")
		time.sleep(10)
		
		recovery_detected = False
		for proc_name, proc_info in processes.items():
			if proc_info.role == 'backup' and proc_info.process.poll() is None:
				try:
					for _ in range(10):
						line = proc_info.process.stdout.readline()
						if line and ('PRIMARY' in line or 'COORDINATOR' in line):
							recovery_detected = True
							logger.log_event('RECOVERY', f"System recovery detected in {proc_name}")
							break
				except:
					pass
		
		success = step_down_detected and recovery_detected
		duration = time.time() - start_time
		
		logger.log_test_end('split_brain', success, duration)
		logger.log_event('RESULT', f"Step Down: {step_down_detected}, Term Validation: {term_validation_detected}, Recovery: {recovery_detected}")
		
		return success
		
	except Exception as e:
		logger.log_event('ERROR', f"Split brain test failed: {e}")
		return False
	finally:
		manager.cleanup_all()
		logger.log_event('CLEANUP', "Split brain test cleanup complete")

def test_client_failure() -> bool:
	print("\n" + "=" * 60)
	print("TEST: Client Failure Recovery")
	print("=" * 60)
	
	manager, config, logger = setup_test_environment()
	
	try:
		logger.log_test_start('client_failure')
		start_time = time.time()
		
		processes = standard_cluster_startup(manager, logger)
		
		logger.log_event('TEST', "Waiting for a node to acquire critical section...")
		
		holder_found = False
		holder_process = None
		wait_time = 0
		max_wait = 20
		
		while not holder_found and wait_time < max_wait:
			for proc_name, proc_info in processes.items():
				if proc_info.role == 'CLIENT' and proc_info.process.poll() is None:
					try:
						line = proc_info.process.stdout.readline()
						if line and ('GRANT' in line or 'granted' in line.lower()):
							holder_found = True
							holder_process = (proc_name, proc_info)
							logger.log_event('HOLDER_FOUND', f"Node {proc_name} acquired critical section")
							break
					except:
						pass
			
			if not holder_found:
				time.sleep(1)
				wait_time += 1
		
		if not holder_found:
			logger.log_event('ERROR', "No node acquired critical section within timeout")
			return False
		
		time.sleep(2)
		
		logger.log_event('TEST', f"Killing client {holder_process[0]} while holding critical section...")
		client_killed = manager.kill_process(holder_process[1])
		if not client_killed:
			logger.log_event('ERROR', f"Failed to kill client {holder_process[0]}")
			return False
		
		logger.log_process_event('KILLED', holder_process[1])
		
		logger.log_event('TEST', "Waiting for lease expiration and next grant...")
		time.sleep(8)
		
		next_grant_detected = False
		lease_expiry_detected = False
		
		for proc_name, proc_info in processes.items():
			if proc_info.role == 'CLIENT' and proc_info.process.poll() is None and proc_name != holder_process[0]:
				try:
					for _ in range(15):
						line = proc_info.process.stdout.readline()
						if line:
							if 'GRANT' in line or 'granted' in line.lower():
								next_grant_detected = True
								logger.log_event('NEXT_GRANT', f"Next grant detected for {proc_name}")
								break
				except:
					pass
		
		for proc_name, proc_info in processes.items():
			if proc_info.role == 'primary' and proc_info.process.poll() is None:
				try:
					for _ in range(10):
						line = proc_info.process.stdout.readline()
						if line and ('lease' in line.lower() and 'expir' in line.lower()):
							lease_expiry_detected = True
							logger.log_event('LEASE_EXPIRY', "Lease expiration detected")
							break
				except:
					pass
		
		success = next_grant_detected
		duration = time.time() - start_time
		
		logger.log_test_end('client_failure', success, duration)
		logger.log_event('RESULT', f"Next Grant: {next_grant_detected}, Lease Expiry: {lease_expiry_detected}")
		
		return success
		
	except Exception as e:
		logger.log_event('ERROR', f"Client failure test failed: {e}")
		return False
	finally:
		manager.cleanup_all()
		logger.log_event('CLEANUP', "Client failure test cleanup complete")

def run_all_tests() -> Dict[str, bool]:
	print("Starting Distributed Critical Section Test Suite")
	print("=" * 60)
	
	test_results = {}
	
	test_results['message_loss'] = test_message_loss()
	test_results['coordinator_failover'] = test_coordinator_failover()
	test_results['split_brain'] = test_split_brain()
	test_results['client_failure'] = test_client_failure()
	
	print("\n" + "=" * 60)
	print("TEST SUITE RESULTS")
	print("=" * 60)
	
	passed = 0
	total = len(test_results)
	
	for test_name, result in test_results.items():
		status = "PASS" if result else "FAIL"
		print(f"{test_name:25} : {status}")
		if result:
			passed += 1
	
	print("-" * 40)
	print(f"Tests passed: {passed}/{total}")
	
	if passed == total:
		print("ALL TESTS PASSED! ✓")
		return True
	else:
		print(f"FAILURES DETECTED: {total - passed} test(s) failed")
		return False

if __name__ == "__main__":
	import sys
	
	if len(sys.argv) > 1:
		test_name = sys.argv[1]
		if test_name == 'message_loss':
			success = test_message_loss()
		elif test_name == 'coordinator_failover':
			success = test_coordinator_failover()
		elif test_name == 'split_brain':
			success = test_split_brain()
		elif test_name == 'client_failure':
			success = test_client_failure()
		else:
			print(f"Unknown test: {test_name}")
			print("Available tests: message_loss, coordinator_failover, split_brain, client_failure")
			sys.exit(1)
		
		sys.exit(0 if success else 1)
	else:
		all_passed = run_all_tests()
		sys.exit(0 if all_passed else 1)