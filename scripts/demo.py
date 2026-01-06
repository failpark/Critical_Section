#!/usr/bin/env python3
import subprocess
import time
import sys
import signal
import argparse
from typing import Optional, List
from dataclasses import dataclass

@dataclass
class DemoResult:
	scenario: str
	success: bool
	duration: float
	details: str

class DockerDemoRunner:
	def __init__(self, compose_file: str = "docker-compose.yml"):
		self.compose_file = compose_file
		self.process: Optional[subprocess.Popen] = None

	def docker_compose_cmd(self, *args: str) -> List[str]:
		return ["docker-compose", "-f", self.compose_file] + list(args)

	def start_cluster(self) -> bool:
		print("\n=== Starting cluster ===")
		try:
			subprocess.run(
				self.docker_compose_cmd("up", "-d", "--build"),
				check=True,
				capture_output=True
			)
			print("✓ Cluster started successfully")
			return True
		except subprocess.CalledProcessError as e:
			print(f"✗ Failed to start cluster: {e.stderr.decode()}")
			return False

	def stop_cluster(self) -> None:
		print("\n=== Stopping cluster ===")
		subprocess.run(
			self.docker_compose_cmd("down"),
			capture_output=True
		)
		print("✓ Cluster stopped")

	def follow_logs(self, duration: int, services: Optional[List[str]] = None) -> None:
		cmd = self.docker_compose_cmd("logs", "-f", "--tail=20")
		if services:
			cmd.extend(services)

		self.process = subprocess.Popen(
			cmd,
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			text=True
		)

		start_time = time.time()
		try:
			while time.time() - start_time < duration:
				line = self.process.stdout.readline()
				if line:
					print(line.rstrip())
				else:
					break
		except KeyboardInterrupt:
			pass
		finally:
			if self.process:
				self.process.terminate()
				self.process.wait()

	def stop_container(self, container: str) -> None:
		print(f"\n=== Stopping {container} ===")
		subprocess.run(
			self.docker_compose_cmd("stop", container),
			capture_output=True
		)
		print(f"✓ {container} stopped")

	def run_scenario_critical_section(self) -> DemoResult:
		print("\n" + "="*60)
		print("SCENARIO 1: Critical Section Access")
		print("="*60)
		print("\nThis scenario demonstrates:")
		print("- Nodes requesting critical section access")
		print("- Coordinator granting exclusive access")
		print("- Nodes holding and releasing the critical section")
		print("\nObserve the REQ → GRANT → work → REL cycle")
		print("\n" + "="*60)

		if not self.start_cluster():
			return DemoResult("critical_section", False, 0.0, "Failed to start cluster")

		print("\nWaiting 5 seconds for cluster initialization...")
		time.sleep(5)

		print("\n--- Monitoring nodes (30 seconds) ---")
		start_time = time.time()
		self.follow_logs(30, ["node1", "node2", "node3"])
		duration = time.time() - start_time

		self.stop_cluster()

		return DemoResult(
			"critical_section",
			True,
			duration,
			"Critical section access demonstrated successfully"
		)

	def run_scenario_leader_election(self) -> DemoResult:
		print("\n" + "="*60)
		print("SCENARIO 2: Leader Election and Failover")
		print("="*60)
		print("\nThis scenario demonstrates:")
		print("- Normal operation with primary coordinator")
		print("- Primary failure detection")
		print("- Bully algorithm election among backups")
		print("- Automatic failover to new coordinator")
		print("\n" + "="*60)

		if not self.start_cluster():
			return DemoResult("leader_election", False, 0.0, "Failed to start cluster")

		print("\nWaiting 10 seconds for normal operation...")
		time.sleep(10)

		print("\n--- Initial cluster state ---")
		self.follow_logs(5)

		print("\n" + "!"*60)
		print("MANUAL ACTION REQUIRED:")
		print("Kill the primary coordinator now by:")
		print("  1. Pressing Ctrl+C on the primary laptop, OR")
		print("  2. Running: docker-compose stop primary")
		print("\nThe demo will continue automatically after 30 seconds...")
		print("!"*60)

		input("\nPress ENTER when you have killed the primary coordinator...")

		print("\n--- Observing election and failover (30 seconds) ---")
		print("Watch for:")
		print("- ELECTION messages from backup coordinators")
		print("- OK responses from higher-ID backups")
		print("- COORDINATOR broadcast from new leader")
		print("- Nodes reconnecting to new coordinator")
		print()

		start_time = time.time()
		self.follow_logs(30)
		duration = time.time() - start_time

		self.stop_cluster()

		return DemoResult(
			"leader_election",
			True,
			duration,
			"Leader election and failover demonstrated successfully"
		)

	def run_scenario_all(self) -> List[DemoResult]:
		results = []

		result1 = self.run_scenario_critical_section()
		results.append(result1)

		print("\n\n" + "="*60)
		print("Waiting 5 seconds before next scenario...")
		print("="*60)
		time.sleep(5)

		result2 = self.run_scenario_leader_election()
		results.append(result2)

		return results

	def print_summary(self, results: List[DemoResult]) -> None:
		print("\n\n" + "="*60)
		print("DEMO SUMMARY")
		print("="*60)
		for result in results:
			status = "✓ PASS" if result.success else "✗ FAIL"
			print(f"\n{status} - {result.scenario}")
			print(f"  Duration: {result.duration:.1f}s")
			print(f"  Details: {result.details}")
		print("\n" + "="*60)

def main():
	parser = argparse.ArgumentParser(
		description='Docker demo runner for distributed critical section system'
	)
	parser.add_argument(
		'--scenario',
		choices=['critical', 'election', 'all'],
		default='all',
		help='Scenario to run (default: all)'
	)
	parser.add_argument(
		'--compose-file',
		default='docker-compose.yml',
		help='Path to docker-compose file'
	)
	args = parser.parse_args()

	runner = DockerDemoRunner(args.compose_file)

	def cleanup_handler(signum, frame):
		print("\n\nInterrupted! Cleaning up...")
		runner.stop_cluster()
		sys.exit(0)

	signal.signal(signal.SIGINT, cleanup_handler)
	signal.signal(signal.SIGTERM, cleanup_handler)

	try:
		results = []

		if args.scenario == 'critical':
			result = runner.run_scenario_critical_section()
			results.append(result)
		elif args.scenario == 'election':
			result = runner.run_scenario_leader_election()
			results.append(result)
		else:
			results = runner.run_scenario_all()

		runner.print_summary(results)

	except Exception as e:
		print(f"\n✗ Demo failed with error: {e}")
		runner.stop_cluster()
		sys.exit(1)

if __name__ == "__main__":
	main()
